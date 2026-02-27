package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/forever-free1/TideKV/storage"
)

// ==================== 节点配置 ====================

// NodeConfig 定义 Raft 节点的配置
type NodeConfig struct {
	// 节点 ID
	NodeID raft.ServerID

	// 监听地址
	BindAddr string

	// 数据目录（用于存储 Raft 日志和快照）
	DataDir string

	// 集群配置
	Bootstrap bool          // 是否引导集群
	Peers     []raft.Server // 初始集群节点
}

// Node Raft 节点封装
type Node struct {
	raft     *raft.Raft
	fsm      *BitcaskFSM
	engine   storage.Engine
	config   *NodeConfig
	mu       sync.RWMutex
	isLeader atomic.Bool
}

// ==================== 节点创建 ====================

// NewNode 创建新的 Raft 节点
//
// 参数：
//   - engine: 底层的存储引擎（Bitcask）
//   - config: 节点配置
//
// 返回：
//   - *Node: Raft 节点
//   - error: 创建错误
func NewNode(engine storage.Engine, config *NodeConfig) (*Node, error) {
	// 确保数据目录存在
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}

	// 创建 FSM
	fsm := NewBitcaskFSM(engine)

	// 配置 Raft
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = config.NodeID

	// 创建日志存储
	logStore, err := newLogStore(filepath.Join(config.DataDir, "raft-log"))
	if err != nil {
		return nil, fmt.Errorf("创建日志存储失败: %w", err)
	}

	// 创建稳定存储
	stableStore, err := newStableStore(filepath.Join(config.DataDir, "raft-stable"))
	if err != nil {
		return nil, fmt.Errorf("创建稳定存储失败: %w", err)
	}

	// 创建快照存储
	snapshotStore, err := newSnapshotStore(filepath.Join(config.DataDir, "raft-snapshots"))
	if err != nil {
		return nil, fmt.Errorf("创建快照存储失败: %w", err)
	}

	// 配置传输层（TCP）
	transport, err := raft.NewTCPTransport(
		config.BindAddr,
		nil, // 监听器由 Raft 自动创建
		3,   // max pool
		10*time.Second,
		os.Stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("创建传输层失败: %w", err)
	}

	// 创建 Raft 实例
	ra, err := raft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Raft 实例失败: %w", err)
	}

	// 如果需要引导集群
	if config.Bootstrap && len(config.Peers) > 0 {
		configuration := raft.Configuration{
			Servers: config.Peers,
		}
		if err := ra.BootstrapCluster(configuration).Error(); err != nil {
			return nil, fmt.Errorf("引导集群失败: %w", err)
		}
	}

	node := &Node{
		raft:   ra,
		fsm:    fsm,
		engine: engine,
		config: config,
	}

	return node, nil
}

// ==================== 客户端操作 ====================

// Put 通过 Raft 集群写入键值对
// 命令会先写入 Raft 日志，经过共识后才应用到 FSM
func (n *Node) Put(key []byte, value []byte) error {
	// 创建命令
	cmd := &LogCommand{
		Type:  CommandPut,
		Key:   key,
		Value: value,
	}

	// 编码命令
	data, err := encodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("编码命令失败: %w", err)
	}

	// 提交到 Raft
	applyFuture := n.raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("提交应用到 Raft 失败: %w", err)
	}

	// 检查返回结果
	if err, ok := applyFuture.Response().(error); ok && err != nil {
		return err
	}

	return nil
}

// Get 从本地存储引擎读取值
// 注意：Get 不经过 Raft，直接从本地读取
func (n *Node) Get(key []byte) ([]byte, error) {
	return n.engine.Get(key)
}

// Delete 通过 Raft 集群删除键值对
func (n *Node) Delete(key []byte) error {
	// 创建命令
	cmd := &LogCommand{
		Type: CommandDelete,
		Key:  key,
	}

	// 编码命令
	data, err := encodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("编码命令失败: %w", err)
	}

	// 提交到 Raft
	applyFuture := n.raft.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("提交应用到 Raft 失败: %w", err)
	}

	// 检查返回结果
	if err, ok := applyFuture.Response().(error); ok && err != nil {
		return err
	}

	return nil
}

// ==================== 集群管理 ====================

// AddPeer 添加节点到集群
func (n *Node) AddPeer(id raft.ServerID, address string) error {
	future := n.raft.AddVoter(id, raft.ServerAddress(address), 0, 0)
	return future.Error()
}

// RemovePeer 从集群移除节点
func (n *Node) RemovePeer(id raft.ServerID) error {
	future := n.raft.RemoveServer(id, 0, 0)
	return future.Error()
}

// GetLeader 获取当前 Leader 节点信息
func (n *Node) GetLeader() (raft.ServerAddress, bool) {
	leader := n.raft.Leader()
	return leader, leader != ""
}

// IsLeader 判断当前节点是否为 Leader
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// GetPeers 获取集群中的所有节点
func (n *Node) GetPeers() []raft.ServerID {
	config := n.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		return nil
	}

	var peers []raft.ServerID
	for _, server := range config.Configuration().Servers {
		peers = append(peers, server.ID)
	}
	return peers
}

// ==================== 快照与压缩 ====================

// Snapshot 创建快照
// 可以用于压缩 Raft 日志，减小日志文件大小
func (n *Node) Snapshot() error {
	future := n.raft.Snapshot()
	return future.Error()
}

// ==================== 关闭 ====================

// Close 关闭 Raft 节点
func (n *Node) Close() error {
	// 关闭 Raft
	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("关闭 Raft 失败: %w", err)
	}

	// 关闭底层存储引擎
	if err := n.engine.Close(); err != nil {
		return fmt.Errorf("关闭存储引擎失败: %w", err)
	}

	return nil
}

// ==================== 存储实现 ====================

// logStore Raft 日志存储实现
type logStore struct {
	*raft.InmemStore
}

// newLogStore 创建新的日志存储
func newLogStore(path string) (raft.LogStore, error) {
	// 使用内存存储（生产环境应使用 boltDB 或其他持久化存储）
	return &logStore{
		InmemStore: raft.NewInmemStore(),
	}, nil
}

// stableStore Raft 稳定存储实现
type stableStore struct {
	*raft.InmemStore
}

// newStableStore 创建新的稳定存储
func newStableStore(path string) (raft.StableStore, error) {
	return &stableStore{
		InmemStore: raft.NewInmemStore(),
	}, nil
}

// snapshotStore Raft 快照存储实现
type snapshotStore struct {
	dir string
}

// newSnapshotStore 创建新的快照存储
func newSnapshotStore(path string) (raft.SnapshotStore, error) {
	// 确保目录存在
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// 使用文件快照存储
	return raft.NewFileSnapshotStore(path, 3, os.Stderr)
}

// 确保 Node 实现了相关接口
var _ storage.Engine = (*Node)(nil)
