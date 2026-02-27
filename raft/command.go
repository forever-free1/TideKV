package raft

import (
	"bytes"
	"fmt"
	"io"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/forever-free1/TideKV/storage"
)

// ==================== 命令定义 ====================

// CommandType 定义命令类型
type CommandType string

const (
	CommandPut    CommandType = "put"
	CommandDelete CommandType = "delete"
)

// LogCommand 用于在 Raft 集群间序列化和传递的用户指令
// 作为 Raft 日志的 payload
type LogCommand struct {
	// 命令类型：Put 或 Delete
	Type CommandType `msgpack:"type"`

	// 命令参数
	Key   []byte `msgpack:"key"`
	Value []byte `msgpack:"value,omitempty"` // Put 时需要
}

// ==================== FSM 实现 ====================

// BitcaskFSM 实现 Hashicorp Raft 的 FSM 接口
// 用于将 Raft 日志应用到 Bitcask 存储引擎
type BitcaskFSM struct {
	engine storage.Engine // 底层的存储引擎
}

// NewBitcaskFSM 创建新的 BitcaskFSM
func NewBitcaskFSM(engine storage.Engine) *BitcaskFSM {
	return &BitcaskFSM{
		engine: engine,
	}
}

// Apply 将 Raft 日志应用到状态机
// 这是 Raft 共识的核心：当日志被复制到多数节点后，会调用 Apply 将命令应用到状态机
//
// 参数：
//   - log: Raft 传递过来的日志消息
//
// 返回：
//   - interface{}: 命令执行的结果（用于返回给客户端）
//   - error: 执行错误
func (f *BitcaskFSM) Apply(log *raft.Log) interface{} {
	// 解析日志中的命令
	var cmd LogCommand
	if err := decodeCommand(log.Data, &cmd); err != nil {
		return fmt.Errorf("解析命令失败: %w", err)
	}

	// 根据命令类型执行不同的操作
	switch cmd.Type {
	case CommandPut:
		// 执行 Put 操作
		if err := f.engine.Put(cmd.Key, cmd.Value); err != nil {
			return fmt.Errorf("Put 执行失败: %w", err)
		}
		return nil

	case CommandDelete:
		// 执行 Delete 操作
		if err := f.engine.Delete(cmd.Key); err != nil {
			return fmt.Errorf("Delete 执行失败: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("未知的命令类型: %s", cmd.Type)
	}
}

// Snapshot 创建状态机的快照
// 用于持久化状态机的当前状态，以便在节点重启时快速恢复
//
// 返回：
//   - raft.FSMSnapshot: 快照对象
//   - error: 创建快照错误
func (f *BitcaskFSM) Snapshot() (raft.FSMSnapshot, error) {
	// 创建快照实现
	return &BitcaskSnapshot{
		// 这里可以传递需要持久化的数据
		// 由于 Bitcask 的数据已经在磁盘文件中，
		// 我们可以选择只快照索引状态或让 Raft 自行处理
	}, nil
}

// Restore 从快照恢复状态机
// 节点启动时或追赶日志时，会从快照恢复状态
//
// 参数：
//   - snapshot: 快照数据的读取器
//
// 返回：
//   - error: 恢复错误
func (f *BitcaskFSM) Restore(snapshot io.ReadCloser) error {
	// 从快照恢复
	// 读取快照数据（如果有的话）
	// 这里可以实现从快照恢复索引的逻辑

	defer snapshot.Close()

	// 注意：由于 Bitcask 的数据本身就在磁盘文件中，
	// 这里可以选择：
	// 1. 不做任何事（依赖 Bootstrapping 重建索引）
	// 2. 从快照中恢复元数据（如最新的 key 列表等）

	return nil
}

// ==================== 快照实现 ====================

// BitcaskSnapshot 实现 raft.FSMSnapshot 接口
type BitcaskSnapshot struct {
	// 可以在这里存储快照的元数据
}

// Persist 将快照数据写入提供的通道
//
// 参数：
//   - sink: 数据存储的目标
//
// 返回：
//   - error: 写入错误
func (s *BitcaskSnapshot) Persist(sink raft.SnapshotSink) error {
	// 序列化快照数据
	// 这里可以写入任意格式的数据

	// 示例：写入空数据（因为 Bitcask 数据本身在磁盘）
	// 如果需要，可以在这里写入索引信息

	_, err := sink.Write(nil)
	if err != nil {
		return err
	}

	// 关闭 sink 表示完成
	if err := sink.Close(); err != nil {
		return err
	}

	return nil
}

// Release 释放快照资源
// 当快照不再需要时调用
func (s *BitcaskSnapshot) Release() {
	// 释放资源
}

// ==================== 命令编码/解码 ====================

// encodeCommand 将 LogCommand 编码为字节数组
func encodeCommand(cmd *LogCommand) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	err := enc.Encode(cmd)
	return buf.Bytes(), err
}

// decodeCommand 从字节数组解码 LogCommand
func decodeCommand(data []byte, cmd *LogCommand) error {
	dec := codec.NewDecoderBytes(data, &codec.MsgpackHandle{})
	return dec.Decode(cmd)
}

// 确保 BitcaskFSM 实现了 raft.FSM 接口
var _ raft.FSM = (*BitcaskFSM)(nil)
