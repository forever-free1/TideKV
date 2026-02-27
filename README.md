# TideKV

一个基于 Bitcask 模型的分布式键值存储引擎，采用 Go 语言实现。

## 架构概览

TideKV 是一个多层架构的分布式存储系统，从底层到顶层依次为：

```
┌─────────────────────────────────────────┐
│           API 网关 (Gin + SSE)          │
│   POST/GET/DELETE /v1/kv/*             │
│   GET /v1/watch (Server-Sent Events)   │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│        Raft 共识层 (Hashicorp)          │
│   分布式一致性保证 / 集群管理            │
│   LogCommand 序列化 / FSM 应用          │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│       三层混合索引 (Hot/Warm/Cold)       │
│   Hot: ART (高频) / Warm: ART (中频)   │
│   Cold: 稀疏索引 + 有序表              │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│        布隆过滤器 (Bloom Filter)        │
│   快速判断 key 是否可能存在              │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│     Bitcask 存储引擎 (datafile)         │
│   Entry 编码 / CRC32 校验 / 文件轮转   │
└─────────────────────────────────────────┘
```

## 核心特性

### 1. L1 纯 ART 索引

使用自适应基数树 (Adaptive Radix Tree) 作为内存索引：

```go
// storage/index/art_index.go
type ARTIndex struct {
    tree adt.AdaptiveRadixTree
}

func (idx *ARTIndex) Put(key []byte, pos *storage.Position) {
    idx.tree.Insert(adt.Key(key), pos)
}

func (idx *ARTIndex) Get(key []byte) *storage.Position {
    value, found := idx.tree.Search(adt.Key(key))
    if !found {
        return nil
    }
    return value.(*storage.Position)
}
```

**特点**：
- 内存紧凑，前缀压缩
- 支持范围查询
- O(k) 时间复杂度（k 为键长度）

### 2. 布隆过滤器优化

在查询不存在的 key 时，布隆过滤器可以快速返回"不存在"，避免不必要的磁盘 I/O：

```go
// storage/bitcask/db.go - Get 方法
func (db *DB) Get(key []byte) ([]byte, error) {
    // 1. 先通过布隆过滤器快速判断
    if !db.bloomFilter.Test(key) {
        return nil, storage.ErrKeyNotFound  // 一定不存在
    }

    // 2. 布隆过滤器返回 true，可能存在，继续查索引
    pos := db.index.Get(key)
    if pos == nil {
        return nil, storage.ErrKeyNotFound
    }

    // 3. 从文件读取数据
    // ...
}
```

### 3. 三层混合索引架构

根据访问频率自动在三层之间流动：

```go
// storage/index/hybrid.go
type HybridIndex struct {
    hotTree    art.Tree    // 热层：ART，存储高频 key
    warmTree   art.Tree    // 温层：ART，存储中频 key
    sparseIndex []SparseIndexEntry  // 冷层：稀疏索引
}
```

**流转规则**：
- Cold → Warm：访问 2 次以上
- Warm → Hot：访问频率达到阈值（默认 10 次）
- Hot → Warm：容量满时降级最少访问的 key
- Warm → Cold：容量满时降级最久未访问的 key

### 4. Raft 共识机制

使用 Hashicorp Raft 实现分布式一致性：

```go
// raft/command.go - LogCommand 结构
type LogCommand struct {
    Type  CommandType  // "put" 或 "delete"
    Key   []byte
    Value []byte
}
```

```go
// raft/command.go - FSM Apply 方法
func (f *BitcaskFSM) Apply(log *raft.Log) interface{} {
    var cmd LogCommand
    decodeCommand(log.Data, &cmd)

    switch cmd.Type {
    case CommandPut:
        f.engine.Put(cmd.Key, cmd.Value)
    case CommandDelete:
        f.engine.Delete(cmd.Key)
    }
    return nil
}
```

### 5. Watch 机制

类似 etcd 的 Watch 机制，支持前缀监听：

```go
// watch/hub.go
type WatchHub struct {
    watchers   []*Watcher
    prefixTree art.Tree  // 利用 ART 前缀匹配
}

func (h *WatchHub) NotifyPut(key string, value string) {
    event := &Event{Type: EventPut, Key: key, Value: value}
    h.Notify(event)
}
```

## 快速开始

### 安装依赖

```bash
go mod tidy
```

### 运行测试

```bash
go test ./storage/bitcask/... -v
```

### 基本使用

```go
package main

import (
    "github.com/forever-free1/TideKV/storage/bitcask"
)

func main() {
    // 打开数据库
    db, err := bitcask.Open("./data")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // 写入数据
    err = db.Put([]byte("key1"), []byte("value1"))
    if err != nil {
        panic(err)
    }

    // 读取数据
    value, err := db.Get([]byte("key1"))
    if err != nil {
        panic(err)
    }
    println(string(value))  // 输出: value1

    // 删除数据
    err = db.Delete([]byte("key1"))
    if err != nil {
        panic(err)
    }
}
```

### 启动 HTTP API 服务器

```go
package main

import (
    "github.com/forever-free1/TideKV/api/http"
    "github.com/forever-free1/TideKV/storage/bitcask"
    "github.com/forever-free1/TideKV/watch"
)

func main() {
    // 打开数据库
    db, _ := bitcask.Open("./data")

    // 创建 Watch Hub
    hub := watch.NewWatchHub()

    // 创建 HTTP 服务器
    server := http.NewServer(":8080", db, hub)

    // 启动
    server.Start()
}
```

### API 调用示例

```bash
# 写入数据
curl -X POST http://localhost:8080/v1/kv/put \
  -H "Content-Type: application/json" \
  -d '{"key": "name", "value": "TideKV"}'

# 读取数据
curl "http://localhost:8080/v1/kv/get?key=name"

# 删除数据
curl -X DELETE "http://localhost:8080/v1/kv/delete?key=name"

# 监听变更 (SSE)
curl "http://localhost:8080/v1/watch?prefix="
```

## 目录结构

```
TideKV/
├── storage/                    # 存储层
│   ├── engine.go              # Engine 接口定义
│   ├── bitcask/               # Bitcask 存储引擎
│   │   ├── db.go              # 主数据库实现
│   │   ├── datafile.go        # 数据文件管理
│   │   ├── entry.go           # Entry 结构编码
│   │   └── errors.go          # 错误定义
│   └── index/                 # 索引层
│       ├── index.go            # Index 接口
│       ├── art_index.go       # ART 索引
│       ├── map_index.go       # Map 索引后备
│       ├── bloom_index.go     # 布隆过滤器
│       └── hybrid.go          # 三层混合索引
├── raft/                      # Raft 共识层
│   ├── command.go             # 命令定义与 FSM
│   └── node.go                # 节点管理
├── watch/                     # Watch 机制
│   └── hub.go                 # 事件通知中心
├── api/http/                  # HTTP API
│   └── handler.go             # Gin 处理器
└── go.mod                     # 依赖管理
```

## 核心数据结构

### Entry (存储格式)

```
┌─────────┬───────────┬─────────┬─────────┬───────┬───────┐
│ CRC(4B) │ Timestamp │ KeySize │ ValueSize│  Key  │ Value │
│         │  (8B)    │  (4B)   │   (4B)  │       │       │
└─────────┴───────────┴─────────┴─────────┴───────┴───────┘
        20 bytes                      Total = 20 + KeySize + ValueSize
```

### Position (文件位置)

```go
type Position struct {
    FileID uint32  // 数据文件 ID
    Offset int64   // 偏移量
    Size   uint32  // 数据大小
}
```

## 扩展阅读

### Bitcask 模型

Bitcask 是一种日志结构的键值存储，具有以下特点：
- 追加写入：所有写操作都追加到活跃文件
- 内存索引：所有 key 索引常驻内存
- 文件轮转：当文件达到大小限制时创建新文件
- 缺点：不支持范围查询，需要定期合并旧文件

### ART (自适应基数树)

Adaptive Radix Tree 是一种紧凑的树形数据结构：
- 节点大小自适应（16/48/256 字节）
- 内存效率高
- 适合字符串键
- 支持前缀查询

### Raft 共识算法

Raft 是一种易于理解的分布式共识算法：
- Leader 选举
- 日志复制
- 安全性保证

## 性能优化建议

1. **布隆过滤器误判率**：设置为 0.01-0.05 可获得较好的性能
2. **文件大小限制**：根据磁盘 I/O 特性调整（默认 64MB）
3. **三层索引容量**：根据内存大小和访问模式调整
4. **Raft 快照**：定期创建快照可压缩日志

## 未来规划

- [ ] 支持范围查询
- [ ] 实现数据压缩
- [ ] 添加持久化布隆过滤器
- [ ] 支持更多一致性级别
- [ ] 添加监控指标

## 许可证

MIT License
