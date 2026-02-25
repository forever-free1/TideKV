package storage

import "errors"

// ErrKeyNotFound 表示键不存在的错误
var ErrKeyNotFound = errors.New("key not found")

// Position 表示数据在文件中的位置
type Position struct {
	FileID uint32 // 数据文件 ID
	Offset int64  // 偏移量
	Size   uint32 // 数据大小
}

// Engine 是存储引擎的抽象接口
// 实现了键值存储的基本操作：Put、Get、Delete、Close
type Engine interface {
	// Put 写入键值对
	// 参数：
	//   - key: 键
	//   - value: 值
	// 返回：
	//   - error: 写入错误
	Put(key []byte, value []byte) error

	// Get 根据键获取值
	// 参数：
	//   - key: 键
	// 返回：
	//   - []byte: 值
	//   - error: 读取错误，如果键不存在返回 ErrKeyNotFound
	Get(key []byte) ([]byte, error)

	// Delete 删除键值对
	// 参数：
	//   - key: 键
	// 返回：
	//   - error: 删除错误
	Delete(key []byte) error

	// Close 关闭存储引擎，释放资源
	// 返回：
	//   - error: 关闭错误
	Close() error
}
