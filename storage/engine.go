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

// Iterator 是键值迭代器的抽象接口
// 用于范围查询和有序遍历
type Iterator interface {
	// Next 移动到下一个键
	// 返回：
	//   - bool: 是否还有更多键
	Next()

	// Key 返回当前键
	// 返回：
	//   - []byte: 当前键
	Key() []byte

	// Value 返回当前值
	// 返回：
	//   - []byte: 当前值
	Value() []byte

	// Error 返回迭代过程中的错误
	// 返回：
	//   - error: 错误
	Error() error

	// Close 关闭迭代器
	Close()
}

// Engine 是存储引擎的抽象接口
// 实现了键值存储的基本操作：Put、Get、Delete、Close、Seek
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

	// Seek 查找第一个大于等于 key 的键，并返回迭代器
	// 参数：
	//   - key: 起始键
	// 返回：
	//   - Iterator: 迭代器
	//   - error: 查找错误
	Seek(key []byte) (Iterator, error)

	// Close 关闭存储引擎，释放资源
	// 返回：
	//   - error: 关闭错误
	Close() error
}
