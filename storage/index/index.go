package index

import "github.com/forever-free1/TideKV/storage"

// Index 是内存索引的抽象接口
// 负责存储键到文件位置（Position）的映射
type Index interface {
	// Put 写入键值对到索引
	// 参数：
	//   - key: 键
	//   - pos: 位置指针
	Put(key []byte, pos *storage.Position)

	// Get 根据键获取位置
	// 参数：
	//   - key: 键
	// 返回：
	//   - *storage.Position: 位置指针，不存在返回 nil
	Get(key []byte) *storage.Position

	// Delete 根据键删除索引
	// 参数：
	//   - key: 键
	// 返回：
	//   - bool: 是否删除成功
	Delete(key []byte) bool

	// Size 返回索引中的键值对数量
	Size() int

	// Close 关闭索引，释放资源
	Close()
}
