package index

import (
	"github.com/forever-free1/TideKV/storage"
)

// MapIndex 是基于 Go 内置 map 的内存索引实现
// 这是一个后备实现，当 ART 库不可用时使用
type MapIndex struct {
	data map[string]*storage.Position
}

// NewMapIndex 创建一个新的 Map 索引实例
// 返回：
//   - *MapIndex: Map 索引指针
func NewMapIndex() *MapIndex {
	return &MapIndex{
		data: make(map[string]*storage.Position),
	}
}

// bytesToString 将字节切片转换为字符串（用于 map 的 key）
// 注意：这是安全的，因为只用于读取，不会修改底层数据
func bytesToString(b []byte) string {
	return string(b)
}

// Put 写入键值对到 Map 索引
// 参数：
//   - key: 键
//   - pos: 位置指针
func (idx *MapIndex) Put(key []byte, pos *storage.Position) {
	idx.data[bytesToString(key)] = pos
}

// Get 根据键从 Map 索引获取位置
// 参数：
//   - key: 键
// 返回：
//   - *storage.Position: 位置指针，不存在返回 nil
func (idx *MapIndex) Get(key []byte) *storage.Position {
	return idx.data[bytesToString(key)]
}

// Delete 从 Map 索引中删除键
// 参数：
//   - key: 键
// 返回：
//   - bool: 是否删除成功
func (idx *MapIndex) Delete(key []byte) bool {
	_, exists := idx.data[bytesToString(key)]
	if exists {
		delete(idx.data, bytesToString(key))
		return true
	}
	return false
}

// Size 返回 Map 索引中的键值对数量
// 返回：
//   - int: 键值对数量
func (idx *MapIndex) Size() int {
	return len(idx.data)
}

// Close 关闭 Map 索引
func (idx *MapIndex) Close() {
	// 清空 map，释放内存
	idx.data = nil
}

// 确保 MapIndex 实现了 Index 接口
var _ Index = (*MapIndex)(nil)
