package index

import (
	"sort"

	"github.com/forever-free1/TideKV/storage"
)

// MapIndex 是基于 Go 内置 map 的内存索引实现
// 这是一个后备实现，当 ART 库不可用时使用
type MapIndex struct {
	data    map[string]*storage.Position
	sorted  []string // 排序后的 keys
	dirty   bool     // 是否有未排序的修改
}

// NewMapIndex 创建一个新的 Map 索引实例
// 返回：
//   - *MapIndex: Map 索引指针
func NewMapIndex() *MapIndex {
	return &MapIndex{
		data:   make(map[string]*storage.Position),
		sorted: make([]string, 0),
		dirty:  false,
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
	idx.dirty = true
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
	keyStr := bytesToString(key)
	_, exists := idx.data[keyStr]
	if exists {
		delete(idx.data, keyStr)
		idx.dirty = true
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

// Seek 查找第一个大于等于 key 的键，返回迭代器
func (idx *MapIndex) Seek(key []byte) IndexIterator {
	// 确保排序列表是最新的
	if idx.dirty {
		idx.rebuildSorted()
	}

	keyStr := bytesToString(key)
	pos := sort.SearchStrings(idx.sorted, keyStr)

	return &MapIterator{
		index: idx,
		pos:   pos,
	}
}

// rebuildSorted 重建排序列表
func (idx *MapIndex) rebuildSorted() {
	idx.sorted = make([]string, 0, len(idx.data))
	for k := range idx.data {
		idx.sorted = append(idx.sorted, k)
	}
	sort.Strings(idx.sorted)
	idx.dirty = false
}

// Close 关闭 Map 索引
func (idx *MapIndex) Close() {
	// 清空 map，释放内存
	idx.data = nil
	idx.sorted = nil
}

// MapIterator 是 Map 索引的迭代器实现
type MapIterator struct {
	index *MapIndex
	pos   int
}

// Next 移动到下一个键
func (it *MapIterator) Next() {
	if it.index == nil || it.index.sorted == nil {
		return
	}
	if it.pos < len(it.index.sorted) {
		it.pos++
	}
}

// Key 返回当前键
func (it *MapIterator) Key() []byte {
	if it.index == nil || it.index.sorted == nil {
		return nil
	}
	if it.pos < 0 || it.pos >= len(it.index.sorted) {
		return nil
	}
	return []byte(it.index.sorted[it.pos])
}

// Value 返回当前位置
func (it *MapIterator) Value() *storage.Position {
	if it.index == nil || it.index.sorted == nil {
		return nil
	}
	if it.pos < 0 || it.pos >= len(it.index.sorted) {
		return nil
	}
	return it.index.data[it.index.sorted[it.pos]]
}

// Error 返回错误
func (it *MapIterator) Error() error {
	return nil
}

// Close 关闭迭代器
func (it *MapIterator) Close() {
	it.index = nil
}

// 确保 MapIndex 实现了 Index 接口
var _ Index = (*MapIndex)(nil)
