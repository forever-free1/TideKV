package index

import (
	"github.com/plar/go-adaptive-radix-tree"
	"github.com/forever-free1/TideKV/storage"
)

// ARTIndex 是基于自适应基数树（Adaptive Radix Tree）的内存索引实现
type ARTIndex struct {
	tree art.Tree
}

// NewARTIndex 创建一个新的 ART 索引实例
// 返回：
//   - *ARTIndex: ART 索引指针
func NewARTIndex() *ARTIndex {
	return &ARTIndex{
		tree: art.New(),
	}
}

// Put 写入键值对到 ART 索引
// 参数：
//   - key: 键
//   - pos: 位置指针
func (idx *ARTIndex) Put(key []byte, pos *storage.Position) {
	idx.tree.Insert(art.Key(key), pos)
}

// Get 根据键从 ART 索引获取位置
// 参数：
//   - key: 键
// 返回：
//   - *storage.Position: 位置指针，不存在返回 nil
func (idx *ARTIndex) Get(key []byte) *storage.Position {
	value, found := idx.tree.Search(art.Key(key))
	if !found {
		return nil
	}
	return value.(*storage.Position)
}

// Delete 从 ART 索引中删除键
// 参数：
//   - key: 键
// 返回：
//   - bool: 是否删除成功
func (idx *ARTIndex) Delete(key []byte) bool {
	_, deleted := idx.tree.Delete(art.Key(key))
	return deleted
}

// Size 返回 ART 索引中的键值对数量
// 返回：
//   - int: 键值对数量
func (idx *ARTIndex) Size() int {
	return idx.tree.Size()
}

// Seek 查找第一个大于等于 key 的键，返回迭代器
func (idx *ARTIndex) Seek(key []byte) IndexIterator {
	iterator := idx.tree.Iterator()
	artIterator := &ARTIterator{
		iterator: iterator,
		node:     nil,
		target:   key,
	}

	// 移动到第一个大于等于 target 的位置
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			break
		}
		if compareARTKeys(node.Key(), key) >= 0 {
			artIterator.node = node
			return artIterator
		}
	}
	return artIterator
}

// Close 关闭 ART 索引
func (idx *ARTIndex) Close() {
	// ART 树没有需要关闭的资源，GC 会自动回收
}

// ARTIterator 是 ART 的迭代器实现
type ARTIterator struct {
	iterator art.Iterator
	node     art.Node
	target   []byte
}

// Next 移动到下一个键
func (it *ARTIterator) Next() {
	if it.iterator == nil {
		return
	}
	for it.iterator.HasNext() {
		node, err := it.iterator.Next()
		if err != nil {
			it.node = nil
			return
		}
		// 继续找大于 target 的键
		if compareARTKeys(node.Key(), it.target) > 0 {
			it.node = node
			return
		}
	}
	it.node = nil
}

// Key 返回当前键
func (it *ARTIterator) Key() []byte {
	if it.node == nil {
		return nil
	}
	return []byte(it.node.Key())
}

// Value 返回当前位置
func (it *ARTIterator) Value() *storage.Position {
	if it.node == nil {
		return nil
	}
	val := it.node.Value()
	if val == nil {
		return nil
	}
	return val.(*storage.Position)
}

// Error 返回错误
func (it *ARTIterator) Error() error {
	return nil
}

// Close 关闭迭代器
func (it *ARTIterator) Close() {
	it.iterator = nil
	it.node = nil
}

// compareARTKeys 比较两个 ART key 的大小
func compareARTKeys(a art.Key, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}
	return 0
}

// 确保 ARTIndex 实现了 Index 接口
var _ Index = (*ARTIndex)(nil)
