//go:build art
// +build art

package index

import (
	"github.com/plar/go-adaptive-radix-tree"
	"github.com/forever-free1/TideKV/storage"
)

// ARTIndex 是基于自适应基数树（Adaptive Radix Tree）的内存索引实现
// 使用方式：go build -tags=art
type ARTIndex struct {
	tree adt.AdaptiveRadixTree
}

// NewARTIndex 创建一个新的 ART 索引实例
// 返回：
//   - *ARTIndex: ART 索引指针
func NewARTIndex() *ARTIndex {
	return &ARTIndex{
		tree: adt.New(),
	}
}

// Put 写入键值对到 ART 索引
// 参数：
//   - key: 键
//   - pos: 位置指针
func (idx *ARTIndex) Put(key []byte, pos *storage.Position) {
	idx.tree.Insert(adt.Key(key), pos)
}

// Get 根据键从 ART 索引获取位置
// 参数：
//   - key: 键
// 返回：
//   - *storage.Position: 位置指针，不存在返回 nil
func (idx *ARTIndex) Get(key []byte) *storage.Position {
	value, found := idx.tree.Search(adt.Key(key))
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
	return idx.tree.Delete(adt.Key(key))
}

// Size 返回 ART 索引中的键值对数量
// 返回：
//   - int: 键值对数量
func (idx *ARTIndex) Size() int {
	return idx.tree.Size()
}

// Close 关闭 ART 索引
func (idx *ARTIndex) Close() {
	// ART 树没有需要关闭的资源，GC 会自动回收
}

// 确保 ARTIndex 实现了 Index 接口
var _ Index = (*ARTIndex)(nil)
