package index

import (
	"github.com/bits-and-blooms/bloom/v3"
	"sync"
)

// BloomFilter 是布隆过滤器的并发安全包装类
// 用于快速判断一个 key 是否可能存在于索引中
type BloomFilter struct {
	filter *bloom.BloomFilter
	mu     sync.RWMutex
}

// NewBloomFilter 创建一个新的布隆过滤器
// 参数：
//   - n: 预期存储的元素数量
//   - fp: 期望的误判率
//
// 返回：
//   - *BloomFilter: 布隆过滤器指针
func NewBloomFilter(n uint, fp float64) *BloomFilter {
	// 使用 NewWithEstimates 自动计算最优的 m 和 k
	return &BloomFilter{
		filter: bloom.NewWithEstimates(n, fp),
	}
}

// Add 添加一个 key 到布隆过滤器
// 参数：
//   - key: 要添加的键
func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.filter.Add(key)
}

// Test 测试一个 key 是否可能存在于布隆过滤器中
// 参数：
//   - key: 要测试的键
//
// 返回：
//   - bool: true 表示可能存在，false 表示一定不存在
func (bf *BloomFilter) Test(key []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.filter.Test(key)
}

// AddAndTest 添加 key 并返回添加后的测试结果
// 参数：
//   - key: 要添加和测试的键
//
// 返回：
//   - bool: 添加后测试的结果
func (bf *BloomFilter) AddAndTest(key []byte) bool {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.filter.Add(key)
	return bf.filter.Test(key)
}

// Reset 重置布隆过滤器
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	m := bf.filter.Cap()
	k := bf.filter.K()
	bf.filter = bloom.New(m, k)
}

// K 返回布隆过滤器使用的哈希函数数量
func (bf *BloomFilter) K() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.filter.K()
}

// Cap 返回布隆过滤器的容量
func (bf *BloomFilter) Cap() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.filter.Cap()
}

// 确保 BloomFilter 实现了相关接口
var _ interface {
	Add(key []byte)
	Test(key []byte) bool
} = (*BloomFilter)(nil)
