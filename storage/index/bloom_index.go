package index

import (
	"io"
	"os"
	"path/filepath"

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

// Save 将布隆过滤器持久化到文件
// 参数：
//   - dir: 数据目录
// 返回：
//   - error: 保存错误
func (bf *BloomFilter) Save(dir string) error {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// 创建 bloom 文件路径
	filename := filepath.Join(dir, "bloom.filter")

	// 打开文件
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// 使用 WriteTo 写入
	_, err = bf.filter.WriteTo(file)
	return err
}

// Load 从文件加载布隆过滤器
// 参数：
//   - dir: 数据目录
//   - n: 预期存储的元素数量（用于创建新的布隆过滤器）
//   - fp: 期望的误判率
// 返回：
//   - bool: 是否成功加载（false 表示文件不存在）
//   - error: 加载错误
func (bf *BloomFilter) Load(dir string, n uint, fp float64) (bool, error) {
	filename := filepath.Join(dir, "bloom.filter")

	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	// 使用 ReadFrom 读取
	loadedFilter := new(bloom.BloomFilter)
	_, err = loadedFilter.ReadFrom(file)
	if err != nil {
		return false, err
	}

	// 更新内部状态
	bf.mu.Lock()
	bf.filter = loadedFilter
	bf.mu.Unlock()

	return true, nil
}

// SaveToWriter 将布隆过滤器数据写入 io.Writer
func (bf *BloomFilter) SaveToWriter(w io.Writer) (int64, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.filter.WriteTo(w)
}

// LoadFromReader 从 io.Reader 加载布隆过滤器
func (bf *BloomFilter) LoadFromReader(r io.Reader) error {
	loadedFilter := new(bloom.BloomFilter)
	_, err := loadedFilter.ReadFrom(r)
	if err != nil {
		return err
	}
	bf.mu.Lock()
	bf.filter = loadedFilter
	bf.mu.Unlock()
	return nil
}

// 确保 BloomFilter 实现了相关接口
var _ interface {
	Add(key []byte)
	Test(key []byte) bool
} = (*BloomFilter)(nil)
