package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/forever-free1/TideKV/storage"
	"github.com/plar/go-adaptive-radix-tree"
)

// ==================== 类型定义 ====================

// AccessType 定义访问类型
type AccessType int

const (
	AccessTypeRead AccessType = iota
	AccessTypeWrite
)

// HotEntry 表示热数据的条目
type HotEntry struct {
	Position *storage.Position
	Frequency atomic.Int64
	LastAccess time.Time
}

// WarmEntry 表示温数据的条目
type WarmEntry struct {
	Position  *storage.Position
	Frequency atomic.Int64
	LastAccess time.Time
}

// SparseIndexEntry 稀疏索引条目
// 用于 Cold 层快速定位数据在文件中的位置
type SparseIndexEntry struct {
	Key     []byte
	FileID  uint32
	Offset  int64
}

// ==================== HybridIndex 主结构体 ====================

// HybridIndex 三层混合索引架构
// - Hot: 内存 ART 树，存储高频访问的 key
// - Warm: 内存 ART 树，存储中频访问的 key
// - Cold: 磁盘稀疏索引 + 有序表，存储所有 key
type HybridIndex struct {
	// 热数据层：高频访问的 key
	hotTree    art.Tree
	hotEntries map[string]*HotEntry
	hotMu      sync.RWMutex

	// 温数据层：中频访问的 key
	warmTree    art.Tree
	warmEntries map[string]*WarmEntry
	warmMu      sync.RWMutex

	// 冷数据层：稀疏索引（内存）+ 有序数据（磁盘）
	sparseIndex     []SparseIndexEntry // 稀疏索引，内存中维护
	sparseIndexMu   sync.RWMutex

	// 统计信息：记录每个 key 的访问频率
	stats sync.Map // map[string]*atomic.Int64

	// 配置参数
	options *HybridOptions

	// 控制通道
	stopCh chan struct{}

	// 容量监控
	totalKeys int64
}

// HybridOptions 三层索引的配置选项
type HybridOptions struct {
	// Hot 层容量（最多存储的 key 数量）
	HotCapacity int

	// Warm 层容量
	WarmCapacity int

	// 提升阈值：从 Warm 提升到 Hot 需要的访问次数
	PromoteThreshold int64

	// 降级阈值：从 Hot 降级到 Warm 后的最小访问次数
	DemoteThreshold int64

	// 统计重置周期（秒）
	StatsResetInterval int

	// 后台任务执行间隔（毫秒）
	BackgroundInterval int
}

// DefaultHybridOptions 返回默认配置
func DefaultHybridOptions() *HybridOptions {
	return &HybridOptions{
		HotCapacity:         10000,      // 热层最多 1 万个 key
		WarmCapacity:        100000,     // 温层最多 10 万个 key
		PromoteThreshold:   10,         // 访问 10 次后提升到热层
		DemoteThreshold:    5,          // 访问低于 5 次后降级到温层
		StatsResetInterval: 300,        // 5 分钟重置统计
		BackgroundInterval: 1000,       // 1 秒执行一次后台任务
	}
}

// ==================== 构造函数 ====================

// NewHybridIndex 创建新的三层混合索引
func NewHybridIndex(opts ...Option) *HybridIndex {
	options := DefaultHybridOptions()
	for _, opt := range opts {
		opt(options)
	}

	hi := &HybridIndex{
		hotTree:    art.New(),
		hotEntries: make(map[string]*HotEntry),
		warmTree:   art.New(),
		warmEntries: make(map[string]*WarmEntry),
		sparseIndex: make([]SparseIndexEntry, 0),
		options:    options,
		stopCh:    make(chan struct{}),
	}

	// 启动后台 goroutine
	go hi.backgroundWorker()

	return hi
}

// ==================== Option 模式 ====================

// Option 定义配置函数
type Option func(*HybridOptions)

// WithHotCapacity 设置热层容量
func WithHotCapacity(capacity int) Option {
	return func(o *HybridOptions) {
		o.HotCapacity = capacity
	}
}

// WithWarmCapacity 设置温层容量
func WithWarmCapacity(capacity int) Option {
	return func(o *HybridOptions) {
		o.WarmCapacity = capacity
	}
}

// WithPromoteThreshold 设置提升阈值
func WithPromoteThreshold(threshold int64) Option {
	return func(o *HybridOptions) {
		o.PromoteThreshold = threshold
	}
}

// ==================== 核心接口实现 ====================

// Put 写入键值对到索引
func (hi *HybridIndex) Put(key []byte, pos *storage.Position) {
	keyStr := string(key)

	// 更新统计信息
	hi.incrementStats(keyStr)

	// 先尝试在现有层查找
	// 如果存在，则更新位置信息
	if hi.existsInHot(keyStr) {
		hi.updateHotEntry(keyStr, pos)
		hi.incrementHotFrequency(keyStr)
		return
	}

	if hi.existsInWarm(keyStr) {
		hi.updateWarmEntry(keyStr, pos)
		hi.incrementWarmFrequency(keyStr)
		// 检查是否需要提升到热层
		if hi.getStats(keyStr) >= hi.options.PromoteThreshold {
			hi.promoteToHot(keyStr)
		}
		return
	}

	// 新 key：添加到冷层（稀疏索引）
	hi.addToCold(key, pos)

	// 原子增加总 key 计数
	atomic.AddInt64(&hi.totalKeys, 1)
}

// Get 查询键值对
// 按照 Hot -> Warm -> Cold 的顺序查询
func (hi *HybridIndex) Get(key []byte) *storage.Position {
	keyStr := string(key)

	// 1. 先查询热层
	if pos := hi.getFromHot(keyStr); pos != nil {
		// 更新统计
		hi.incrementStats(keyStr)
		hi.incrementHotFrequency(keyStr)
		hi.updateHotAccessTime(keyStr)
		return pos
	}

	// 2. 查询温层
	if pos := hi.getFromWarm(keyStr); pos != nil {
		// 更新统计
		hi.incrementStats(keyStr)
		hi.incrementWarmFrequency(keyStr)
		hi.updateWarmAccessTime(keyStr)

		// 检查是否需要提升到热层
		if hi.getStats(keyStr) >= hi.options.PromoteThreshold {
			hi.promoteToHot(keyStr)
		}
		return pos
	}

	// 3. 查询冷层（稀疏索引）
	if pos := hi.getFromCold(key); pos != nil {
		// 更新统计
		hi.incrementStats(keyStr)

		// 冷层查询也算一次访问，可能提升到温层
		freq := hi.getStats(keyStr)
		if freq >= 2 {
			// 如果访问频率足够，添加到温层
			hi.addToWarm(key, pos)
		}
		return pos
	}

	return nil
}

// Delete 删除键值对
func (hi *HybridIndex) Delete(key []byte) bool {
	keyStr := string(key)

	// 从热层删除
	if hi.removeFromHot(keyStr) {
		return true
	}

	// 从温层删除
	if hi.removeFromWarm(keyStr) {
		return true
	}

	// 从冷层删除
	if hi.removeFromCold(key) {
		return true
	}

	// 删除统计
	hi.stats.Delete(keyStr)
	atomic.AddInt64(&hi.totalKeys, -1)
	return false
}

// Size 返回索引中的键值对数量
func (hi *HybridIndex) Size() int {
	hotSize := hi.hotTree.Size()
	warmSize := hi.warmTree.Size()
	coldSize := len(hi.sparseIndex)
	return hotSize + warmSize + coldSize
}

// Close 关闭索引
func (hi *HybridIndex) Close() {
	// 停止后台 goroutine
	close(hi.stopCh)
}

// ==================== 热层操作 ====================

func (hi *HybridIndex) existsInHot(key string) bool {
	hi.hotMu.RLock()
	defer hi.hotMu.RUnlock()
	_, found := hi.hotEntries[key]
	return found
}

func (hi *HybridIndex) getFromHot(key string) *storage.Position {
	hi.hotMu.RLock()
	defer hi.hotMu.RUnlock()
	if entry, found := hi.hotEntries[key]; found {
		return entry.Position
	}
	return nil
}

func (hi *HybridIndex) updateHotEntry(key string, pos *storage.Position) {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()
	if entry, found := hi.hotEntries[key]; found {
		entry.Position = pos
	}
}

func (hi *HybridIndex) incrementHotFrequency(key string) {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()
	if entry, found := hi.hotEntries[key]; found {
		entry.Frequency.Add(1)
	}
}

func (hi *HybridIndex) updateHotAccessTime(key string) {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()
	if entry, found := hi.hotEntries[key]; found {
		entry.LastAccess = time.Now()
	}
}

func (hi *HybridIndex) removeFromHot(key string) bool {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()
	if _, found := hi.hotEntries[key]; found {
		hi.hotTree.Delete(art.Key(key))
		delete(hi.hotEntries, key)
		return true
	}
	return false
}

func (hi *HybridIndex) addToHot(key string, pos *storage.Position) {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()

	// 检查容量
	if hi.hotTree.Size() >= hi.options.HotCapacity {
		// 需要降级一个条目到温层
		hi.demoteOneFromHot()
	}

	entry := &HotEntry{
		Position:    pos,
		LastAccess: time.Now(),
	}
	entry.Frequency.Store(1)

	hi.hotEntries[key] = entry
	hi.hotTree.Insert(art.Key(key), pos)
}

// ==================== 温层操作 ====================

func (hi *HybridIndex) existsInWarm(key string) bool {
	hi.warmMu.RLock()
	defer hi.warmMu.RUnlock()
	_, found := hi.warmEntries[key]
	return found
}

func (hi *HybridIndex) getFromWarm(key string) *storage.Position {
	hi.warmMu.RLock()
	defer hi.warmMu.RUnlock()
	if entry, found := hi.warmEntries[key]; found {
		return entry.Position
	}
	return nil
}

func (hi *HybridIndex) updateWarmEntry(key string, pos *storage.Position) {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()
	if entry, found := hi.warmEntries[key]; found {
		entry.Position = pos
	}
}

func (hi *HybridIndex) incrementWarmFrequency(key string) {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()
	if entry, found := hi.warmEntries[key]; found {
		entry.Frequency.Add(1)
	}
}

func (hi *HybridIndex) updateWarmAccessTime(key string) {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()
	if entry, found := hi.warmEntries[key]; found {
		entry.LastAccess = time.Now()
	}
}

func (hi *HybridIndex) removeFromWarm(key string) bool {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()
	if _, found := hi.warmEntries[key]; found {
		hi.warmTree.Delete(art.Key(key))
		delete(hi.warmEntries, key)
		return true
	}
	return false
}

func (hi *HybridIndex) addToWarm(key []byte, pos *storage.Position) {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()

	// 检查容量
	if hi.warmTree.Size() >= hi.options.WarmCapacity {
		// 温层已满，删除最旧的条目
		hi.demoteOneFromWarm()
	}

	keyStr := string(key)
	entry := &WarmEntry{
		Position:    pos,
		LastAccess: time.Now(),
	}
	entry.Frequency.Store(hi.getStats(keyStr))

	hi.warmEntries[keyStr] = entry
	hi.warmTree.Insert(art.Key(key), pos)
}

// ==================== 冷层操作 ====================

func (hi *HybridIndex) addToCold(key []byte, pos *storage.Position) {
	hi.sparseIndexMu.Lock()
	defer hi.sparseIndexMu.Unlock()

	entry := SparseIndexEntry{
		Key:    key,
		FileID: pos.FileID,
		Offset: pos.Offset,
	}

	// 使用二分查找插入保持有序
	hi.sparseIndex = append(hi.sparseIndex, entry)
	// 简单排序（实际生产应该用插入排序或保持有序）
	// 这里省略排序逻辑，假设插入是有序的
}

func (hi *HybridIndex) getFromCold(key []byte) *storage.Position {
	hi.sparseIndexMu.RLock()
	defer hi.sparseIndexMu.RUnlock()

	// 二分查找
	idx := hi.binarySearch(key)
	if idx >= 0 && idx < len(hi.sparseIndex) {
		entry := hi.sparseIndex[idx]
		if string(entry.Key) == string(key) {
			return &storage.Position{
				FileID: entry.FileID,
				Offset: entry.Offset,
				Size:   0, // Cold 层不记录 size，需要从数据文件读取
			}
		}
	}
	return nil
}

func (hi *HybridIndex) removeFromCold(key []byte) bool {
	hi.sparseIndexMu.Lock()
	defer hi.sparseIndexMu.Unlock()

	idx := hi.binarySearch(key)
	if idx >= 0 && idx < len(hi.sparseIndex) {
		if string(hi.sparseIndex[idx].Key) == string(key) {
			hi.sparseIndex = append(hi.sparseIndex[:idx], hi.sparseIndex[idx+1:]...)
			return true
		}
	}
	return false
}

// binarySearch 二分查找key在稀疏索引中的位置
func (hi *HybridIndex) binarySearch(key []byte) int {
	lo, hiIdx := 0, len(hi.sparseIndex)-1
	for lo <= hiIdx {
		mid := (lo + hiIdx) / 2
		cmp := compareKeys(hi.sparseIndex[mid].Key, key)
		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hiIdx = mid - 1
		}
	}
	return lo
}

// compareKeys 比较两个 key 的大小
func compareKeys(a, b []byte) int {
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

// ==================== 层级提升/降级 ====================

// promoteToHot 将 key 从温层提升到热层
func (hi *HybridIndex) promoteToHot(key string) {
	// 从温层获取
	hi.warmMu.RLock()
	entry, found := hi.warmEntries[key]
	hi.warmMu.RUnlock()

	if !found {
		return
	}

	// 检查热层容量
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()

	if hi.hotTree.Size() >= hi.options.HotCapacity {
		// 需要先降级一个
		hi.demoteOneFromHot()
	}

	// 从温层移除
	hi.warmMu.Lock()
	delete(hi.warmEntries, key)
	hi.warmTree.Delete(art.Key(key))
	hi.warmMu.Unlock()

	// 添加到热层
	hi.hotEntries[key] = &HotEntry{
		Position:    entry.Position,
		Frequency:   *new(atomic.Int64),
		LastAccess: time.Now(),
	}
	hi.hotEntries[key].Frequency.Store(entry.Frequency.Load())
	hi.hotTree.Insert(art.Key(key), entry.Position)

	// 重置统计
	hi.stats.Delete(key)
}

// demoteOneFromHot 将热层中最不常用的一个 key 降级到温层
func (hi *HybridIndex) demoteOneFromHot() {
	hi.hotMu.Lock()
	defer hi.hotMu.Unlock()

	if hi.hotTree.Size() == 0 {
		return
	}

	// 找到访问频率最低的条目
	var minKey string
	var minFreq int64 = ^int64(0) >> 1

	for key, entry := range hi.hotEntries {
		freq := entry.Frequency.Load()
		if freq < minFreq {
			minFreq = freq
			minKey = key
		}
	}

	if minKey != "" {
		// 获取位置信息
		entry := hi.hotEntries[minKey]
		pos := entry.Position

		// 从热层删除
		delete(hi.hotEntries, minKey)
		hi.hotTree.Delete(art.Key(minKey))

		// 添加到温层
		hi.warmMu.Lock()
		hi.warmEntries[minKey] = &WarmEntry{
			Position:    pos,
			Frequency:   *new(atomic.Int64),
			LastAccess: time.Now(),
		}
		hi.warmEntries[minKey].Frequency.Store(minFreq)
		hi.warmTree.Insert(art.Key(minKey), pos)
		hi.warmMu.Unlock()
	}
}

// demoteOneFromWarm 将温层中最不常用的一个 key 降级到冷层
func (hi *HybridIndex) demoteOneFromWarm() {
	hi.warmMu.Lock()
	defer hi.warmMu.Unlock()

	if hi.warmTree.Size() == 0 {
		return
	}

	// 找到访问频率最低的条目
	var minKey string
	var minTime time.Time

	for key, entry := range hi.warmEntries {
		if minKey == "" || entry.LastAccess.Before(minTime) {
			minTime = entry.LastAccess
			minKey = key
		}
	}

	if minKey != "" {
		// 获取位置信息
		entry := hi.warmEntries[minKey]
		pos := entry.Position

		// 从温层删除
		delete(hi.warmEntries, minKey)
		hi.warmTree.Delete(art.Key(minKey))

		// 添加到冷层
		hi.sparseIndexMu.Lock()
		hi.sparseIndex = append(hi.sparseIndex, SparseIndexEntry{
			Key:    []byte(minKey),
			FileID: pos.FileID,
			Offset: pos.Offset,
		})
		hi.sparseIndexMu.Unlock()
	}
}

// ==================== 统计操作 ====================

func (hi *HybridIndex) incrementStats(key string) {
	value, _ := hi.stats.LoadOrStore(key, new(atomic.Int64))
	value.(*atomic.Int64).Add(1)
}

func (hi *HybridIndex) getStats(key string) int64 {
	value, found := hi.stats.Load(key)
	if !found {
		return 0
	}
	return value.(*atomic.Int64).Load()
}

// ==================== 后台任务 ====================

// backgroundWorker 后台 goroutine，定期执行维护任务
func (hi *HybridIndex) backgroundWorker() {
	ticker := time.NewTicker(time.Duration(hi.options.BackgroundInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-hi.stopCh:
			return
		case <-ticker.C:
			hi.runMaintenance()
		}
	}
}

// runMaintenance 执行维护任务
func (hi *HybridIndex) runMaintenance() {
	// 1. 检查热层是否需要降级
	hi.hotMu.RLock()
	hotSize := hi.hotTree.Size()
	hi.hotMu.RUnlock()

	if hotSize >= hi.options.HotCapacity {
		hi.demoteOneFromHot()
	}

	// 2. 检查温层是否需要降级
	hi.warmMu.RLock()
	warmSize := hi.warmTree.Size()
	hi.warmMu.RUnlock()

	if warmSize >= hi.options.WarmCapacity {
		hi.demoteOneFromWarm()
	}

	// 3. 清理过期的统计信息
	hi.cleanupStats()
}

// cleanupStats 清理长时间未访问的 key 的统计信息
func (hi *HybridIndex) cleanupStats() {
	// 简单策略：每 100 次调用清理一次
	// 实际应该根据时间或数量触发
}

// ==================== 调试和监控方法 ====================

// GetStats 返回索引的统计信息
func (hi *HybridIndex) GetStats() map[string]interface{} {
	hi.hotMu.RLock()
	hotSize := hi.hotTree.Size()
	hi.hotMu.RUnlock()

	hi.warmMu.RLock()
	warmSize := hi.warmTree.Size()
	hi.warmMu.RUnlock()

	hi.sparseIndexMu.RLock()
	coldSize := len(hi.sparseIndex)
	hi.sparseIndexMu.RUnlock()

	return map[string]interface{}{
		"hot_size":  hotSize,
		"warm_size": warmSize,
		"cold_size": coldSize,
		"total":     hotSize + warmSize + coldSize,
	}
}

// String 返回索引的字符串描述
func (hi *HybridIndex) String() string {
	return fmt.Sprintf("HybridIndex{Hot: %d, Warm: %d, Cold: %d}",
		hi.hotTree.Size(), hi.warmTree.Size(), len(hi.sparseIndex))
}

// 确保 HybridIndex 实现了 Index 接口
var _ Index = (*HybridIndex)(nil)
