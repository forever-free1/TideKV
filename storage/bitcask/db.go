package bitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/forever-free1/TideKV/storage"
	"github.com/forever-free1/TideKV/storage/index"
)

// DB 表示 Bitcask 存储引擎的核心结构体
// 封装了数据文件管理、内存索引和配置选项
type DB struct {
	dir          string                  // 数据目录
	activeFile   *DataFile               // 当前活跃的数据文件
	olderFiles   map[uint32]*DataFile   // 历史数据文件集合
	index        index.Index            // 内存索引（支持 Map 或 ART）
	bloomFilter  *index.BloomFilter     // 布隆过滤器，用于快速判断 key 是否存在
	options      *Options               // 配置选项
	mu           sync.RWMutex           // 写锁，保证写入顺序
	fileID       uint32                 // 当前文件 ID
}

// Options 定义 DB 的配置选项
type Options struct {
	// DataFileSizeLimit 单个数据文件的大小限制（字节）
	// 超过限制时创建新文件
	DataFileSizeLimit int64

	// IndexType 索引类型：true 使用 ART (Adaptive Radix Tree)，false 使用 Map
	// 注意：使用 ART 需要安装 github.com/plar/go-adaptive-radix-tree 依赖
	// 默认使用 Map 索引
	IndexType IndexType

	// BloomFilterFP 布隆过滤器的期望误判率
	// 值越小，需要的内存越多
	BloomFilterFP float64
}

// IndexType 定义索引类型
type IndexType int

const (
	// IndexTypeMap 使用内置 Map 作为索引（默认）
	IndexTypeMap IndexType = iota
	// IndexTypeART 使用自适应基数树作为索引
	IndexTypeART
)

// Option 定义 Options 的配置函数
type Option func(*Options)

// WithDataFileSizeLimit 设置单文件大小限制
func WithDataFileSizeLimit(limit int64) Option {
	return func(o *Options) {
		o.DataFileSizeLimit = limit
	}
}

// WithIndexType 设置索引类型
func WithIndexType(indexType IndexType) Option {
	return func(o *Options) {
		o.IndexType = indexType
	}
}

// WithBloomFilterFP 设置布隆过滤器的期望误判率
func WithBloomFilterFP(fp float64) Option {
	return func(o *Options) {
		o.BloomFilterFP = fp
	}
}

// Open 打开或创建一个 Bitcask 数据库
// 参数：
//   - dir: 数据库目录
//   - opts: 配置选项
//
// 返回：
//   - *DB: 数据库指针
//   - error: 打开错误
func Open(dir string, opts ...Option) (*DB, error) {
	// 应用配置选项
	options := &Options{
		DataFileSizeLimit: 64 * 1024 * 1024, // 默认 64MB
		IndexType:        IndexTypeART,       // 默认使用 ART 索引
		BloomFilterFP:   0.01,               // 默认 1% 误判率
	}
	for _, opt := range opts {
		opt(options)
	}

	// 创建索引实例
	var idx index.Index
	switch options.IndexType {
	case IndexTypeART:
		idx = index.NewARTIndex()
	default:
		idx = index.NewMapIndex()
	}

	// 创建布隆过滤器
	// 初始容量设置为 1000000，预估最多存储 100 万个 key
	bloomFilter := index.NewBloomFilter(1000000, options.BloomFilterFP)

	// 创建数据库实例
	db := &DB{
		dir:         dir,
		olderFiles:  make(map[uint32]*DataFile),
		index:       idx,
		bloomFilter: bloomFilter,
		options:     options,
		fileID:      0,
	}

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据库目录失败: %w", err)
	}

	// Bootstrapping：加载或创建数据文件
	if err := db.bootstrap(); err != nil {
		return nil, fmt.Errorf("启动引导失败: %w", err)
	}

	return db, nil
}

// bootstrap 启动引导逻辑
// 如果存在旧的数据文件，遍历它们并重建索引
//
// 【布隆过滤器重建说明】
// 在 Bootstrapping 过程中，我们遍历所有数据文件中的 Entry：
// - 每当读取到一个有效的 Entry 时，将其 Key 同时写入：
//   1. ART/Map 索引（用于定位数据在文件中的位置）
//   2. 布隆过滤器（用于快速判断 key 是否可能存在）
//
// 这样在系统重启后，布隆过滤器会被重建，可以继续用于优化查询不存在的 key
func (db *DB) bootstrap() error {
	// 读取目录中的所有数据文件
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return fmt.Errorf("读取目录失败: %w", err)
	}

	// 收集所有数据文件 ID
	var fileIDs []uint32
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".data") {
			idStr := strings.TrimSuffix(f.Name(), ".data")
			var id uint32
			if _, err := fmt.Sscanf(idStr, "%d", &id); err == nil {
				fileIDs = append(fileIDs, id)
			}
		}
	}

	// 如果没有数据文件，创建第一个活跃文件
	if len(fileIDs) == 0 {
		db.fileID = 0
		activeFile, err := OpenDataFile(db.dir, db.fileID)
		if err != nil {
			return fmt.Errorf("创建活跃数据文件失败: %w", err)
		}
		db.activeFile = activeFile
		return nil
	}

	// 按文件 ID 排序
	sort.Slice(fileIDs, func(i, j int) bool {
		return fileIDs[i] < fileIDs[j]
	})

	// 遍历所有数据文件，构建索引
	for i, fileID := range fileIDs {
		// 打开数据文件
		dataFile, err := OpenDataFile(db.dir, fileID)
		if err != nil {
			return fmt.Errorf("打开数据文件 %d 失败: %w", fileID, err)
		}

		if i == len(fileIDs)-1 {
			// 最后一个文件是当前活跃文件
			db.activeFile = dataFile
			db.fileID = fileID
		} else {
			// 旧文件
			db.olderFiles[fileID] = dataFile
		}

		// 遍历文件中的所有 Entry，构建索引
		var offset int64 = 0
		for {
			entry, err := dataFile.ReadEntry(offset)
			if err != nil {
				if err == io.EOF {
					// 读取完成
					break
				}
				// 如果读取出错（可能是损坏的 Entry），跳过继续
				// 计算下一个可能的 Entry 位置
				// 这里简单处理：每次跳过 20 字节尝试读取下一个
				offset += 20
				if offset >= dataFile.GetWriteOff() {
					break
				}
				continue
			}

			// 构建位置信息
			pos := &storage.Position{
				FileID: fileID,
				Offset: offset,
				Size:   entry.Size(),
			}

			// 写入索引
			db.index.Put(entry.Key, pos)

			// 【关键】重建布隆过滤器：将 Key 加入布隆过滤器
			// 这样在系统重启后，布隆过滤器会被恢复到之前的状态
			db.bloomFilter.Add(entry.Key)

			// 移动到下一个 Entry
			offset += int64(entry.Size())
		}
	}

	// 如果活跃文件为空，从下一个 ID 开始
	if db.activeFile.GetWriteOff() == 0 {
		db.fileID = fileIDs[len(fileIDs)-1] + 1
		newFile, err := OpenDataFile(db.dir, db.fileID)
		if err != nil {
			return fmt.Errorf("创建新的活跃数据文件失败: %w", err)
		}
		db.activeFile = newFile
	}

	return nil
}

// Put 写入键值对
// 参数：
//   - key: 键
//   - value: 值
// 返回：
//   - error: 写入错误
func (db *DB) Put(key []byte, value []byte) error {
	// 加写锁，保证写入顺序
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查是否需要创建新文件
	if db.activeFile.GetWriteOff() >= db.options.DataFileSizeLimit {
		if err := db.rotateActiveFile(); err != nil {
			return fmt.Errorf("轮转活跃文件失败: %w", err)
		}
	}

	// 创建 Entry
	entry := NewEntry(key, value)

	// 追加写入活跃文件
	offset, err := db.activeFile.Write(entry)
	if err != nil {
		return fmt.Errorf("写入数据文件失败: %w", err)
	}

	// 构建位置信息
	pos := &storage.Position{
		FileID: db.activeFile.GetFileID(),
		Offset: offset,
		Size:   entry.Size(),
	}

	// 更新内存索引
	db.index.Put(key, pos)

	// 【关键】将 Key 加入布隆过滤器
	// 这样在后续的 Get 操作中，可以通过布隆过滤器快速判断 key 是否可能存在
	db.bloomFilter.Add(key)

	return nil
}

// rotateActiveFile 轮转活跃文件
// 当活跃文件达到大小限制时，创建一个新的活跃文件
func (db *DB) rotateActiveFile() error {
	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return fmt.Errorf("关闭活跃文件失败: %w", err)
	}

	// 将当前活跃文件移动到旧文件集合
	db.olderFiles[db.activeFile.GetFileID()] = db.activeFile

	// 创建新的活跃文件
	db.fileID++
	newFile, err := OpenDataFile(db.dir, db.fileID)
	if err != nil {
		return fmt.Errorf("创建新的活跃文件失败: %w", err)
	}
	db.activeFile = newFile

	return nil
}

// Get 根据键获取值
// 参数：
//   - key: 键
// 返回：
//   - []byte: 值
//   - error: 读取错误，如果键不存在返回 ErrKeyNotFound
func (db *DB) Get(key []byte) ([]byte, error) {
	// 加读锁
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 【优化】先通过布隆过滤器快速判断 key 是否可能存在
	// 布隆过滤器的 Test 方法：
	//   - 返回 false：key 一定不存在，直接返回 ErrKeyNotFound
	//   - 返回 true：key 可能存在，继续查询 ART 索引
	if !db.bloomFilter.Test(key) {
		// 布隆过滤器返回 false，一定不存在
		return nil, storage.ErrKeyNotFound
	}

	// 布隆过滤器返回 true，可能存在，继续查询 ART 索引
	pos := db.index.Get(key)
	if pos == nil {
		// 索引中也没有，key 确实不存在（布隆过滤器误判）
		return nil, storage.ErrKeyNotFound
	}

	// 根据 FileID 获取数据文件
	var dataFile *DataFile
	if pos.FileID == db.activeFile.GetFileID() {
		dataFile = db.activeFile
	} else {
		var ok bool
		dataFile, ok = db.olderFiles[pos.FileID]
		if !ok {
			return nil, storage.ErrKeyNotFound
		}
	}

	// 从文件读取 Entry
	entry, err := dataFile.ReadEntry(pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("读取 Entry 失败: %w", err)
	}

	// 返回 Value
	return entry.Value, nil
}

// Delete 删除键值对
// 参数：
//   - key: 键
// 返回：
//   - error: 删除错误
func (db *DB) Delete(key []byte) error {
	// 加写锁
	db.mu.Lock()
	defer db.mu.Unlock()

	// 从索引中删除
	// 注意：Bitcask 使用标记删除或直接删除，这里使用直接删除
	// 实际生产环境可能需要使用墓碑机制
	db.index.Delete(key)

	// 注意：布隆过滤器不支持删除操作
	// 如果需要支持删除，应该使用计数布隆过滤器或布谷鸟过滤器
	// 但由于 Bitcask 的特性，我们可以在 Get 时通过索引二次确认

	return nil
}

// Close 关闭数据库
// 返回：
//   - error: 关闭错误
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭所有数据文件
	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return fmt.Errorf("关闭活跃文件失败: %w", err)
		}
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return fmt.Errorf("关闭旧文件失败: %w", err)
		}
	}

	// 关闭索引
	if db.index != nil {
		db.index.Close()
	}

	// 布隆过滤器不需要显式关闭

	return nil
}

// GetFilePath 获取指定文件 ID 的文件路径
// 参数：
//   - fileID: 文件 ID
// 返回：
//   - string: 文件路径
func (db *DB) GetFilePath(fileID uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf("%08d.data", fileID))
}

// 确保 DB 实现了 storage.Engine 接口
var _ storage.Engine = (*DB)(nil)
