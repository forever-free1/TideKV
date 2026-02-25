package bitcask

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DataFile 表示一个数据文件
// 支持追加写入、随机读取和同步操作
type DataFile struct {
	FileID   uint32       // 文件 ID，用于标识不同的数据文件
	File     *os.File     // 底层文件句柄
	WriteOff int64        // 当前写入偏移量
	mu       sync.RWMutex // 读写锁，保护文件操作
}

// DataFileOption 定义 DataFile 的配置选项
type DataFileOption func(*DataFile)

// WithFileID 设置文件 ID
func WithFileID(id uint32) DataFileOption {
	return func(df *DataFile) {
		df.FileID = id
	}
}

// OpenDataFile 打开或创建一个数据文件
// 参数：
//   - dir: 文件所在目录
//   - fileID: 文件 ID
//
// 返回：
//   - *DataFile: 数据文件指针
//   - error: 打开错误
func OpenDataFile(dir string, fileID uint32) (*DataFile, error) {
	// 生成文件名
	filename := fmt.Sprintf("%s/%08d.data", dir, fileID)

	// 以读写追加模式打开文件（不存在则创建）
	// O_APPEND: 每次写入从文件末尾开始
	// O_CREATE: 文件不存在时创建
	// O_RDWR: 读写模式，支持同时读写
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开数据文件失败: %w", err)
	}

	// 获取当前文件大小作为初始写入偏移量
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("获取文件状态失败: %w", err)
	}

	df := &DataFile{
		FileID:   fileID,
		File:     file,
		WriteOff: stat.Size(),
	}

	return df, nil
}

// OpenDataFileWithOptions 使用选项打开或创建数据文件
// 参数：
//   - dir: 文件所在目录
//   - opts: 配置选项
//
// 返回：
//   - *DataFile: 数据文件指针
//   - error: 打开错误
func OpenDataFileWithOptions(dir string, opts ...DataFileOption) (*DataFile, error) {
	// 创建默认配置
	df := &DataFile{
		FileID:   0,
		WriteOff: 0,
	}

	// 应用配置选项
	for _, opt := range opts {
		opt(df)
	}

	// 使用默认配置打开文件
	return OpenDataFile(dir, df.FileID)
}

// Write 追加写入 Entry 到数据文件
// 参数：
//   - entry: 要写入的 Entry
//
// 返回：
//   - int64: 写入后的偏移量
//   - error: 写入错误
func (df *DataFile) Write(entry *Entry) (int64, error) {
	df.mu.Lock()
	defer df.mu.Unlock()

	// 检查文件是否已关闭
	if df.File == nil {
		return 0, ErrFileClosed
	}

	// 编码 Entry 为字节切片
	data := entry.Encode()

	// 记录写入前的偏移量（作为返回的 Position）
	offset := df.WriteOff

	// 写入数据
	n, err := df.File.Write(data)
	if err != nil {
		return offset, fmt.Errorf("写入数据失败: %w", err)
	}

	// 更新写入偏移量
	df.WriteOff += int64(n)

	return offset, nil
}

// WriteBytes 直接写入字节数据
// 参数：
//   - data: 要写入的字节数据
//
// 返回：
//   - int64: 写入后的偏移量
//   - error: 写入错误
func (df *DataFile) WriteBytes(data []byte) (int64, error) {
	df.mu.Lock()
	defer df.mu.Unlock()

	// 检查文件是否已关闭
	if df.File == nil {
		return 0, ErrFileClosed
	}

	// 记录写入前的偏移量
	offset := df.WriteOff

	// 写入数据
	n, err := df.File.Write(data)
	if err != nil {
		return offset, fmt.Errorf("写入字节数据失败: %w", err)
	}

	// 更新写入偏移量
	df.WriteOff += int64(n)

	return offset, nil
}

// Read 从指定偏移量读取数据
// 参数：
//   - offset: 读取起始偏移量
//   - size: 要读取的字节数
//
// 返回：
//   - []byte: 读取的数据
//   - error: 读取错误
func (df *DataFile) Read(offset int64, size uint32) ([]byte, error) {
	df.mu.RLock()
	defer df.mu.RUnlock()

	// 检查文件是否已关闭
	if df.File == nil {
		return nil, ErrFileClosed
	}

	// 跳转到指定偏移量
	_, err := df.File.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("文件定位失败 (offset=%d): %w", offset, err)
	}

	// 读取指定大小的数据
	data := make([]byte, size)
	n, err := df.File.Read(data)
	if err != nil {
		if err == io.EOF {
			// 读取到文件末尾，返回已读取的数据
			return data[:n], nil
		}
		return nil, fmt.Errorf("读取数据失败 (offset=%d, size=%d): %w", offset, size, err)
	}

	return data, nil
}

// ReadEntry 从指定偏移量读取一个完整的 Entry
// 参数：
//   - offset: 读取起始偏移量
//
// 返回：
//   - *Entry: 读取的 Entry
//   - error: 读取错误
func (df *DataFile) ReadEntry(offset int64) (*Entry, error) {
	// 首先读取头部信息（20 字节）
	header, err := df.Read(offset, HeaderSize)
	if err != nil {
		return nil, err
	}

	// 从头部解析 KeySize 和 ValueSize
	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])

	// 计算 Entry 总大小
	totalSize := HeaderSize + int(keySize+valueSize)

	// 读取完整的 Entry 数据
	data, err := df.Read(offset, uint32(totalSize))
	if err != nil {
		return nil, err
	}

	// 解码 Entry
	return Decode(data)
}

// Sync 将缓冲区中的数据同步到磁盘
// 返回：
//   - error: 同步错误
func (df *DataFile) Sync() error {
	df.mu.RLock()
	defer df.mu.RUnlock()

	// 检查文件是否已关闭
	if df.File == nil {
		return ErrFileClosed
	}

	// 调用 fsync 将数据同步到磁盘
	err := df.File.Sync()
	if err != nil {
		return fmt.Errorf("同步数据到磁盘失败: %w", err)
	}

	return nil
}

// Close 关闭数据文件
// 返回：
//   - error: 关闭错误
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()

	// 检查文件是否已关闭
	if df.File == nil {
		return nil
	}

	// 同步数据
	if err := df.File.Sync(); err != nil {
		return fmt.Errorf("关闭前同步数据失败: %w", err)
	}

	// 关闭文件
	err := df.File.Close()
	if err != nil {
		return fmt.Errorf("关闭文件失败: %w", err)
	}

	// 置空文件句柄
	df.File = nil

	return nil
}

// GetWriteOff 获取当前写入偏移量
// 返回：
//   - int64: 当前写入偏移量
func (df *DataFile) GetWriteOff() int64 {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.WriteOff
}

// GetFileID 获取文件 ID
// 返回：
//   - uint32: 文件 ID
func (df *DataFile) GetFileID() uint32 {
	return df.FileID
}

// GetFilePath 获取文件的完整路径
// 参数：
//   - dir: 文件所在目录
//
// 返回：
//   - string: 文件路径
func (df *DataFile) GetFilePath(dir string) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.data", df.FileID))
}

// SetWriteOff 设置写入偏移量
// 注意：此方法仅用于恢复或特殊情况，不应在正常写入时使用
// 参数：
//   - offset: 新的写入偏移量
func (df *DataFile) SetWriteOff(offset int64) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.WriteOff = offset
}

// IsClosed 检查文件是否已关闭
// 返回：
//   - bool: 是否已关闭
func (df *DataFile) IsClosed() bool {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.File == nil
}

// Name 获取文件名（不含路径）
// 返回：
//   - string: 文件名
func (df *DataFile) Name() string {
	return fmt.Sprintf("%08d.data", df.FileID)
}
