package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

// Entry 表示存储在数据文件中的记录条目
// 格式：| CRC32 (4B) | Timestamp (8B) | KeySize (4B) | ValueSize (4B) | Key | Value |
type Entry struct {
	CRC       uint32    // 校验和，4 字节
	Timestamp int64     // 时间戳，8 字节
	KeySize   uint32    // Key 长度，4 字节
	ValueSize uint32    // Value 长度，4 字节
	Key       []byte    // 键数据
	Value     []byte    // 值数据
}

// 固定头部大小：CRC(4) + Timestamp(8) + KeySize(4) + ValueSize(4) = 20 字节
const HeaderSize = 20

// NewEntry 创建一个新的 Entry 实例
// 参数：
//   - key: 键
//   - value: 值
//
// 返回：
//   - *Entry: 新的 Entry 指针
func NewEntry(key []byte, value []byte) *Entry {
	return &Entry{
		Timestamp: time.Now().UnixNano(),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
}

// Encode 将 Entry 编码为字节切片
// 编码顺序：小端字节序
// 格式：| CRC32 (4B) | Timestamp (8B) | KeySize (4B) | ValueSize (4B) | Key | Value |
//
// 返回：
//   - []byte: 编码后的字节切片
func (e *Entry) Encode() []byte {
	// 计算总大小并分配缓冲区
	buf := make([]byte, HeaderSize+int(e.KeySize+e.ValueSize))

	// 写入 Timestamp (8 字节，小端序)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(e.Timestamp))

	// 写入 KeySize (4 字节，小端序)
	binary.LittleEndian.PutUint32(buf[12:16], e.KeySize)

	// 写入 ValueSize (4 字节，小端序)
	binary.LittleEndian.PutUint32(buf[16:20], e.ValueSize)

	// 写入 Key
	copy(buf[20:20+e.KeySize], e.Key)

	// 写入 Value
	copy(buf[20+e.KeySize:], e.Value)

	// 计算 CRC32 校验和（不包括 CRC 字段本身）
	// 使用 IEEE 多项式
	e.CRC = crc32.ChecksumIEEE(buf[4:])

	// 将 CRC 写入头部（4 字节，小端序）
	binary.LittleEndian.PutUint32(buf[0:4], e.CRC)

	return buf
}

// Decode 从字节切片解码出 Entry
// 参数：
//   - data: 字节切片
//
// 返回：
//   - *Entry: 解码后的 Entry 指针
//   - error: 解码错误
func Decode(data []byte) (*Entry, error) {
	// 检查数据长度是否足够
	if len(data) < HeaderSize {
		return nil, ErrInvalidEntry
	}

	entry := &Entry{}

	// 读取 CRC (4 字节，小端序)
	entry.CRC = binary.LittleEndian.Uint32(data[0:4])

	// 读取 Timestamp (8 字节，小端序)
	entry.Timestamp = int64(binary.LittleEndian.Uint64(data[4:12]))

	// 读取 KeySize (4 字节，小端序)
	entry.KeySize = binary.LittleEndian.Uint32(data[12:16])

	// 读取 ValueSize (4 字节，小端序)
	entry.ValueSize = binary.LittleEndian.Uint32(data[16:20])

	// 验证数据长度
	totalSize := HeaderSize + int(entry.KeySize+entry.ValueSize)
	if len(data) < totalSize {
		return nil, ErrInvalidEntry
	}

	// 读取 Key
	entry.Key = data[20 : 20+entry.KeySize]

	// 读取 Value
	entry.Value = data[20+entry.KeySize : totalSize]

	// 验证 CRC
	calculatedCRC := crc32.ChecksumIEEE(data[4:totalSize])
	if calculatedCRC != entry.CRC {
		return nil, ErrCRCMismatch
	}

	return entry, nil
}

// GetCRC 获取 CRC 字段的值（用于外部验证）
func (e *Entry) GetCRC() uint32 {
	return e.CRC
}

// GetKey 获取键的副本
func (e *Entry) GetKey() []byte {
	return e.Key
}

// GetValue 获取值的副本
func (e *Entry) GetValue() []byte {
	return e.Value
}

// GetKeySize 获取键的长度
func (e *Entry) GetKeySize() uint32 {
	return e.KeySize
}

// GetValueSize 获取值的长度
func (e *Entry) GetValueSize() uint32 {
	return e.ValueSize
}

// GetTimestamp 获取时间戳
func (e *Entry) GetTimestamp() int64 {
	return e.Timestamp
}

// Size 返回 Entry 的总大小（字节）
func (e *Entry) Size() uint32 {
	return HeaderSize + e.KeySize + e.ValueSize
}

// IsValid 检查 Entry 是否有效
// 通过验证 Key 和 Value 长度是否与声明的一致
func (e *Entry) IsValid() bool {
	return e.KeySize == uint32(len(e.Key)) && e.ValueSize == uint32(len(e.Value))
}

// Equals 比较两个 Entry 是否相等
func (e *Entry) Equals(other *Entry) bool {
	if e == other {
		return true
	}
	if e == nil || other == nil {
		return false
	}
	return e.CRC == other.CRC &&
		e.Timestamp == other.Timestamp &&
		e.KeySize == other.KeySize &&
		e.ValueSize == other.ValueSize &&
		bytes.Equal(e.Key, other.Key) &&
		bytes.Equal(e.Value, other.Value)
}
