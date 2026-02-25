package bitcask

import "errors"

// ErrInvalidEntry 表示无效的 Entry 数据
var ErrInvalidEntry = errors.New("invalid entry data")

// ErrCRCMismatch 表示 CRC 校验失败
var ErrCRCMismatch = errors.New("CRC checksum mismatch")

// ErrFileNotFound 表示数据文件未找到
var ErrFileNotFound = errors.New("data file not found")

// ErrFileClosed 表示文件已关闭
var ErrFileClosed = errors.New("file is closed")

// ErrWriteFailed 表示写入失败
var ErrWriteFailed = errors.New("write failed")

// ErrReadFailed 表示读取失败
var ErrReadFailed = errors.New("read failed")

// ErrSeekFailed 表示文件定位失败
var ErrSeekFailed = errors.New("seek failed")

// ErrSyncFailed 表示同步失败
var ErrSyncFailed = errors.New("sync failed")
