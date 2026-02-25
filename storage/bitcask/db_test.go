package bitcask

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/forever-free1/TideKV/storage"
)

func TestDB_PutAndGet(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	// 打开数据库
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 测试 Put
	key := []byte("test_key")
	value := []byte("test_value")
	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	// 测试 Get
	gotValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if string(gotValue) != string(value) {
		t.Errorf("值不匹配: got %s, want %s", gotValue, value)
	}
}

func TestDB_GetNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 测试获取不存在的键
	_, err = db.Get([]byte("not_exist"))
	if err != storage.ErrKeyNotFound {
		t.Errorf("期望 ErrKeyNotFound, 得到: %v", err)
	}
}

func TestDB_Delete(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 添加数据
	key := []byte("test_key")
	value := []byte("test_value")
	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	// 删除数据
	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}

	// 验证删除
	_, err = db.Get(key)
	if err != storage.ErrKeyNotFound {
		t.Errorf("删除后 Get 应返回 ErrKeyNotFound, 得到: %v", err)
	}
}

func TestDB_MultiplePuts(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 写入多组数据
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}

	for i := range keys {
		err = db.Put([]byte(keys[i]), []byte(values[i]))
		if err != nil {
			t.Fatalf("Put %d 失败: %v", i, err)
		}
	}

	// 验证所有数据
	for i := range keys {
		got, err := db.Get([]byte(keys[i]))
		if err != nil {
			t.Fatalf("Get %d 失败: %v", i, err)
		}
		if string(got) != values[i] {
			t.Errorf("值不匹配: got %s, want %s", got, values[i])
		}
	}

	// 验证文件存在
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录失败: %v", err)
	}
	t.Logf("创建了 %d 个数据文件", len(files))
}

func TestDB_Bootstrap(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	// 第一次写入数据
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	err = db1.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	err = db1.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	db1.Close()

	// 第二次打开数据库，验证 Bootstrapping
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 验证数据是否正确加载
	val, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 失败: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("key1 值不匹配: got %s, want value1", val)
	}

	val, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 失败: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("key2 值不匹配: got %s, want value2", val)
	}

	t.Log("Bootstrapping 测试通过")
}

func TestDB_UpdateValue(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 初始写入
	key := []byte("key")
	err = db.Put(key, []byte("value1"))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	// 更新值
	err = db.Put(key, []byte("value2"))
	if err != nil {
		t.Fatalf("Put 更新失败: %v", err)
	}

	// 获取最新值
	val, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("值不匹配: got %s, want value2", val)
	}
}

func TestDB_FileRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(dir)

	// 使用小的文件大小限制来触发文件轮转
	db, err := Open(dir, WithDataFileSizeLimit(1024))
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()

	// 写入大量数据触发文件轮转
	for i := 0; i < 100; i++ {
		key := []byte("key")
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i)
		}
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Put 失败: %v", err)
		}
	}

	// 验证最后一个值
	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}

	// 验证值是否为最后一个
	expected := byte(99)
	if val[0] != expected {
		t.Errorf("值不匹配: got %d, want %d", val[0], expected)
	}

	// 检查文件数量
	files, _ := os.ReadDir(dir)
	dataFiles := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".data" {
			dataFiles++
		}
	}
	t.Logf("创建了 %d 个数据文件", dataFiles)
}
