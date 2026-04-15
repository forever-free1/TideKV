package http

import (
	"bytes"
	"net/http"
	"sync"
	"time"
)

// ClientPool HTTP 客户端连接池
type ClientPool struct {
	pool sync.Pool
}

// NewClientPool 创建新的客户端池
func NewClientPool() *ClientPool {
	return &ClientPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &http.Client{
					Transport: &http.Transport{
						MaxIdleConns:        100,
						MaxIdleConnsPerHost: 10,
						IdleConnTimeout:     90 * time.Second,
						DisableKeepAlives:   false,
					},
					Timeout: 30 * time.Second,
				}
			},
		},
	}
}

// Get 获取一个 HTTP 客户端
func (cp *ClientPool) Get() *http.Client {
	return cp.pool.Get().(*http.Client)
}

// Put 归还一个 HTTP 客户端
func (cp *ClientPool) Put(client *http.Client) {
	// 重置 Transport 以确保连接可以被复用
	if transport, ok := client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	cp.pool.Put(client)
}

// PooledClient 带连接池的 HTTP 客户端包装器
type PooledClient struct {
	pool *ClientPool
}

// NewPooledClient 创建新的池化客户端
func NewPooledClient() *PooledClient {
	return &PooledClient{
		pool: NewClientPool(),
	}
}

// Do 执行 HTTP 请求（自动获取/归还客户端）
func (pc *PooledClient) Do(req *http.Request) (*http.Response, error) {
	client := pc.pool.Get()
	defer pc.pool.Put(client)
	return client.Do(req)
}

// Get 执行 GET 请求
func (pc *PooledClient) Get(url string) (*http.Response, error) {
	client := pc.pool.Get()
	defer pc.pool.Put(client)
	return client.Get(url)
}

// Post 执行 POST 请求
func (pc *PooledClient) Post(url, contentType string, body []byte) (*http.Response, error) {
	client := pc.pool.Get()
	defer pc.pool.Put(client)
	return client.Post(url, contentType, bytes.NewReader(body))
}

// DefaultClientPool 默认的客户端池（全局单例）
var defaultClientPool *ClientPool
var defaultPoolOnce sync.Once

// GetDefaultPool 获取默认客户端池
func GetDefaultPool() *ClientPool {
	defaultPoolOnce.Do(func() {
		defaultClientPool = NewClientPool()
	})
	return defaultClientPool
}
