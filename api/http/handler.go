package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/forever-free1/TideKV/watch"
)

// ==================== Handler 定义 ====================

// Handler HTTP 请求处理器
type Handler struct {
	// 存储引擎（通过 Raft Node 封装）
	node interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}

	// 事件通知中心
	watchHub *watch.WatchHub
}

// NewHandler 创建新的 Handler
//
// 参数：
//   - node: 存储节点（可以是 Raft Node 或直接的 Engine）
//   - watchHub: 事件通知中心
//
// 返回：
//   - *Handler: Handler 实例
func NewHandler(node interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}, watchHub *watch.WatchHub) *Handler {
	return &Handler{
		node:      node,
		watchHub:  watchHub,
	}
}

// ==================== API 路由 ====================

// RegisterRoutes 注册所有路由
//
// 参数：
//   - engine: Gin 引擎
func (h *Handler) RegisterRoutes(engine *gin.Engine) {
	// 健康检查
	engine.GET("/health", h.HealthCheck)

	// KV 存储 API
	v1 := engine.Group("/v1")
	{
		kv := v1.Group("/kv")
		{
			kv.POST("/put", h.Put)
			kv.GET("/get", h.Get)
			kv.DELETE("/delete", h.Delete)
		}

		// Watch API (SSE 长连接)
		v1.GET("/watch", h.Watch)
	}
}

// ==================== API 处理函数 ====================

// HealthCheck 健康检查
func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"time":   time.Now().Unix(),
	})
}

// Put 请求处理
// POST /v1/kv/put
func (h *Handler) Put(c *gin.Context) {
	// 解析请求体
	type PutRequest struct {
		Key   string `json:"key" binding:"required"`
		Value string `json:"value" binding:"required"`
	}

	var req PutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request: " + err.Error(),
		})
		return
	}

	// 写入存储
	err := h.node.Put([]byte(req.Key), []byte(req.Value))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "put failed: " + err.Error(),
		})
		return
	}

	// 返回成功
	c.JSON(http.StatusOK, gin.H{
		"message": "ok",
		"key":     req.Key,
	})
}

// Get 请求处理
// GET /v1/kv/get?key=xxx
func (h *Handler) Get(c *gin.Context) {
	// 获取查询参数
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "key is required",
		})
		return
	}

	// 读取数据
	value, err := h.node.Get([]byte(key))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "key not found",
		})
		return
	}

	// 返回成功
	c.JSON(http.StatusOK, gin.H{
		"key":   key,
		"value": string(value),
	})
}

// Delete 请求处理
// DELETE /v1/kv/delete?key=xxx
func (h *Handler) Delete(c *gin.Context) {
	// 获取查询参数
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "key is required",
		})
		return
	}

	// 先获取旧值（用于事件通知）
	prevValue, _ := h.node.Get([]byte(key))

	// 删除数据
	err := h.node.Delete([]byte(key))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "delete failed: " + err.Error(),
		})
		return
	}

	// 【挂载点】通知 Watch 客户端
	// 在 Delete 操作成功后，触发 WatchHub 的通知
	if h.watchHub != nil {
		h.watchHub.NotifyDelete(key, string(prevValue))
	}

	// 返回成功
	c.JSON(http.StatusOK, gin.H{
		"message": "ok",
		"key":     key,
	})
}

// ==================== Watch (SSE) ====================

// Watch 处理 Watch 请求
// GET /v1/watch?prefix=xxx
// 使用 Server-Sent Events (SSE) 实现长连接
func (h *Handler) Watch(c *gin.Context) {
	// 获取要监听的前缀
	prefix := c.DefaultQuery("prefix", "")

	// 设置响应头
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	// 注册 Watcher
	// 使用较大的缓冲区以支持高并发场景
	watcher := h.watchHub.Watch(prefix, 1000)
	defer h.watchHub.Unregister(watcher)

	// 创建客户端断开连接的检测
	clientGone := c.Request.Context().Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// 开始推送事件
	c.Status(http.StatusOK)
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "streaming not supported",
		})
		return
	}

	// 发送初始连接消息
	fmt.Fprintf(c.Writer, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-clientGone:
			// 客户端断开连接
			return

		case event := <-watcher.Ch:
			// 发送事件
			data, err := watch.EventToJSON(event)
			if err != nil {
				continue
			}
			fmt.Fprintf(c.Writer, "data: %s\n\n", data)
			flusher.Flush()

		case <-ticker.C:
			// 发送心跳，保持连接
			fmt.Fprintf(c.Writer, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

// ==================== 服务器启动 ====================

// Server HTTP 服务器
type Server struct {
	addr   string
	engine *gin.Engine
	handler *Handler
}

// NewServer 创建新的 Server
func NewServer(addr string, node interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}, watchHub *watch.WatchHub) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	handler := NewHandler(node, watchHub)
	handler.RegisterRoutes(engine)

	return &Server{
		addr:   addr,
		engine: engine,
		handler: handler,
	}
}

// Start 启动服务器
func (s *Server) Start() error {
	return s.engine.Run(s.addr)
}

// ServeHTTP 实现 http.Handler 接口
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.engine.ServeHTTP(w, r)
}

// StartTLS 启动 HTTPS 服务器
func (s *Server) StartTLS(certFile, keyFile string) error {
	return s.engine.RunTLS(s.addr, certFile, keyFile)
}
