package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ==================== 存储指标 ====================

	// StorageKeysTotal 总键数量
	StorageKeysTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_storage_keys_total",
		Help: "Total number of keys in storage",
	})

	// StorageDataFilesTotal 数据文件总数
	StorageDataFilesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_storage_data_files_total",
		Help: "Total number of data files",
	})

	// StorageActiveFileSize 当前活跃文件大小
	StorageActiveFileSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_storage_active_file_size_bytes",
		Help: "Size of the active data file in bytes",
	})

	// StoragePutTotal Put 操作总数
	StoragePutTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_put_total",
		Help: "Total number of Put operations",
	})

	// StorageGetTotal Get 操作总数
	StorageGetTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_get_total",
		Help: "Total number of Get operations",
	})

	// StorageGetHitTotal Get 命中总数
	StorageGetHitTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_get_hit_total",
		Help: "Total number of Get operations that found the key",
	})

	// StorageGetMissTotal Get 未命中总数
	StorageGetMissTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_get_miss_total",
		Help: "Total number of Get operations that did not find the key",
	})

	// StorageDeleteTotal Delete 操作总数
	StorageDeleteTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_delete_total",
		Help: "Total number of Delete operations",
	})

	// StorageBloomFilterCheckTotal 布隆过滤器检查总数
	StorageBloomFilterCheckTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_bloom_filter_check_total",
		Help: "Total number of Bloom filter checks",
	})

	// StorageBloomFilterHitTotal 布隆过滤器命中总数
	StorageBloomFilterHitTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_storage_bloom_filter_hit_total",
		Help: "Total number of Bloom filter positive results",
	})

	// ==================== Raft 指标 ====================

	// RaftCommitIndex 当前提交索引
	RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_raft_commit_index",
		Help: "Current Raft commit index",
	})

	// RaftAppliedIndex 当前已应用索引
	RaftAppliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_raft_applied_index",
		Help: "Current Raft applied index",
	})

	// RaftLogSize Raft 日志大小
	RaftLogSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_raft_log_size",
		Help: "Number of entries in Raft log",
	})

	// RaftAppliedIndexLag 提交索引与应用索引的差距
	RaftAppliedIndexLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_raft_applied_index_lag",
		Help: "Lag between commit index and applied index",
	})

	// RaftApplyTotal Raft Apply 操作总数
	RaftApplyTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_raft_apply_total",
		Help: "Total number of Raft Apply operations",
	})

	// RaftApplyDurationMs Raft Apply 耗时（毫秒）
	RaftApplyDurationMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tidekv_raft_apply_duration_milliseconds",
		Help:    "Duration of Raft Apply operations in milliseconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})

	// RaftIsLeader 当前是否为 Leader
	RaftIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_raft_is_leader",
		Help: "Whether this node is the leader (1 = yes, 0 = no)",
	})

	// ==================== Watch 指标 ====================

	// WatchClientsTotal 当前 Watch 客户端数量
	WatchClientsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tidekv_watch_clients_total",
		Help: "Total number of active watch clients",
	})

	// WatchEventsTotal Watch 事件总数
	WatchEventsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_watch_events_total",
		Help: "Total number of watch events sent",
	})

	// WatchEventDurationMs Watch 事件发送耗时（毫秒）
	WatchEventDurationMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tidekv_watch_event_duration_milliseconds",
		Help:    "Duration of watch event sending in milliseconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})

	// ==================== HTTP API 指标 ====================

	// HTTPRequestsTotal HTTP 请求总数
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tidekv_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDurationMs HTTP 请求耗时（毫秒）
	HTTPRequestDurationMs = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tidekv_http_request_duration_milliseconds",
			Help:    "Duration of HTTP requests in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		},
		[]string{"method", "path"},
	)

	// ==================== 连接池指标 ====================

	// ConnectionPoolGetTotal 连接池获取次数
	ConnectionPoolGetTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_connection_pool_get_total",
		Help: "Total number of connection pool gets",
	})

	// ConnectionPoolPutTotal 连接池归还次数
	ConnectionPoolPutTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tidekv_connection_pool_put_total",
		Help: "Total number of connection pool puts",
	})
)

// RecordPut 记录一次 Put 操作
func RecordPut() {
	StoragePutTotal.Inc()
}

// RecordGet 记录一次 Get 操作（hit 表示是否命中）
func RecordGet(hit bool) {
	StorageGetTotal.Inc()
	if hit {
		StorageGetHitTotal.Inc()
	} else {
		StorageGetMissTotal.Inc()
	}
}

// RecordDelete 记录一次 Delete 操作
func RecordDelete() {
	StorageDeleteTotal.Inc()
}

// RecordBloomFilterCheck 记录一次布隆过滤器检查（hit 表示是否可能存在）
func RecordBloomFilterCheck(hit bool) {
	StorageBloomFilterCheckTotal.Inc()
	if hit {
		StorageBloomFilterHitTotal.Inc()
	}
}

// RecordApply 记录一次 Raft Apply
func RecordApply(durationMs float64) {
	RaftApplyTotal.Inc()
	RaftApplyDurationMs.Observe(durationMs)
}

// RecordHTTPRequest 记录一次 HTTP 请求
func RecordHTTPRequest(method, path, status string, durationMs float64) {
	HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
	HTTPRequestDurationMs.WithLabelValues(method, path).Observe(durationMs)
}

// RecordWatchEvent 记录一次 Watch 事件发送
func RecordWatchEvent(durationMs float64) {
	WatchEventsTotal.Inc()
	WatchEventDurationMs.Observe(durationMs)
}
