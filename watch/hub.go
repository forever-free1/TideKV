package watch

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/plar/go-adaptive-radix-tree"
)

// ==================== 事件定义 ====================

// EventType 定义事件类型
type EventType string

const (
	EventPut    EventType = "put"
	EventDelete EventType = "delete"
)

// Event 表示键值变更事件
type Event struct {
	Type      EventType `json:"type"`       // 事件类型：put 或 delete
	Key       string    `json:"key"`        // 变更的键
	Value     string    `json:"value,omitempty"` // 变更的值（仅 put 事件有值）
	PrevValue string    `json:"prev_value,omitempty"` // 变更前的值
}

// ==================== Watcher 定义 ====================

// Watcher 表示一个watcher客户端
// 包含用于推送事件的 channel
type Watcher struct {
	// 用于推送事件的通道
	// 当有键值变更时，事件会通过这个 channel 发送给客户端
	Ch chan *Event

	// 该 watcher 关注的前缀
	// 如果为空字符串，表示关注所有键
	Prefix string

	// 是否已关闭
	closed bool
}

// NewWatcher 创建新的 Watcher
//
// 参数：
//   - prefix: 关注的前缀，为空表示关注所有
//   - bufferSize: 事件通道的缓冲区大小
//
// 返回：
//   - *Watcher: Watcher 实例
func NewWatcher(prefix string, bufferSize int) *Watcher {
	return &Watcher{
		Ch:     make(chan *Event, bufferSize),
		Prefix: prefix,
	}
}

// IsMatch 检查事件是否匹配该 Watcher 的前缀
func (w *Watcher) IsMatch(event *Event) bool {
	// 如果前缀为空，表示匹配所有
	if w.Prefix == "" {
		return true
	}
	// 检查事件的 key 是否以指定前缀开头
	return strings.HasPrefix(event.Key, w.Prefix)
}

// Close 关闭 Watcher
func (w *Watcher) Close() {
	if !w.closed {
		close(w.Ch)
		w.closed = true
	}
}

// ==================== WatchHub 定义 ====================

// WatchHub 事件通知中心
// 负责管理所有的 Watcher，并将键值变更事件分发到对应的 Watcher
type WatchHub struct {
	// 所有的 watcher 列表
	watchers []*Watcher

	// 保护 watchers 列表的锁
	mu sync.RWMutex

	// 用于前缀匹配的 ART 树
	// key: 前缀字符串
	// value: 关注该前缀的所有 watcher 列表
	prefixTree art.Tree

	// 统计信息
	watcherCount int64
}

// NewWatchHub 创建新的 WatchHub
//
// 返回：
//   - *WatchHub: WatchHub 实例
func NewWatchHub() *WatchHub {
	return &WatchHub{
		watchers:    make([]*Watcher, 0),
		prefixTree:  art.New(),
	}
}

// ==================== Watcher 管理 ====================

// Watch 注册一个新的 Watcher
// 当有键值变更时，会向该 Watcher 的 channel 发送事件
//
// 参数：
//   - prefix: 关注的前缀，为空表示关注所有键
//   - bufferSize: 事件通道的缓冲区大小
//
// 返回：
//   - *Watcher: 注册的 Watcher 实例
func (h *WatchHub) Watch(prefix string, bufferSize int) *Watcher {
	watcher := NewWatcher(prefix, bufferSize)

	h.mu.Lock()
	defer h.mu.Unlock()

	// 将 watcher 添加到列表
	h.watchers = append(h.watchers, watcher)

	// 如果指定了前缀，将其添加到前缀树以便快速匹配
	if prefix != "" {
		// 获取该前缀已有的 watcher 列表
		val, _ := h.prefixTree.Search(art.Key(prefix))
		var list []*Watcher
		if val != nil {
			list = val.([]*Watcher)
		}
		list = append(list, watcher)
		h.prefixTree.Insert(art.Key(prefix), list)
	}

	// 更新统计
	h.watcherCount++

	return watcher
}

// Unregister 取消注册一个 Watcher
//
// 参数：
//   - watcher: 要取消注册的 Watcher
func (h *WatchHub) Unregister(watcher *Watcher) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 从 watchers 列表中移除
	for i, w := range h.watchers {
		if w == watcher {
			h.watchers = append(h.watchers[:i], h.watchers[i+1:]...)
			break
		}
	}

	// 如果有前缀，从前缀树中移除
	if watcher.Prefix != "" {
		val, found := h.prefixTree.Search(art.Key(watcher.Prefix))
		if found {
			list := val.([]*Watcher)
			for i, w := range list {
				if w == watcher {
					list = append(list[:i], list[i+1:]...)
					break
				}
			}
			if len(list) > 0 {
				h.prefixTree.Insert(art.Key(watcher.Prefix), list)
			} else {
				h.prefixTree.Delete(art.Key(watcher.Prefix))
			}
		}
	}

	// 关闭 watcher
	watcher.Close()

	// 更新统计
	h.watcherCount--
}

// ==================== 事件通知 ====================

// Notify 通知所有匹配的 Watcher 有键值变更
// 这个方法会在 Raft Apply 成功后调用
//
// 参数：
//   - event: 变更事件
func (h *WatchHub) Notify(event *Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// 遍历所有 watcher，检查是否匹配
	for _, watcher := range h.watchers {
		// 跳过已关闭的 watcher
		if watcher.closed {
			continue
		}

		// 检查事件是否匹配该 watcher 的前缀
		if watcher.IsMatch(event) {
			// 非阻塞发送，避免阻塞主流程
			select {
			case watcher.Ch <- event:
			default:
				// 如果 channel 已满，跳过这个 watcher
				// 实际生产环境可以考虑记录警告日志
			}
		}
	}
}

// NotifyPut 通知 Put 事件
// 【挂载点】在 Raft FSM 的 Apply 方法中，Put 操作成功后调用
//
// 参数：
//   - key: 变更的键
//   - value: 变更后的值
func (h *WatchHub) NotifyPut(key string, value string) {
	event := &Event{
		Type:  EventPut,
		Key:   key,
		Value: value,
	}
	h.Notify(event)
}

// NotifyDelete 通知 Delete 事件
// 【挂载点】在 Raft FSM 的 Apply 方法中，Delete 操作成功后调用
//
// 参数：
//   - key: 变更的键
//   - prevValue: 删除前的值（可选，用于完整事件信息）
func (h *WatchHub) NotifyDelete(key string, prevValue string) {
	event := &Event{
		Type:      EventDelete,
		Key:       key,
		PrevValue: prevValue,
	}
	h.Notify(event)
}

// ==================== 前缀匹配（利用 ART 特性） ====================

// WatchPrefix 监听指定前缀的所有键
// 利用 ART 树的前缀遍历特性，可以高效地找到所有匹配前缀的 watcher
//
// 参数：
//   - prefix: 要监听的前缀
//   - bufferSize: 事件通道的缓冲区大小
//
// 返回：
//   - *Watcher: 注册的 Watcher 实例
func (h *WatchHub) WatchPrefix(prefix string, bufferSize int) *Watcher {
	return h.Watch(prefix, bufferSize)
}

// FindWatchersByPrefix 找到所有关注指定前缀的 watcher
// 这个方法利用 ART 树的前缀匹配特性
//
// 参数：
//   - key: 变更的键
//
// 返回：
//   - []*Watcher: 匹配的所有 watcher
func (h *WatchHub) FindWatchersByPrefix(key string) []*Watcher {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []*Watcher

	// 遍历前缀树，查找所有匹配的前缀
	// ART 树支持前缀查询，可以找到所有以给定前缀开头的键
	prefixes := h.findMatchingPrefixes(key)

	for _, prefix := range prefixes {
		val, found := h.prefixTree.Search(art.Key(prefix))
		if found {
			list := val.([]*Watcher)
			result = append(result, list...)
		}
	}

	// 也添加关注所有键的 watcher
	for _, watcher := range h.watchers {
		if watcher.Prefix == "" && !containsWatcher(result, watcher) {
			result = append(result, watcher)
		}
	}

	return result
}

// findMatchingPrefixes 找到所有可能匹配的前缀
// 这是一个简化的实现，实际可以使用更高效的算法
func (h *WatchHub) findMatchingPrefixes(key string) []string {
	var prefixes []string

	// 从最短的前缀开始尝试
	for i := 1; i <= len(key); i++ {
		prefix := key[:i]
		_, found := h.prefixTree.Search(art.Key(prefix))
		if found {
			prefixes = append(prefixes, prefix)
		}
	}

	return prefixes
}

// ==================== 工具方法 ====================

// Count 返回当前注册的 watcher 数量
func (h *WatchHub) Count() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.watcherCount
}

// Close 关闭所有 watcher
func (h *WatchHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, watcher := range h.watchers {
		watcher.Close()
	}
	h.watchers = nil
	h.prefixTree = art.New()
	h.watcherCount = 0
}

// String 返回 WatchHub 的字符串描述
func (h *WatchHub) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return fmt.Sprintf("WatchHub{watchers: %d}", len(h.watchers))
}

// ==================== 辅助函数 ====================

// containsWatcher 检查 watcher 列表中是否包含指定的 watcher
func containsWatcher(list []*Watcher, w *Watcher) bool {
	for _, x := range list {
		if x == w {
			return true
		}
	}
	return false
}

// EventToJSON 将事件转换为 JSON 字符串
func EventToJSON(event *Event) (string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ParseEventFromJSON 从 JSON 字符串解析事件
func ParseEventFromJSON(data string) (*Event, error) {
	var event Event
	err := json.Unmarshal([]byte(data), &event)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// 确保 WatchHub 实现了相关接口
var _ interface{} = (*WatchHub)(nil)
