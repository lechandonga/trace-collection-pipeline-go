package trace

import (
	"sync"
	"time"
)

// Clock 是可被测试推进的时钟：Now 返回当前逻辑时间，
// Advance 仅推进逻辑时钟，不依赖真实睡眠，便于确定性测试。
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock 创建一个起始时间固定的逻辑时钟。
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now 返回当前逻辑时间。
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 把逻辑时间向前推进 d。
func (c *Clock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}
