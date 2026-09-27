package trace

import (
	"math/rand/v2"
	"sync"
)

// rollMu/rollRng 让概率判定可被测试替换（SetSamplingRollForTest），
// 同时保持并发安全。
var (
	rollMu  sync.Mutex
	rollRng = rand.New(rand.NewPCG(1, 2))
)

// rollSampling 返回 true 的概率约等于 fraction（fraction=1 恒真）。
func rollSampling(fraction float64) bool {
	if fraction >= 1 {
		return true
	}
	rollMu.Lock()
	defer rollMu.Unlock()
	return rollRng.Float64() < fraction
}

// SetSamplingRollForTest 注入确定性随机源，仅用于测试。
func SetSamplingRollForTest(seed uint64) {
	rollMu.Lock()
	defer rollMu.Unlock()
	rollRng = rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}
