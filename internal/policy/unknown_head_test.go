package policy_test

import (
	"context"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/internal/policy"
	"github.com/erpc/erpc/internal/policy/stdlib"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// newLagGateEngine registers rpc1..rpc3 under a policy that only applies the
// block-head lag gates, and returns the engine with its tracker and pool.
func newLagGateEngine(t *testing.T) (*policy.Engine, *health.Tracker, []common.Upstream) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := zerolog.Nop()
	tracker := health.NewTracker(&logger, "test", time.Minute)
	tracker.Bootstrap(ctx)

	cfg := &common.SelectionPolicyConfig{
		EvalInterval:         common.Duration(0),
		EvalTimeout:          common.Duration(500 * time.Millisecond),
		EvalScope:            common.EvalScopeNetwork,
		EvalFunc:             "(ups) => ups.excludeIf(any(blockNumberLagAbove(16), blockSecondsLagAbove(30)))",
		DisableTickerForTest: true,
	}
	require.NoError(t, cfg.SetDefaults())

	engine := policy.NewEngine(ctx, &logger, "p1", tracker, stdlib.Install, nil)
	t.Cleanup(engine.Stop)

	ups := []common.Upstream{
		&fakeUpstream{id: "rpc1"},
		&fakeUpstream{id: "rpc2"},
		&fakeUpstream{id: "rpc3"},
	}
	require.NoError(t, engine.RegisterNetwork("evm:1", "", func() []common.Upstream { return ups }, cfg))
	return engine, tracker, ups
}

// An upstream that has never reported a head (a node stuck at block 0, or one
// whose polls all fail) must not read as "at the tip" once its peers have
// established a network head: the lag gates have to be able to exclude it.
func TestEngine_LagGate_ExcludesUpstreamWithNoHead(t *testing.T) {
	engine, tracker, ups := newLagGateEngine(t)

	// rpc3 never reports a head.
	for _, head := range []int64{1000, 1001} {
		tracker.SetLatestBlockNumber(ups[0], head, 0)
		tracker.SetLatestBlockNumber(ups[1], head, 0)
	}
	policy.TickForTest(engine, "evm:1", "*")

	require.Equal(t, []string{"rpc1", "rpc2"}, ids(engine.GetOrdered("evm:1", "*", "*")))
}

// At cold start nobody has a head, so the network head is unknown too and the
// lag gates must exclude nobody.
func TestEngine_LagGate_ColdStartExcludesNobody(t *testing.T) {
	engine, _, _ := newLagGateEngine(t)

	policy.TickForTest(engine, "evm:1", "*")

	require.Equal(t, []string{"rpc1", "rpc2", "rpc3"}, ids(engine.GetOrdered("evm:1", "*", "*")))
}
