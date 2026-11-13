package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/clients"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/failsafe"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// These tests drive the upstream executor through the real HTTP client
// against an endpoint that accepts a request and never answers it, so the
// error codes are the ones the client produces in production.

// hungEndpoint answers request number n (1-based) for every n in answer and
// holds every other request until the caller goes away.
func hungEndpoint(t *testing.T, answer ...int32) (*url.URL, *atomic.Int32) {
	t.Helper()
	release := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		for _, a := range answer {
			if n == a {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
				return
			}
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return u, &calls
}

type breakerTimeoutTestExtractor struct{}

func (breakerTimeoutTestExtractor) Extract(*http.Response, *common.NormalizedResponse, *common.JsonRpcResponse, common.Upstream) error {
	return nil
}

func newBreakerTimeoutTestClient(t *testing.T, u *url.URL) clients.HttpJsonRpcClient {
	t.Helper()
	lg := zerolog.Nop()
	ups := common.NewFakeUpstream("rpc1")
	ups.Config().Type = common.UpstreamTypeEvm
	ups.Config().Endpoint = u.String()
	c, err := clients.NewGenericHttpJsonRpcClient(context.Background(), &lg, "prj1", ups, u, ups.Config().JsonRpc, nil, breakerTimeoutTestExtractor{})
	require.NoError(t, err)
	return c
}

func newBreakerTimeoutTestExecutor(t *testing.T, timeout time.Duration, failures uint, hedge *common.HedgePolicyConfig) *upstreamExecutor {
	t.Helper()
	lg := zerolog.Nop()
	ex, err := NewUpstreamExecutor(&common.FailsafeConfig{
		Timeout: &common.TimeoutPolicyConfig{Duration: common.NewStaticDuration(timeout)},
		Hedge:   hedge,
		CircuitBreaker: &common.CircuitBreakerPolicyConfig{
			FailureThresholdCount:    failures,
			FailureThresholdCapacity: failures,
			HalfOpenAfter:            common.Duration(time.Hour),
			SuccessThresholdCount:    1,
			SuccessThresholdCapacity: 1,
		},
	}, &lg)
	require.NoError(t, err)
	return ex
}

func breakerTimeoutTestRequest() *common.NormalizedRequest {
	return common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
}

// An upstream that never answers within its own attempt timeout fails the
// attempt, so its breaker opens once the failure threshold is reached.
func TestUpstreamExecutor_BreakerOpensOnEndpointAttemptTimeouts(t *testing.T) {
	u, _ := hungEndpoint(t)
	client := newBreakerTimeoutTestClient(t, u)
	ex := newBreakerTimeoutTestExecutor(t, 30*time.Millisecond, 3, nil)

	for i := 1; i <= 3; i++ {
		req := breakerTimeoutTestRequest()
		_, err := ex.Run(context.Background(), req, func(ctx context.Context, _ bool) (*common.NormalizedResponse, error) {
			return client.SendRequest(ctx, req)
		})
		require.Error(t, err)
		require.True(t, common.HasErrorCode(err, common.ErrCodeEndpointRequestTimeout), "attempt %d: %v", i, err)
		if i < 3 {
			require.Equal(t, failsafe.StateClosed, ex.Breaker().State(), "attempt %d", i)
		}
	}
	require.Equal(t, failsafe.StateOpen, ex.Breaker().State())
}

// The request's own budget (the network-level deadline) running out before
// the upstream attempt timeout says nothing about this upstream.
func TestUpstreamExecutor_BreakerIgnoresCallerDeadline(t *testing.T) {
	u, _ := hungEndpoint(t)
	client := newBreakerTimeoutTestClient(t, u)
	ex := newBreakerTimeoutTestExecutor(t, 5*time.Second, 1, nil)

	req := breakerTimeoutTestRequest()
	ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Millisecond, common.ErrDynamicTimeoutExceeded)
	defer cancel()
	_, err := ex.Run(ctx, req, func(ctx context.Context, _ bool) (*common.NormalizedResponse, error) {
		return client.SendRequest(ctx, req)
	})
	require.Error(t, err)
	require.True(t, common.HasErrorCode(err, common.ErrCodeEndpointRequestTimeout), "%v", err)
	require.Equal(t, failsafe.StateClosed, ex.Breaker().State())
	_, _, executions := ex.Breaker().Metrics()
	require.Zero(t, executions)
}

// A client that goes away does not count against the upstream.
func TestUpstreamExecutor_BreakerIgnoresClientCancel(t *testing.T) {
	u, _ := hungEndpoint(t)
	client := newBreakerTimeoutTestClient(t, u)
	ex := newBreakerTimeoutTestExecutor(t, 5*time.Second, 1, nil)

	req := breakerTimeoutTestRequest()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	_, err := ex.Run(ctx, req, func(ctx context.Context, _ bool) (*common.NormalizedResponse, error) {
		return client.SendRequest(ctx, req)
	})
	require.Error(t, err)
	require.Equal(t, failsafe.StateClosed, ex.Breaker().State())
	_, _, executions := ex.Breaker().Metrics()
	require.Zero(t, executions)
}

// The primary attempt that loses to a hedge is cancelled, not timed out,
// and does not count against the upstream.
func TestUpstreamExecutor_BreakerIgnoresLostHedge(t *testing.T) {
	u, calls := hungEndpoint(t, 2)
	client := newBreakerTimeoutTestClient(t, u)
	ex := newBreakerTimeoutTestExecutor(t, 5*time.Second, 1, &common.HedgePolicyConfig{
		Delay:    common.NewStaticDuration(20 * time.Millisecond),
		MaxCount: 1,
	})

	req := breakerTimeoutTestRequest()
	primaryDone := make(chan error, 1)
	resp, err := ex.Run(context.Background(), req, func(ctx context.Context, isHedge bool) (*common.NormalizedResponse, error) {
		r, err := client.SendRequest(ctx, req)
		if !isHedge {
			primaryDone <- err
		}
		return r, err
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int32(2), calls.Load())

	select {
	case perr := <-primaryDone:
		require.True(t, common.HasErrorCode(perr, common.ErrCodeEndpointRequestCanceled), "%v", perr)
	case <-time.After(2 * time.Second):
		t.Fatal("the losing primary attempt did not return")
	}
	// The executor records the primary's outcome right after it returns.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, failsafe.StateClosed, ex.Breaker().State())
	_, _, executions := ex.Breaker().Metrics()
	require.Zero(t, executions)
}

// A node that times out its own call answers at once with a JSON-RPC error.
// The client's query often causes that (a huge eth_getLogs range, a heavy
// trace), so it does not count against the upstream.
func TestUpstreamExecutor_BreakerIgnoresNodeReportedTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"context deadline exceeded"}}`))
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	client := newBreakerTimeoutTestClient(t, u)
	ex := newBreakerTimeoutTestExecutor(t, 5*time.Second, 1, nil)

	req := breakerTimeoutTestRequest()
	_, err = ex.Run(context.Background(), req, func(ctx context.Context, _ bool) (*common.NormalizedResponse, error) {
		return client.SendRequest(ctx, req)
	})
	require.Error(t, err)
	require.True(t, common.HasErrorCode(err, common.ErrCodeEndpointRequestTimeout), "%v", err)
	require.Equal(t, failsafe.StateClosed, ex.Breaker().State())
	_, _, executions := ex.Breaker().Metrics()
	require.Zero(t, executions)
}
