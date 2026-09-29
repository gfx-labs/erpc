package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Every family of the opt-in head cache and WebSocket features. Listed by
// exposed name on purpose: KnownFamilies() only knows families declared through
// Define*, so a family built with a raw prometheus constructor would silently
// skip Configure (and every customization) without failing any generic test.
var featureFamilies = []string{
	"erpc_head_cache_requests_total", "erpc_head_cache_fetches_total",
	"erpc_head_cache_reorgs_total", "erpc_head_cache_snapshots_published_total",
	"erpc_head_cache_leader_acquisitions_total", "erpc_head_cache_leader",
	"erpc_head_cache_head_block", "erpc_head_cache_bytes",
	"erpc_head_cache_snapshot_timestamp_seconds", "erpc_head_cache_subscribers",
	"erpc_head_cache_subscriber_closed_total",
	"erpc_ws_connections", "erpc_ws_subscriptions",
	"erpc_ws_notifications_total", "erpc_ws_connections_closed_total",
}

// Configure registers every feature family on the default registry, and the
// documented ws_* / head_cache_* drop subjects remove all of them.
func TestFeatureMetrics_ConfigureRegistration(t *testing.T) {
	cases := []struct {
		name       string
		opts       *Options
		registered bool
	}{
		{"no customizations", &Options{}, true},
		{"ws_* and head_cache_* dropped", &Options{Customizations: []Customization{
			{Subject: "ws_*", Action: ActionDrop},
			{Subject: "head_cache_*", Action: ActionDrop},
		}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := withFreshRegistry(t)
			if err := Configure(tc.opts); err != nil {
				t.Fatal(err)
			}
			registered := registeredFamilies(reg)
			for _, f := range featureFamilies {
				if _, ok := registered[f]; ok != tc.registered {
					t.Errorf("%s registered=%v, want %v", f, ok, tc.registered)
				}
			}
		})
	}
}

// Temporary: the WS counters have no lifecycle assertion yet, so their exposure
// on a real scrape is only guarded here.
func TestFeatureMetrics_WsSeriesScraped(t *testing.T) {
	reg := withFreshRegistry(t)
	if err := Configure(&Options{}); err != nil {
		t.Fatal(err)
	}
	MetricWsConnections.WithLabelValues("p", "evm:1").Inc()
	MetricWsSubscriptions.WithLabelValues("p", "evm:1", "newHeads").Inc()
	CounterHandle(MetricWsNotificationsTotal, "p", "evm:1", "newHeads").Inc()
	CounterHandle(MetricWsClosedTotal, "p", "evm:1", "client").Inc()
	t.Cleanup(func() {
		MetricWsConnections.Reset()
		MetricWsSubscriptions.Reset()
	})

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	body := string(b)
	for _, l := range []string{
		`erpc_ws_connections{network="evm:1",project="p"} 1`,
		`erpc_ws_subscriptions{kind="newHeads",network="evm:1",project="p"} 1`,
		`erpc_ws_notifications_total{kind="newHeads",network="evm:1",project="p"} 1`,
		`erpc_ws_connections_closed_total{network="evm:1",project="p",reason="client"} 1`,
	} {
		if !strings.Contains(body, l+"\n") {
			t.Errorf("scrape missing %q", l)
		}
	}
}
