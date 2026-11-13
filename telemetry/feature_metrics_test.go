package telemetry

import "testing"

// Every family of the opt-in head cache feature. Listed by
// exposed name on purpose: KnownFamilies() only knows families declared through
// Define*, so a family built with a raw prometheus constructor would silently
// skip Configure (and every customization) without failing any generic test.
// Real emission is owned by the feature tests (headcache TestMetrics_*, erpc
// TestWs_*); this only guards registration.
var featureFamilies = []string{
	"erpc_head_cache_requests_total", "erpc_head_cache_fetches_total",
	"erpc_head_cache_reorgs_total", "erpc_head_cache_snapshots_published_total",
	"erpc_head_cache_leader_acquisitions_total", "erpc_head_cache_leader",
	"erpc_head_cache_head_block", "erpc_head_cache_bytes",
	"erpc_head_cache_snapshot_timestamp_seconds", "erpc_head_cache_subscribers",
	"erpc_head_cache_subscriber_closed_total",
}

// Configure registers every feature family on the default registry, and the
// documented head_cache_* drop subject removes all of them.
func TestFeatureMetrics_ConfigureRegistration(t *testing.T) {
	cases := []struct {
		name       string
		opts       *Options
		registered bool
	}{
		{"no customizations", &Options{}, true},
		{"head_cache_* dropped", &Options{Customizations: []Customization{
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
