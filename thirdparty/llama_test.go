package thirdparty

import (
	"net/http"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLlamaVendor_Name(t *testing.T) {
	assert.Equal(t, "llama", CreateLlamaVendor().Name())
}

func TestLlamaVendor_SupportsNetwork(t *testing.T) {
	svtCheckSupports(t, CreateLlamaVendor(), common.VendorSettings{}, []svtSupportCase{
		{"evm:1", true, false},
		{"evm:56", true, false},
		{"evm:999999999", false, false},
		{"solana:mainnet", false, false},
		{"evm:abc", false, true},
	})
}

func TestLlamaVendor_GenerateConfigs(t *testing.T) {
	key := common.VendorSettings{"apiKey": "KEY"}
	svtCheckURLs(t, CreateLlamaVendor, key, map[int64]string{
		1:  "https://eth.llamarpc.com/KEY",
		56: "https://binance.llamarpc.com/KEY",
	})
	svtCheckGenErrors(t, CreateLlamaVendor, []svtErrCase{
		{"missing apiKey", svtEvm(1), common.VendorSettings{}, "apiKey is required in llama settings"},
		{"zero chain", svtEvm(0), key, "llama vendor requires upstream.evm.chainId to be defined"},
		{"unsupported chain", svtEvm(999999999), key, "unsupported network chain ID for Llama: 999999999"},
	})
	svtCheckPreset(t, CreateLlamaVendor(), "https://eth.llamarpc.com/own")
}

func TestLlamaVendor_GetVendorSpecificErrorIfAny(t *testing.T) {
	v := CreateLlamaVendor()

	err, details := vendorErr(t, v, "eth_getLogs", 429, -32000, "cloudflare error code: 1015 rate limited", "d")
	require.Error(t, err)
	assert.True(t, common.HasErrorCode(err, common.ErrCodeEndpointCapacityExceeded), "got %v", err)
	assert.Equal(t, common.JsonRpcErrorCapacityExceeded, normalizedCode(t, err))
	assert.Equal(t, "d", details["data"])

	svtCheckDataOnly(t, v)
}

func TestLlamaVendor_OwnsUpstream(t *testing.T) {
	svtAssertOwns(t, CreateLlamaVendor(), map[string]bool{
		"https://eth.llamarpc.com/key": true,
		"llama://key":                  false,
		"https://rpc.ankr.com/eth/key": false,
	})
}

// A raw Cloudflare "error code: 1015" page fails JSON parsing, so its body only
// reaches the vendor as the parse exception's data. Only that shape is
// reclassified as capacity exceeded; every other unparseable body, and the same
// text carried by a non-parse error, keeps its generic classification.
func TestLlamaVendor_Cloudflare1015Classification(t *testing.T) {
	parseErr := func(body string) *common.ErrJsonRpcExceptionExternal {
		return common.NewErrJsonRpcExceptionExternal(int(common.JsonRpcErrorParseException),
			"cannot parse json-rpc response: invalid character 'e' looking for beginning of value", body)
	}
	cases := []struct {
		name     string
		err      *common.ErrJsonRpcExceptionExternal
		capacity bool
	}{
		{"exact", parseErr("error code: 1015"), true},
		{"trailing newline", parseErr("error code: 1015\n"), true},
		{"surrounding text", parseErr("  error code: 1015 extra"), true},
		{"other cloudflare code", parseErr("error code: 1020"), false},
		{"html gateway page", parseErr("<html>502 Bad Gateway</html>"), false},
		{"empty body", parseErr(""), false},
		{"1015 not at start", parseErr("rate limited error code: 1015"), false},
		{"non-parse error with 1015 data", common.NewErrJsonRpcExceptionExternal(-32000, "boom", "error code: 1015"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			jrr := &common.JsonRpcResponse{Error: tc.err}
			err := CreateLlamaVendor().GetVendorSpecificErrorIfAny(nil, &http.Response{StatusCode: 200}, jrr, map[string]interface{}{})
			if tc.capacity {
				require.True(t, common.HasErrorCode(err, common.ErrCodeEndpointCapacityExceeded), "%v", err)
			} else {
				require.Nil(t, err, "must keep generic classification")
			}
		})
	}
}
