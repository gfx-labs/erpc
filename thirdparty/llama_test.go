package thirdparty

import (
	"net/http"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/require"
)

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
