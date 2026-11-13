package integrity

import (
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/require"
)

// txFromObject must yield exactly the Tx view (and accept/reject) of the
// original re-encode and decode of each transaction object.
func checkTxViewParity(t *testing.T, objJSON []byte) {
	var obj map[string]any
	if common.SonicCfg.Unmarshal(objJSON, &obj) != nil || obj == nil {
		return
	}
	want, wantOK := txFromObjectRoundTrip(obj)
	got, gotOK := txFromObject(obj)
	require.Equal(t, wantOK, gotOK, "accept/reject differs for %q", objJSON)
	require.Equal(t, want, got, "view differs for %q", objJSON)
}

var txViewSeeds = []string{
	`{"hash":"0x1","from":"0xa","to":null,"gas":"0x5208","blockHash":"0xb","blockNumber":"0x1","transactionIndex":"0x0","input":"0x"}`,
	`{"hash":"0x1","Hash":"0x2"}`,
	`{"HASH":"0x1"}`,
	`{"ha\u017fh":"0x1"}`,
	`{"gas":5208}`,
	`{"to":{"x":1}}`,
	`{"from":["a"]}`,
	`{"hash":true}`,
	`{"hash":"\u0000\ud800x"}`,
	`{"transactionIndex":"0x1","transactionindex":"0x2"}`,
	`{"blockNumber":null,"value":"0x1"}`,
	`{}`,
}

func TestTxFromObjectParity(t *testing.T) {
	for _, s := range txViewSeeds {
		checkTxViewParity(t, []byte(s))
	}
	// Every transaction of the real mainnet block.
	fx := loadRealFixtures(t, "mainnet")["block-full"]
	var block struct {
		Transactions []map[string]any `json:"transactions"`
	}
	require.NoError(t, common.SonicCfg.Unmarshal(fx.Result, &block))
	require.NotEmpty(t, block.Transactions)
	for _, obj := range block.Transactions {
		want, wantOK := txFromObjectRoundTrip(obj)
		got, gotOK := txFromObject(obj)
		require.True(t, wantOK)
		require.Equal(t, wantOK, gotOK)
		require.Equal(t, want, got)
	}
}

func FuzzTxFromObjectParity(f *testing.F) {
	for _, s := range txViewSeeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) { checkTxViewParity(t, b) })
}
