package blockstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/erpc/erpc/architecture/evm/logfilter"
	"github.com/erpc/erpc/common"
)

// emptyTrieRoot is the transactionsRoot of a block with no transactions.
const emptyTrieRoot = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"

// ErrInvalid wraps every validation failure of a payload.
var ErrInvalid = errors.New("blockstore: invalid payload")

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Header is the subset of a block header validation reads.
type Header struct {
	Number           int64
	Hash             string // lowercase
	ParentHash       string // lowercase
	LogsBloom        []byte // nil when absent or malformed
	TransactionsRoot string
	// TxHashes are the header's transaction hashes, lowercase, in order;
	// nil when the header lists none or lists full objects without hashes.
	TxHashes []string
	// TxListed: the header carries a transactions array.
	TxListed bool
}

type rawHeader struct {
	Number           string            `json:"number"`
	Hash             string            `json:"hash"`
	ParentHash       string            `json:"parentHash"`
	LogsBloom        string            `json:"logsBloom"`
	TransactionsRoot string            `json:"transactionsRoot"`
	Transactions     []json.RawMessage `json:"transactions"`
}

// ParseHeader decodes raw, a block as eth_getBlockBy* returns it, with
// hashes or full transactions. A transaction list that repeats a hash is
// rejected: a canonical block lists each transaction once, and receipts are
// compared against this set.
func ParseHeader(raw []byte) (*Header, error) {
	if isNull(raw) {
		return nil, invalid("null header")
	}
	var r rawHeader
	if err := common.SonicCfg.Unmarshal(raw, &r); err != nil {
		return nil, invalid("header: %v", err)
	}
	n, err := common.HexToInt64(r.Number)
	if err != nil || n < 0 {
		return nil, invalid("header number %q", r.Number)
	}
	if !isHexLen(r.Hash, 64) || !isHexLen(r.ParentHash, 64) {
		return nil, invalid("header hash")
	}
	h := &Header{
		Number:           n,
		Hash:             NormHash(r.Hash),
		ParentHash:       NormHash(r.ParentHash),
		TransactionsRoot: NormHash(r.TransactionsRoot),
		TxListed:         r.Transactions != nil,
	}
	if isHexLen(r.LogsBloom, 2*logfilter.BloomLen) {
		h.LogsBloom, _ = common.HexToBytes(r.LogsBloom)
	}
	seen := make(map[string]struct{}, len(r.Transactions))
	for _, t := range r.Transactions {
		var hash string
		if len(t) > 0 && t[0] == '"' {
			if err := common.SonicCfg.Unmarshal(t, &hash); err != nil {
				return nil, invalid("header transaction: %v", err)
			}
		} else {
			var obj struct {
				Hash string `json:"hash"`
			}
			if err := common.SonicCfg.Unmarshal(t, &obj); err != nil {
				return nil, invalid("header transaction: %v", err)
			}
			hash = obj.Hash
		}
		hash = NormHash(hash)
		if hash == "" {
			return nil, invalid("header transaction without hash")
		}
		if _, dup := seen[hash]; dup {
			return nil, invalid("header repeats transaction %s", hash)
		}
		seen[hash] = struct{}{}
		h.TxHashes = append(h.TxHashes, hash)
	}
	return h, nil
}

// LinksTo reports whether h is the child of parent.
func (h *Header) LinksTo(parent *Header) bool {
	return parent != nil && h.Number == parent.Number+1 && h.ParentHash == parent.Hash
}

// Empty: the header proves the block has no transactions.
func (h *Header) Empty() bool {
	return h.TransactionsRoot == emptyTrieRoot || (h.TxListed && len(h.TxHashes) == 0)
}

type rawReceipt struct {
	BlockHash       string   `json:"blockHash"`
	BlockNumber     string   `json:"blockNumber"`
	TransactionHash string   `json:"transactionHash"`
	LogsBloom       string   `json:"logsBloom"`
	Logs            []rawLog `json:"logs"`
}

type rawLog struct {
	Address         string   `json:"address"`
	Topics          []string `json:"topics"`
	BlockHash       string   `json:"blockHash"`
	BlockNumber     string   `json:"blockNumber"`
	TransactionHash string   `json:"transactionHash"`
	LogIndex        string   `json:"logIndex"`
	Removed         bool     `json:"removed"`
}

// ValidateReceipts checks raw, a block's eth_getBlockReceipts answer,
// against its header h before it is shared or read back from the store:
//   - non-null, and non-empty unless the header proves an empty block;
//   - every receipt and log names h's hash and number, no log is removed;
//   - transaction hashes are unique and, when h lists its transactions,
//     every one of them has a receipt (extra receipts, such as a chain's
//     system transactions the block body omits, are allowed);
//   - log indexes strictly increase across the block;
//   - the bloom recomputed from the logs (addresses and topics) equals h's
//     logsBloom, when h carries one. checkBloom=false skips this for a
//     chain profile whose blooms are not Ethereum's.
func ValidateReceipts(h *Header, raw []byte, checkBloom bool) error {
	if h == nil {
		return invalid("no header")
	}
	if isNull(raw) {
		return invalid("null receipts")
	}
	var rs []rawReceipt
	if err := common.SonicCfg.Unmarshal(raw, &rs); err != nil {
		return invalid("receipts: %v", err)
	}
	if len(rs) == 0 && !h.Empty() {
		return invalid("no receipts for a block with transactions")
	}
	if h.TxListed && len(rs) < len(h.TxHashes) {
		return invalid("%d receipts for %d transactions", len(rs), len(h.TxHashes))
	}
	seen := make(map[string]struct{}, len(rs))
	bloom := make([]byte, logfilter.BloomLen)
	prev := int64(-1)
	for i := range rs {
		r := &rs[i]
		if NormHash(r.BlockHash) != h.Hash {
			return invalid("receipt %d names block %q", i, r.BlockHash)
		}
		if r.BlockNumber != "" {
			if n, err := common.HexToInt64(r.BlockNumber); err != nil || n != h.Number {
				return invalid("receipt %d number %q", i, r.BlockNumber)
			}
		}
		tx := NormHash(r.TransactionHash)
		if tx == "" {
			return invalid("receipt %d without transactionHash", i)
		}
		if _, dup := seen[tx]; dup {
			return invalid("receipt %d repeats transaction %s", i, tx)
		}
		seen[tx] = struct{}{}
		for j := range r.Logs {
			l := &r.Logs[j]
			if NormHash(l.BlockHash) != h.Hash || NormHash(l.TransactionHash) != tx || l.Removed {
				return invalid("receipt %d log %d does not belong to it", i, j)
			}
			idx, err := common.HexToInt64(l.LogIndex)
			if err != nil || idx <= prev {
				return invalid("receipt %d log %d logIndex not increasing", i, j)
			}
			prev = idx
			if err := addLogBloom(bloom, l); err != nil {
				return err
			}
		}
	}
	for _, t := range h.TxHashes {
		if _, ok := seen[t]; !ok {
			return invalid("no receipt for transaction %s", t)
		}
	}
	if checkBloom && h.LogsBloom != nil && !bytes.Equal(bloom, h.LogsBloom) {
		return invalid("logsBloom mismatch (receipts incomplete)")
	}
	return nil
}

func addLogBloom(bloom []byte, l *rawLog) error {
	a, err := common.HexToBytes(l.Address)
	if err != nil || len(a) != 20 {
		return invalid("log address %q", l.Address)
	}
	logfilter.BloomAdd(bloom, a)
	for _, t := range l.Topics {
		b, err := common.HexToBytes(t)
		if err != nil || len(b) != 32 {
			return invalid("log topic %q", t)
		}
		logfilter.BloomAdd(bloom, b)
	}
	return nil
}

// BlockLogs are a block's complete logs, verbatim, in order, with the
// address and topics matching reads.
type BlockLogs struct {
	Number int64
	Hash   string
	Logs   []json.RawMessage
	addr   []string
	topics [][]string
}

// LogsOf extracts the logs of a receipts answer that passed
// ValidateReceipts.
func LogsOf(number int64, hash string, receipts []byte) (*BlockLogs, error) {
	var rs []struct {
		Logs []json.RawMessage `json:"logs"`
	}
	if err := common.SonicCfg.Unmarshal(receipts, &rs); err != nil {
		return nil, invalid("receipts: %v", err)
	}
	b := &BlockLogs{Number: number, Hash: NormHash(hash)}
	for i := range rs {
		for _, raw := range rs[i].Logs {
			var l rawLog
			if err := common.SonicCfg.Unmarshal(raw, &l); err != nil {
				return nil, invalid("log: %v", err)
			}
			b.Logs = append(b.Logs, raw)
			b.addr = append(b.addr, l.Address)
			b.topics = append(b.topics, l.Topics)
		}
	}
	return b, nil
}

// Match is the logs of b that f matches, in order (geth filterLogs).
func (b *BlockLogs) Match(f *logfilter.LogsFilter) []json.RawMessage {
	var out []json.RawMessage
	for i, raw := range b.Logs {
		if f.Matches(b.addr[i], b.topics[i]) {
			out = append(out, raw)
		}
	}
	return out
}

func isNull(raw []byte) bool {
	r := bytes.TrimSpace(raw)
	return len(r) == 0 || bytes.Equal(r, []byte("null"))
}

func isHexLen(s string, n int) bool {
	if len(s) != 2+n || !strings.HasPrefix(strings.ToLower(s), "0x") {
		return false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
