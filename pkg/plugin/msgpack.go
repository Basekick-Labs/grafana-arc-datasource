package plugin

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// queryMsgpack executes a query against Arc's /api/v1/query/msgpack endpoint
// (stable since Arc 26.09.1) and returns the decoded Grafana DataFrame. The
// response is a single columnar MessagePack envelope, decoded in a streaming
// pass so no intermediate map[string]interface{} tree is materialized.
func queryMsgpack(ctx context.Context, settings *ArcInstanceSettings, sql string) (*data.Frame, error) {
	start := time.Now()
	body, err := settings.doRequest(ctx, "/api/v1/query/msgpack", map[string]any{"sql": sql})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	// The element cap bounds up-front slice allocations from length headers:
	// a hostile/buggy server can claim a 4-billion-element array in 5 wire
	// bytes, and http.MaxBytesReader only caps bytes actually read. Every
	// element costs at least one body byte, so the response cap is a sound
	// per-array element bound.
	frame, err := MsgpackToDataFrame(body, settings.maxResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Arc msgpack response: %w", err)
	}

	duration := time.Since(start)
	log.DefaultLogger.Debug("Msgpack query completed",
		"duration_ms", duration.Milliseconds(),
		"rows", frame.Rows(),
		"fields", len(frame.Fields),
	)

	frame.Meta = &data.FrameMeta{
		ExecutedQueryString: sql,
		Custom: map[string]interface{}{
			"executionTime": duration.Milliseconds(),
		},
	}

	return frame, nil
}

// MsgpackToDataFrame decodes Arc's columnar MessagePack query envelope into a
// Grafana DataFrame. The envelope (Arc internal/api/query_msgpack.go) is:
//
//	map {
//	  "success":           bool
//	  "columns":           [string...]
//	  "types":             [string...]   // frozen vocabulary, see decodeMsgpackColumn
//	  "data":              [[...]...]    // COLUMN-major: one array per column
//	  "row_count":         uint
//	  "execution_time_ms": uint
//	  "timestamp":         string
//	}
//
// Key order is fixed by the server (columns and types always precede data),
// but this decoder only requires that ordering rather than assuming the full
// sequence, and skips unknown keys so additive server changes don't break it.
// Nulls are msgpack nil, in place inside the column arrays. Timestamps are
// the standard msgpack timestamp extension (type -1), already UTC instants.
//
// maxElems caps every array-length header before allocation. msgpack length
// prefixes are attacker-controlled and DecodeArrayLen returns -1 for a nil
// value, so lengths are validated through checkedLen at every site: without
// it, `make` with a negative or multi-billion length panics or OOMs the
// plugin process before a single element byte is read.
func MsgpackToDataFrame(r io.Reader, maxElems int64) (*data.Frame, error) {
	dec := msgpack.NewDecoder(r)

	mapLen, err := dec.DecodeMapLen()
	if err != nil {
		return nil, fmt.Errorf("invalid envelope: %w", err)
	}

	var columns, types []string
	var fields []*data.Field
	sawData := false

	for i := 0; i < mapLen; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return nil, fmt.Errorf("invalid envelope key: %w", err)
		}
		switch key {
		case "columns":
			if columns, err = decodeStringArray(dec, maxElems); err != nil {
				return nil, fmt.Errorf("invalid columns: %w", err)
			}
		case "types":
			if types, err = decodeStringArray(dec, maxElems); err != nil {
				return nil, fmt.Errorf("invalid types: %w", err)
			}
		case "data":
			// The server writes columns and types before data; a data key
			// arriving first means we can't type the columns — malformed.
			if columns == nil || types == nil {
				return nil, fmt.Errorf("malformed envelope: data precedes columns/types")
			}
			if len(columns) != len(types) {
				return nil, fmt.Errorf("malformed envelope: %d columns but %d types", len(columns), len(types))
			}
			numCols, err := checkedLen(dec, maxElems)
			if err != nil {
				return nil, fmt.Errorf("invalid data array: %w", err)
			}
			if numCols != len(columns) {
				return nil, fmt.Errorf("malformed envelope: %d data columns but %d column names", numCols, len(columns))
			}
			fields = make([]*data.Field, numCols)
			expectedRows := -1
			for colIdx := 0; colIdx < numCols; colIdx++ {
				field, rows, err := decodeMsgpackColumn(dec, columns[colIdx], types[colIdx], maxElems)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", columns[colIdx], err)
				}
				if expectedRows == -1 {
					expectedRows = rows
				} else if rows != expectedRows {
					return nil, fmt.Errorf("column %q has %d rows, expected %d", columns[colIdx], rows, expectedRows)
				}
				fields[colIdx] = field
			}
			sawData = true
		case "error":
			// Defensive: error envelopes normally arrive with a non-200 status
			// and are handled in doRequest, but surface one here rather than
			// returning an empty frame if the server ever pairs it with a 200.
			// A nil error value (another plausible success-envelope shape) is
			// simply skipped.
			isNil, err := nextIsNil(dec)
			if err != nil {
				return nil, fmt.Errorf("invalid error field: %w", err)
			}
			if isNil {
				continue
			}
			msg, err := dec.DecodeString()
			if err != nil {
				return nil, fmt.Errorf("invalid error field: %w", err)
			}
			return nil, fmt.Errorf("arc error: %s", truncateForLog(msg))
		default:
			// success / row_count / execution_time_ms / timestamp / profile /
			// future additions — not needed to build the frame.
			if err := dec.Skip(); err != nil {
				return nil, fmt.Errorf("invalid envelope value for %q: %w", key, err)
			}
		}
	}

	if !sawData {
		// SHOW-style empty results ship columns=[] types=[] data=[]; a missing
		// data key entirely means the envelope wasn't a query response.
		return nil, fmt.Errorf("malformed envelope: no data field")
	}

	return data.NewFrame("", fields...), nil
}

// checkedLen decodes an array-length header and validates it before any
// caller allocates. DecodeArrayLen returns -1 for msgpack nil, and a hostile
// length header can claim billions of elements in 5 wire bytes: both must be
// rejected here, not fed to make().
func checkedLen(dec *msgpack.Decoder, maxElems int64) (int, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("malformed envelope: expected array, got nil")
	}
	if int64(n) > maxElems {
		return 0, fmt.Errorf("array length %d exceeds response element limit %d", n, maxElems)
	}
	return n, nil
}

// decodeStringArray decodes a msgpack array of strings.
func decodeStringArray(dec *msgpack.Decoder, maxElems int64) ([]string, error) {
	n, err := checkedLen(dec, maxElems)
	if err != nil {
		return nil, err
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		if out[i], err = dec.DecodeString(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// nextIsNil reports whether the decoder's next value is msgpack nil, consuming
// it when it is.
func nextIsNil(dec *msgpack.Decoder) (bool, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return false, err
	}
	if code != msgpcode.Nil {
		return false, nil
	}
	return true, dec.DecodeNil()
}

// decodeMsgpackColumn decodes one column-major value array into a nullable
// Grafana field. `typeStr` comes from Arc's frozen wire-type vocabulary
// (internal/api/wiretypes.go): int8..int64, uint8..uint64, float32/float64,
// bool, timestamp[unit], date32, utf8, large_utf8, binary, string_encoded,
// list, struct, map, and unknown:<detail>.
//
// Field shapes mirror the Arrow decode path (arrow.go) so switching protocol
// never changes a dashboard's field types:
//   - int64/uint64 promote to *float64 (Grafana numeric compatibility)
//   - every field is nullable — nulls arrive as in-place msgpack nil
//   - anything non-scalar or unknown lands as *string
func decodeMsgpackColumn(dec *msgpack.Decoder, name, typeStr string, maxElems int64) (*data.Field, int, error) {
	rows, err := checkedLen(dec, maxElems)
	if err != nil {
		return nil, 0, err
	}

	switch {
	case typeStr == "int64":
		vals, err := decodeColumnValues(dec, rows, func(dec *msgpack.Decoder) (float64, error) {
			v, err := dec.DecodeInt64()
			return float64(v), err
		})
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "uint64":
		vals, err := decodeColumnValues(dec, rows, func(dec *msgpack.Decoder) (float64, error) {
			v, err := dec.DecodeUint64()
			return float64(v), err
		})
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "int32":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeInt32)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "int16":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeInt16)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "int8":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeInt8)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "uint32":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeUint32)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "uint16":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeUint16)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "uint8":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeUint8)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "float64":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeFloat64)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "float32":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeFloat32)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "bool":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeBool)
		return data.NewField(name, nil, vals), rows, err
	case strings.HasPrefix(typeStr, "timestamp"), typeStr == "date32":
		// The wire value is a msgpack timestamp extension carrying a real UTC
		// instant — the unit in the type name describes the source Arrow
		// column, not the encoding, so no scaling is needed here.
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeTime)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "utf8", typeStr == "large_utf8":
		vals, err := decodeColumnValues(dec, rows, (*msgpack.Decoder).DecodeString)
		return data.NewField(name, nil, vals), rows, err
	case typeStr == "binary":
		// Base64, matching what the Arrow path renders for BINARY columns
		// (array.Binary.ValueStr): protocol flips must not change displayed
		// values, and raw bytes are not guaranteed valid UTF-8.
		vals, err := decodeColumnValues(dec, rows, func(dec *msgpack.Decoder) (string, error) {
			b, err := dec.DecodeBytes()
			if err != nil {
				return "", err
			}
			return base64.StdEncoding.EncodeToString(b), nil
		})
		return data.NewField(name, nil, vals), rows, err
	default:
		// string_encoded, list, struct, map, unknown:<detail> — the server
		// stringifies all of these before encoding, but decode generically and
		// re-stringify so a future non-string representation still renders
		// instead of failing the query (mirrors arrow.go's string fallback).
		vals, err := decodeColumnValues(dec, rows, func(dec *msgpack.Decoder) (string, error) {
			v, err := dec.DecodeInterface()
			if err != nil {
				return "", err
			}
			if s, ok := v.(string); ok {
				return s, nil
			}
			return fmt.Sprintf("%v", v), nil
		})
		return data.NewField(name, nil, vals), rows, err
	}
}

// decodeColumnValues decodes `rows` values into a nullable pointer slice,
// treating in-place msgpack nil as a typed nil pointer.
func decodeColumnValues[T any](dec *msgpack.Decoder, rows int, decodeOne func(*msgpack.Decoder) (T, error)) ([]*T, error) {
	vals := make([]*T, rows)
	for i := 0; i < rows; i++ {
		isNil, err := nextIsNil(dec)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		if isNil {
			continue
		}
		v, err := decodeOne(dec)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		vals[i] = &v
	}
	return vals, nil
}
