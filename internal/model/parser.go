package model

import (
	"errors"
	"strconv"
	"strings"
)

var (
	ErrEmptyLine        = errors.New("empty log line")
	ErrMissingIP        = errors.New("missing client IP")
	ErrInvalidTimestamp = errors.New("invalid bracketed timestamp")
	ErrInvalidRequest   = errors.New("invalid quoted HTTP request")
	ErrInvalidStatus    = errors.New("invalid HTTP status code")
	ErrInvalidBytes     = errors.New("invalid bytes_sent field")
	ErrInvalidLatency   = errors.New("invalid latency_ms field")
)

// ParseLine parses a single HTTP access log line without regular expressions.
func ParseLine(line string) (LogEvent, error) {
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return LogEvent{}, ErrEmptyLine
	}

	sp := strings.IndexByte(line, ' ')
	if sp <= 0 {
		return LogEvent{}, ErrMissingIP
	}
	ip := line[:sp]
	rest := line[sp+1:]

	openBr := strings.IndexByte(rest, '[')
	if openBr < 0 {
		return LogEvent{}, ErrInvalidTimestamp
	}
	rest = rest[openBr+1:]
	closeBr := strings.IndexByte(rest, ']')
	if closeBr <= 0 {
		return LogEvent{}, ErrInvalidTimestamp
	}
	ts := rest[:closeBr]
	rest = rest[closeBr+1:]

	qOpen := strings.IndexByte(rest, '"')
	if qOpen < 0 {
		return LogEvent{}, ErrInvalidRequest
	}
	rest = rest[qOpen+1:]
	qClose := strings.IndexByte(rest, '"')
	if qClose <= 0 {
		return LogEvent{}, ErrInvalidRequest
	}
	reqStr := rest[:qClose]
	rest = strings.TrimSpace(rest[qClose+1:])

	reqSp1 := strings.IndexByte(reqStr, ' ')
	if reqSp1 <= 0 {
		return LogEvent{}, ErrInvalidRequest
	}
	method := reqStr[:reqSp1]
	afterMethod := reqStr[reqSp1+1:]
	reqSp2 := strings.IndexByte(afterMethod, ' ')
	if reqSp2 <= 0 {
		return LogEvent{}, ErrInvalidRequest
	}
	path := afterMethod[:reqSp2]

	sp1 := strings.IndexByte(rest, ' ')
	if sp1 <= 0 {
		return LogEvent{}, ErrInvalidStatus
	}
	statusStr := rest[:sp1]
	rest = strings.TrimSpace(rest[sp1+1:])

	sp2 := strings.IndexByte(rest, ' ')
	if sp2 <= 0 {
		return LogEvent{}, ErrInvalidBytes
	}
	bytesStr := rest[:sp2]
	latencyStr := strings.TrimSpace(rest[sp2+1:])

	status, err := strconv.Atoi(statusStr)
	if err != nil || status < 100 || status > 599 {
		return LogEvent{}, ErrInvalidStatus
	}

	bytesSent, err := strconv.ParseUint(bytesStr, 10, 64)
	if err != nil {
		return LogEvent{}, ErrInvalidBytes
	}

	latency, err := strconv.ParseFloat(latencyStr, 64)
	if err != nil || latency < 0 {
		return LogEvent{}, ErrInvalidLatency
	}

	return LogEvent{
		IP:         ip,
		Timestamp:  ts,
		Method:     method,
		Path:       path,
		StatusCode: status,
		BytesSent:  bytesSent,
		LatencyMs:  latency,
	}, nil
}
