package model

import "testing"

func TestParseLine_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantIP  string
		wantSt  int
		wantErr error
	}{
		{
			name:   "valid POST checkout line",
			input:  `10.0.1.5 - - [25/Oct/2026:14:22:01 +0000] "POST /api/v1/checkout HTTP/1.1" 201 2048 18.45`,
			wantIP: "10.0.1.5",
			wantSt: 201,
		},
		{
			name:    "empty line",
			input:   "",
			wantErr: ErrEmptyLine,
		},
		{
			name:    "unclosed timestamp bracket",
			input:   `10.0.1.5 - - [25/Oct/2026:14:22:01 +0000 "GET / HTTP/1.1" 200 64 1.0`,
			wantErr: ErrInvalidTimestamp,
		},
		{
			name:    "status out of range",
			input:   `10.0.1.5 - - [25/Oct/2026:14:22:01 +0000] "GET / HTTP/1.1" 700 64 1.0`,
			wantErr: ErrInvalidStatus,
		},
		{
			name:    "negative latency rejected",
			input:   `10.0.1.5 - - [25/Oct/2026:14:22:01 +0000] "GET / HTTP/1.1" 200 64 -4.5`,
			wantErr: ErrInvalidLatency,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLine(tc.input)
			if tc.wantErr != nil {
				if err != tc.wantErr {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.IP != tc.wantIP || got.StatusCode != tc.wantSt {
				t.Errorf("got (%s, %d), want (%s, %d)", got.IP, got.StatusCode, tc.wantIP, tc.wantSt)
			}
		})
	}
}
