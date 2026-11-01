package model

import "testing"

const sampleBenchLine = `10.0.1.15 - - [25/Oct/2026:14:22:01 +0000] "POST /api/v1/checkout HTTP/1.1" 200 2048 14.75`

func BenchmarkParseLine_SingleCore(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(sampleBenchLine)))
	for i := 0; i < b.N; i++ {
		ev, err := ParseLine(sampleBenchLine)
		if err != nil || ev.StatusCode != 200 {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseLine_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(sampleBenchLine)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ev, err := ParseLine(sampleBenchLine)
			if err != nil || ev.StatusCode != 200 {
				b.Fatal(err)
			}
		}
	})
}
