package main

import "testing"

func TestParseSize(t *testing.T) {
	ok := map[string]int64{
		"":        0,
		"0":       0,
		"512":     512,
		"10KB":    10_000,
		"500mb":   500_000_000,
		"10GB":    10_000_000_000,
		"2TB":     2_000_000_000_000,
		" 5 GB ":  5_000_000_000,
		"1000000": 1_000_000,
	}
	for in, want := range ok {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d, nil", in, got, err, want)
		}
	}
	for _, in := range []string{"-1", "abc", "10XB", "1.5GB", "GB", "9223373TB", "99999999999999999999"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = nil error, want failure", in)
		}
	}
}

func TestFormatSize(t *testing.T) {
	cases := map[int64]string{
		512:            "512B",
		10_000:         "10.0KB",
		1_500_000:      "1.5MB",
		10_000_000_000: "10.0GB",
	}
	for in, want := range cases {
		if got := formatSize(in); got != want {
			t.Errorf("formatSize(%d) = %q, want %q", in, got, want)
		}
	}
}
