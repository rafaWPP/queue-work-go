package main

import (
	"regexp"
	"testing"
)

func TestNewProcessingToken(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		token, err := newProcessingToken()
		if err != nil {
			t.Fatal(err)
		}
		if !pattern.MatchString(token) {
			t.Fatalf("invalid processing token: %q", token)
		}
		if seen[token] {
			t.Fatalf("duplicate processing token: %q", token)
		}
		seen[token] = true
	}
}
