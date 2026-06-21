package server

import (
	"testing"
)

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"bearer with space", "Bearer mytoken123", "mytoken123"},
		{"bearer lowercase", "bearer mytoken123", "mytoken123"},
		{"bearer uppercase", "BEARER mytoken123", "mytoken123"},
		{"bearer mixed case", "BeArEr mytoken123", "mytoken123"},
		{"no bearer prefix - just token", "mytoken123", "mytoken123"},
		{"bearer with extra spaces", "Bearer   mytoken123", "  mytoken123"},
		{"basic auth", "Basic dXNlcjpwYXNz", "Basic dXNlcjpwYXNz"},
		{"bearer with empty token", "Bearer ", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBearerToken(tt.input)
			if result != tt.expected {
				t.Errorf("extractBearerToken(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
