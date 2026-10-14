package main

import "testing"

func TestConfiguredAuth(t *testing.T) {
	for _, tt := range []struct {
		name, key, secret   string
		dev, require, valid bool
	}{
		{"missing credentials", "", "", false, false, false},
		{"missing secret", "test", "", false, false, false},
		{"missing key", "", "secret", false, false, false},
		{"authenticated", "test", "secret", false, false, true},
		{"signed payload", "test", "secret", false, true, true},
		{"development", "", "", true, false, true},
		{"development with credentials", "test", "secret", true, false, false},
		{"development with partial credentials", "test", "", true, false, false},
		{"development with signed payload", "", "", true, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ESSTHREE_ACCESS_KEY_ID", tt.key)
			t.Setenv("ESSTHREE_SECRET_ACCESS_KEY", tt.secret)
			auth, err := configuredAuth(tt.dev, "us-east-1", tt.require)
			if (err == nil) != tt.valid || tt.valid && (auth == nil) != tt.dev {
				t.Fatalf("middleware=%v err=%v", auth != nil, err)
			}
		})
	}
}
