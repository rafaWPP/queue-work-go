package main

import "testing"

func TestValidatedPluginPath(t *testing.T) {
	cases := []struct {
		name string
		want string
		valid bool
	}{
		{"jobs/sendmessageevo", "jobs/sendmessageevo.so", true},
		{"jobs/mk/sendmessageevo", "jobs/mk/sendmessageevo.so", true},
		{"", "", false},
		{"../secret", "", false},
		{"jobs/../../secret", "", false},
		{"jobs/../secret", "", false},
		{"jobs//plugin", "", false},
		{"jobs/plugin.so", "", false},
		{"jobs/plugin name", "", false},
		{"jobs/\\\\plugin", "", false},
		{"/tmp/plugin", "", false},
		{"secret", "", false},
	}
	for _, tc := range cases {
		got, err := validatedPluginPath(tc.name)
		if tc.valid && (err != nil || got != tc.want) {
			t.Errorf("%q: got %q, %v; want %q", tc.name, got, err, tc.want)
		}
		if !tc.valid && err == nil {
			t.Errorf("%q: expected rejection, got %q", tc.name, got)
		}
	}
}
