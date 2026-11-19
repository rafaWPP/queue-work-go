package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckAuthRejectsMissingSecret(t *testing.T) {
	t.Setenv("AUTH", "")
	req := httptest.NewRequest(http.MethodGet, "/?auth=", nil)
	rec := httptest.NewRecorder()
	if checkAuth(rec, req) {
		t.Fatal("empty AUTH must never authorize access")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestCheckAuthRejectsIncorrectSecret(t *testing.T) {
	t.Setenv("AUTH", "expected-secret")
	req := httptest.NewRequest(http.MethodGet, "/?auth=wrong-secret", nil)
	rec := httptest.NewRecorder()
	if checkAuth(rec, req) {
		t.Fatal("incorrect secret must not authorize access")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestCheckAuthAcceptsConfiguredSecret(t *testing.T) {
	t.Setenv("AUTH", "expected-secret")
	req := httptest.NewRequest(http.MethodGet, "/?auth=expected-secret", nil)
	rec := httptest.NewRecorder()
	if !checkAuth(rec, req) {
		t.Fatal("configured secret should authorize access")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("authenticated responses must not be cached")
	}
	if rec.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatal("authenticated responses must not leak URL in referrers")
	}
}
