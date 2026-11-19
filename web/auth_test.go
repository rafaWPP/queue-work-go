package web

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSessionValidAndTamperResistant(t *testing.T) {
	now := time.Now()
	token, err := newSession("strong-test-secret", now)
	if err != nil {
		t.Fatal(err)
	}
	if !validSession("strong-test-secret", token, now) {
		t.Fatal("valid session rejected")
	}
	if validSession("wrong-secret", token, now) {
		t.Fatal("session accepted with wrong secret")
	}
	if validSession("strong-test-secret", token, now.Add(13*time.Hour)) {
		t.Fatal("expired session accepted")
	}
	if validSession("strong-test-secret", token+"tampered", now) {
		t.Fatal("tampered session accepted")
	}
}

func TestCheckAuthRequiresSessionCookie(t *testing.T) {
	t.Setenv("AUTH", "strong-test-secret")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.URL.RawQuery = "auth=strong-test-secret"
	rec := httptest.NewRecorder()
	if checkAuth(rec, req) {
		t.Fatal("URL tokens must not authenticate")
	}
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
		t.Fatalf("expected redirect to login, got %d", rec.Code)
	}
}

func TestCheckAuthAcceptsSignedCookie(t *testing.T) {
	t.Setenv("AUTH", "strong-test-secret")
	token, err := newSession("strong-test-secret", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	if !checkAuth(rec, req) {
		t.Fatal("signed session rejected")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing no-store header")
	}
}

func TestCheckAuthRejectsEmptySecret(t *testing.T) {
	t.Setenv("AUTH", "")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "anything"})
	if checkAuth(httptest.NewRecorder(), req) {
		t.Fatal("empty AUTH must never authenticate")
	}
}

func TestLoginSetsHttpOnlyCookie(t *testing.T) {
	t.Setenv("AUTH", "strong-test-secret")
	form := url.Values{"password": {"strong-test-secret"}}
	req := httptest.NewRequest(http.MethodPost, "https://example.test/login", strings.NewReader(form.Encode()))
	req.Header.Set("Origin", "https://example.test")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	loginHandler(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("login status %d", rec.Code)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || !cookies[0].Secure || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("login cookie missing security attributes")
	}
}

func TestRequirePostAndSameOrigin(t *testing.T) {
	for _, tc := range []struct {
		method string
		origin string
		want   bool
	}{
		{http.MethodGet, "https://example.test", false},
		{http.MethodPost, "https://evil.test", false},
		{http.MethodPost, "", false},
		{http.MethodPost, "https://example.test", true},
	} {
		req := httptest.NewRequest(tc.method, "https://example.test/clear/active", nil)
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		if got := requirePost(httptest.NewRecorder(), req); got != tc.want {
			t.Errorf("requirePost(%s, %s) = %v, want %v", tc.method, tc.origin, got, tc.want)
		}
	}
}
