package web

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const sessionCookieName = "queue_session"
const sessionLifetime = 12 * time.Hour


const loginWindow = 15 * time.Minute
const loginLimit = 5

type loginFailure struct {
	count   int
	expires time.Time
}

var loginGuard = struct {
	sync.Mutex
	failures map[string]loginFailure
}{failures: make(map[string]loginFailure)}

func loginPeer(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func loginBlocked(peer string, now time.Time) bool {
	loginGuard.Lock()
	defer loginGuard.Unlock()
	entry := loginGuard.failures[peer]
	return now.Before(entry.expires) && entry.count >= loginLimit
}

func loginFailed(peer string, now time.Time) {
	loginGuard.Lock()
	defer loginGuard.Unlock()
	if len(loginGuard.failures) >= 10000 {
		for key, entry := range loginGuard.failures {
			if !now.Before(entry.expires) {
				delete(loginGuard.failures, key)
			}
		}
		if len(loginGuard.failures) >= 10000 {
			return
		}
	}
	entry := loginGuard.failures[peer]
	if !now.Before(entry.expires) {
		entry = loginFailure{expires: now.Add(loginWindow)}
	}
	entry.count++
	loginGuard.failures[peer] = entry
}

func loginSucceeded(peer string) {
	loginGuard.Lock()
	delete(loginGuard.failures, peer)
	loginGuard.Unlock()
}

// Sessions are signed using AUTH. Changing AUTH invalidates all sessions.
func sessionSignature(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("queue-work-go/session/v1:"))
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func newSession(secret string, now time.Time) (string, error) {
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	payload := strconv.FormatInt(now.Add(sessionLifetime).Unix(), 10) + "." + base64.RawURLEncoding.EncodeToString(nonce)
	return payload + "." + sessionSignature(secret, payload), nil
}

func validSession(secret, token string, now time.Time) bool {
	if secret == "" || len(token) > 512 {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	expiry, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || expiry <= now.Unix() || expiry > now.Add(sessionLifetime).Unix()+60 {
		return false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(nonce) != 24 {
		return false
	}
	payload := parts[0] + "." + parts[1]
	expected := sessionSignature(secret, payload)
	return subtle.ConstantTimeCompare([]byte(parts[2]), []byte(expected)) == 1
}

func secureRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func checkAuth(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	secret := os.Getenv("AUTH")
	cookie, err := r.Cookie(sessionCookieName)
	if secret != "" && err == nil && validSession(secret, cookie.Value, time.Now()) {
		return true
	}
	if r.URL.Path == "/login" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	} else {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	}
	return false
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Queue Work Go — Login</title></head><body style="font-family:system-ui;background:#111827;color:#f9fafb;display:grid;place-items:center;min-height:90vh"><form method="post" action="/login" style="display:grid;gap:16px;padding:32px;background:#1f2937;border-radius:12px;width:min(320px,90vw)"><h1 style="font-size:24px">Queue Work Go</h1><label for="password">Chave de acesso</label><input id="password" name="password" type="password" required autocomplete="current-password" style="padding:12px"><button type="submit" style="padding:12px;cursor:pointer">Entrar</button></form></body></html>`))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if !sameOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	peer := loginPeer(r)
	if loginBlocked(peer, time.Now()) {
		w.Header().Set("Retry-After", "900")
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form", http.StatusBadRequest)
		return
	}
	secret := os.Getenv("AUTH")
	provided := r.PostForm.Get("password")
	if secret == "" || subtle.ConstantTimeCompare([]byte(secret), []byte(provided)) != 1 {
		loginFailed(peer, time.Now())
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	loginSucceeded(peer)
	token, err := newSession(secret, time.Now())
	if err != nil {
		http.Error(w, "Session unavailable", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: token, Path: "/", MaxAge: int(sessionLifetime.Seconds()), HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteStrictMode})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !sameOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Path: "/", MaxAge: -1, HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteStrictMode})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// sameOrigin rejects cross-site state-changing requests. Proxies must preserve Host.
// The Origin header is required on POST; this avoids relying on SameSite alone.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.Host != r.Host {
		return false
	}
	if secureRequest(r) {
		return u.Scheme == "https"
	}
	return u.Scheme == "http"
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return false
	}
	if !sameOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return false
	}
	return true
}
