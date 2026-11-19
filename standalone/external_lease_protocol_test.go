package main

import (
 "bytes"
 "context"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "path/filepath"
 "strings"
 "testing"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine/sqlite"
)

// Exercise the external-client lease protocol through HTTP, including
// rejection of a stale or forged ownership token.
func TestStandaloneAPIExternalLeaseRenewalAndOwnership(t *testing.T) {
 ctx := context.Background()
 store, err := sqlite.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "external.db"))
 if err != nil { t.Fatal(err) }
 defer store.Close()
 key := strings.Repeat("k", 32)
 api := &apiServer{store: store, key: key}
 post := func(path, body string) *httptest.ResponseRecorder {
  req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
  req.Header.Set("Authorization", "Bearer "+key)
  rec := httptest.NewRecorder()
  api.ServeHTTP(rec, req)
  return rec
 }
 enqueue := post("/v1/tasks", `{"queue":"external","name":"app/send-email","payload":"customer:42"}`)
 if enqueue.Code != http.StatusCreated { t.Fatalf("enqueue: %d %s", enqueue.Code, enqueue.Body.String()) }
 claim := post("/v1/claim", `{"queue":"external","lease_seconds":30}`)
 if claim.Code != http.StatusOK { t.Fatalf("claim: %d %s", claim.Code, claim.Body.String()) }
 var result struct {
  Found bool `json:"found"`
  Lease struct {
   Token string `json:"Token"`
   Task struct {
    ID string `json:"ID"`
    Name string `json:"Name"`
    Payload []byte `json:"Payload"`
   } `json:"Task"`
  } `json:"lease"`
 }
 if err := json.Unmarshal(claim.Body.Bytes(), &result); err != nil { t.Fatal(err) }
 if !result.Found || result.Lease.Token == "" || result.Lease.Task.ID == "" {
  t.Fatalf("invalid claim: %s", claim.Body.String())
 }
 if result.Lease.Task.Name != "app/send-email" || string(result.Lease.Task.Payload) != "customer:42" {
  t.Fatalf("unexpected task: %+v", result.Lease.Task)
 }
 body := func(token string) string {
  data, err := json.Marshal(map[string]any{"id": result.Lease.Task.ID, "token": token, "lease_seconds": 30})
  if err != nil { t.Fatal(err) }
  return string(data)
 }
 if rec := post("/v1/renew", body("wrong-token")); rec.Code != http.StatusConflict {
  t.Fatalf("foreign renewal accepted: %d %s", rec.Code, rec.Body.String())
 }
 if rec := post("/v1/complete", body("wrong-token")); rec.Code != http.StatusConflict {
  t.Fatalf("foreign completion accepted: %d %s", rec.Code, rec.Body.String())
 }
 if rec := post("/v1/renew", body(result.Lease.Token)); rec.Code != http.StatusOK {
  t.Fatalf("valid renewal failed: %d %s", rec.Code, rec.Body.String())
 }
 if rec := post("/v1/complete", body(result.Lease.Token)); rec.Code != http.StatusOK {
  t.Fatalf("valid completion failed: %d %s", rec.Code, rec.Body.String())
 }
 if rec := post("/v1/claim", `{"queue":"external"}`); rec.Code != http.StatusOK {
  t.Fatalf("final claim: %d %s", rec.Code, rec.Body.String())
 } else {
  var empty struct { Found bool `json:"found"` }
  if err := json.Unmarshal(rec.Body.Bytes(), &empty); err != nil { t.Fatal(err) }
  if empty.Found { t.Fatal("completed task was claimed again") }
 }
}
