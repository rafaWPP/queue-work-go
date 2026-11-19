package client

import (
 "context"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
)

func TestEnqueue(t *testing.T){
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  if r.Method!=http.MethodPost||r.URL.Path!="/v1/tasks"{t.Errorf("wrong request: %s %s",r.Method,r.URL.Path)}
  if r.Header.Get("Authorization")!="Bearer secret"{t.Error("missing bearer token")}
  if !strings.Contains(r.Header.Get("Content-Type"),"application/json"){t.Error("missing JSON content type")}
  w.WriteHeader(http.StatusCreated)
  _,_=w.Write([]byte(`{"id":"job-123","status":"pending"}`))
 }))
 defer server.Close()
 c:=&Client{URL:server.URL,APIKey:"secret"}
 got,err:=c.Enqueue(context.Background(),EnqueueRequest{Name:"jobs/email",Payload:"hello"})
 if err!=nil{t.Fatal(err)}
 if got.ID!="job-123"||got.Status!="pending"{t.Fatalf("unexpected response: %+v",got)}
}

func TestEnqueueRejectsBadResponse(t *testing.T){
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  http.Error(w,"unauthorized",http.StatusUnauthorized)
 }))
 defer server.Close()
 c:=&Client{URL:server.URL,APIKey:"secret"}
 _,err:=c.Enqueue(context.Background(),EnqueueRequest{Name:"jobs/email"})
 if err==nil||!strings.Contains(err.Error(),"401"){t.Fatalf("expected HTTP 401, got %v",err)}
}
