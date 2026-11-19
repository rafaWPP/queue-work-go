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

func TestStandaloneAPIAuthenticationAndTaskLifecycle(t *testing.T) {
 store,err:=sqlite.Open(context.Background(),"sqlite3",filepath.Join(t.TempDir(),"api.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 handler:=&apiServer{store:store,key:strings.Repeat("x",32)}
 post:=func(path,body,auth string)*httptest.ResponseRecorder{
  req:=httptest.NewRequest(http.MethodPost,path,bytes.NewBufferString(body))
  if auth!=""{req.Header.Set("Authorization","Bearer "+auth)}
  response:=httptest.NewRecorder()
  handler.ServeHTTP(response,req)
  return response
 }
 if r:=post("/v1/tasks",`{"name":"job"}`,"");r.Code!=401{t.Fatalf("unauthenticated request accepted: %d",r.Code)}
 if r:=post("/v1/tasks",`{"name":"job","extra":true}`,handler.key);r.Code!=400{t.Fatalf("unknown field accepted: %d",r.Code)}
 if r:=post("/v1/tasks",`{"name":"job"}{"name":"another"}`,handler.key);r.Code!=400{t.Fatalf("multiple objects accepted: %d",r.Code)}
 r:=post("/v1/tasks",`{"queue":"default","name":"job","payload":"hello"}`,handler.key)
 if r.Code!=201{t.Fatalf("enqueue status %d: %s",r.Code,r.Body.String())}
 r=post("/v1/claim",`{"queue":"default"}`,handler.key)
 if r.Code!=200{t.Fatalf("claim status %d: %s",r.Code,r.Body.String())}
 var response struct{
  Found bool `json:"found"`
  Lease struct{Token string `json:"Token"`;Task struct{ID string `json:"ID"`}`json:"Task"`}`json:"lease"`
 }
 if err:=json.Unmarshal(r.Body.Bytes(),&response);err!=nil{t.Fatal(err)}
 if !response.Found||response.Lease.Token==""||response.Lease.Task.ID==""{t.Fatalf("bad claim: %s",r.Body.String())}
 data,_:=json.Marshal(map[string]string{"id":response.Lease.Task.ID,"token":response.Lease.Token})
 if r:=post("/v1/complete",string(data),handler.key);r.Code!=200{t.Fatalf("complete status %d: %s",r.Code,r.Body.String())}
 if r:=post("/v1/complete",string(data),handler.key);r.Code!=409{t.Fatalf("duplicate completion status %d",r.Code)}
}

func TestServeRequiresKeyAndLoopback(t *testing.T) {
 ctx:=context.Background()
 if err:=serve(ctx,"ignored.db","127.0.0.1:0","short");err==nil{t.Fatal("accepted short API key")}
 if err:=serve(ctx,"ignored.db","0.0.0.0:8080",strings.Repeat("x",32));err==nil{t.Fatal("accepted public listener")}
}
