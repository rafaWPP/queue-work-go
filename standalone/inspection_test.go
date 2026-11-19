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
 "time"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

func TestInspectionAndManualReplay(t *testing.T){
 ctx:=context.Background()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"inspect.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 id,err:=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"example"})
 if err!=nil{t.Fatal(err)}
 lease,found,err:=store.Claim(ctx,"default",time.Minute)
 if err!=nil||!found{t.Fatalf("claim: %v %v",found,err)}
 if err:=store.Fail(ctx,id,lease.Token,"temporary failure");err!=nil{t.Fatal(err)}
 handler:=&apiServer{store:store,key:strings.Repeat("k",32)}
 post:=func(path string,body any)*httptest.ResponseRecorder{
  raw,_:=json.Marshal(body)
  req:=httptest.NewRequest(http.MethodPost,path,bytes.NewReader(raw))
  req.Header.Set("Authorization","Bearer "+handler.key)
  response:=httptest.NewRecorder()
  handler.ServeHTTP(response,req)
  return response
 }
 r:=post("/v1/tasks/list",map[string]string{"queue":"default","name":"failed"})
 if r.Code!=200||!strings.Contains(r.Body.String(),id){t.Fatalf("list: %d %s",r.Code,r.Body.String())}
 r=post("/v1/tasks/get",map[string]string{"id":id})
 if r.Code!=200||!strings.Contains(r.Body.String(),"temporary failure"){t.Fatalf("get: %d %s",r.Code,r.Body.String())}
 r=post("/v1/tasks/replay",map[string]string{"id":id})
 if r.Code!=200{t.Fatalf("replay: %d %s",r.Code,r.Body.String())}
 r=post("/v1/tasks/replay",map[string]string{"id":id})
 if r.Code!=404{t.Fatalf("duplicate replay: %d",r.Code)}
 _,found,err=store.Claim(ctx,"default",time.Minute)
 if err!=nil||!found{t.Fatalf("replayed task unavailable: %v %v",found,err)}
}
