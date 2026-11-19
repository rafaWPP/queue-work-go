package main

import (
 "context"
 "errors"
 "path/filepath"
 "sync/atomic"
 "testing"
 "time"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

func TestWorkerProcessesQueuedTasks(t *testing.T){
 ctx,cancel:=context.WithCancel(context.Background())
 defer cancel()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"worker.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 var calls atomic.Int32
 done:=make(chan struct{},1)
 w:=&worker{store:store,queue:"default",lease:time.Second,poll:10*time.Millisecond,handlers:map[string]Handler{
  "test/job":func(ctx context.Context,p []byte)error{
   if string(p)!="hello"{return errors.New("wrong payload")}
   calls.Add(1)
   done<-struct{}{}
   return nil
  },
 }}
 _,err=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"test/job",Payload:[]byte("hello")})
 if err!=nil{t.Fatal(err)}
 finished:=make(chan error,1)
 go func(){finished<-w.run(ctx)}()
 select{case <-done:case <-time.After(3*time.Second):t.Fatal("worker did not process task")}
 // Wait until the completion is committed before inspecting queue state.
 deadline:=time.Now().Add(3*time.Second)
 for {
  _,found,err:=store.Claim(ctx,"default",time.Second)
  if err!=nil{t.Fatal(err)}
  if !found{break}
  if time.Now().After(deadline){t.Fatal("completed job remains claimable")}
  time.Sleep(10*time.Millisecond)
 }
 cancel()
 select{case err:=<-finished:if err!=nil{t.Fatal(err)};case <-time.After(3*time.Second):t.Fatal("worker did not stop")}
 if calls.Load()!=1{t.Fatalf("handler called %d times",calls.Load())}
}

func TestWorkerRejectsUnknownHandler(t *testing.T){
 ctx:=context.Background()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"unknown.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 id,err:=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"missing"})
 if err!=nil{t.Fatal(err)}
 lease,found,err:=store.Claim(ctx,"default",time.Second)
 if err!=nil||!found{t.Fatalf("claim: %v %v",found,err)}
 w:=&worker{store:store,handlers:map[string]Handler{}}
 w.execute(ctx,lease)
 _,found,err=store.Claim(ctx,"default",time.Second)
 if err!=nil||found{t.Fatalf("unknown job was not marked failed: id=%s found=%v err=%v",id,found,err)}
}

func TestWorkerHandlerFailure(t *testing.T){
 ctx:=context.Background()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"failed.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 _,err=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"fails"})
 if err!=nil{t.Fatal(err)}
 lease,found,err:=store.Claim(ctx,"default",time.Second)
 if err!=nil||!found{t.Fatalf("claim: %v %v",found,err)}
 w:=&worker{store:store,lease:time.Second,handlers:map[string]Handler{"fails":func(context.Context,[]byte)error{return errors.New("expected")}}}
 w.execute(ctx,lease)
 _,found,err=store.Claim(ctx,"default",time.Second)
 if err!=nil||found{t.Fatalf("failed job reappeared: %v %v",found,err)}
}
