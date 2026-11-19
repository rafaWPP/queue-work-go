package main

import (
 "context"
 "errors"
 "path/filepath"
 "sync"
 "testing"
 "time"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

func TestStandalonePersistenceAndOwnership(t *testing.T) {
 ctx:=context.Background()
 path:=filepath.Join(t.TempDir(),"queue.db")
 s,err:=sqlite.Open(ctx,"sqlite3",path)
 if err!=nil{t.Fatal(err)}
 id,err:=s.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"jobs/example",Payload:[]byte("hello")})
 if err!=nil{t.Fatal(err)}
 if err:=s.Close();err!=nil{t.Fatal(err)}
 s,err=sqlite.Open(ctx,"sqlite3",path)
 if err!=nil{t.Fatal(err)}
 defer s.Close()
 lease,found,err:=s.Claim(ctx,"default",time.Minute)
 if err!=nil||!found||lease.Task.ID!=id||string(lease.Task.Payload)!="hello"{
  t.Fatalf("persisted task not recovered: lease=%+v found=%v err=%v",lease,found,err)
 }
 if err:=s.Complete(ctx,id,"invalid");!errors.Is(err,queueengine.ErrNotOwner){t.Fatalf("stale token accepted: %v",err)}
 if err:=s.Renew(ctx,id,lease.Token,time.Minute);err!=nil{t.Fatal(err)}
 if err:=s.Complete(ctx,id,lease.Token);err!=nil{t.Fatal(err)}
 _,found,err=s.Claim(ctx,"default",time.Minute)
 if err!=nil||found{t.Fatalf("completed task reclaimed: found=%v err=%v",found,err)}
}

func TestStandaloneExpiredLeaseFencing(t *testing.T) {
 ctx:=context.Background()
 s,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"queue.db"))
 if err!=nil{t.Fatal(err)}
 defer s.Close()
 id,err:=s.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"job"})
 if err!=nil{t.Fatal(err)}
 first,found,err:=s.Claim(ctx,"default",20*time.Millisecond)
 if err!=nil||!found{t.Fatalf("first claim: %v %v",found,err)}
 time.Sleep(45*time.Millisecond)
 second,found,err:=s.Claim(ctx,"default",time.Minute)
 if err!=nil||!found||second.Token==first.Token||second.Task.ID!=id{
  t.Fatalf("expired claim not recovered: %+v %v %v",second,found,err)
 }
 if err:=s.Complete(ctx,id,first.Token);!errors.Is(err,queueengine.ErrNotOwner){t.Fatalf("old worker completed: %v",err)}
 if err:=s.Complete(ctx,id,second.Token);err!=nil{t.Fatal(err)}
}

func TestStandaloneConcurrentClaims(t *testing.T) {
 ctx:=context.Background()
 s,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"queue.db"))
 if err!=nil{t.Fatal(err)}
 defer s.Close()
 _,err=s.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"job"})
 if err!=nil{t.Fatal(err)}
 var wg sync.WaitGroup
 results:=make(chan bool,20)
 failures:=make(chan error,20)
 for i:=0;i<20;i++ {
  wg.Add(1)
  go func(){
   defer wg.Done()
   _,found,err:=s.Claim(ctx,"default",time.Minute)
   if err!=nil{failures<-err;return}
   results<-found
  }()
 }
 wg.Wait()
 close(results);close(failures)
 for err:=range failures {t.Fatal(err)}
 count:=0
 for found:=range results {if found{count++}}
 if count!=1{t.Fatalf("expected one claim, got %d",count)}
}
