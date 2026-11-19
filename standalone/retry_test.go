package main

import (
 "context"
 "errors"
 "path/filepath"
 "testing"
 "time"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

func TestWorkerRetriesThenSucceeds(t *testing.T){
 ctx:=context.Background()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"retry.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 _,err=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"flaky"})
 if err!=nil{t.Fatal(err)}
 calls:=0
 w:=&worker{store:store,lease:time.Second,maxAttempts:3,retryDelay:0,handlers:map[string]Handler{
  "flaky":func(context.Context,[]byte)error{calls++;if calls==1{return errors.New("temporary")};return nil},
 }}
 first,found,err:=store.Claim(ctx,"default",time.Second)
 if err!=nil||!found{t.Fatalf("initial claim: %v %v",found,err)}
 w.execute(ctx,first)
 second,found,err:=store.Claim(ctx,"default",time.Second)
 if err!=nil||!found||second.Task.Attempts!=2{t.Fatalf("retry claim: %+v %v %v",second,found,err)}
 if err:=store.Complete(ctx,first.Task.ID,first.Token);!errors.Is(err,queueengine.ErrNotOwner){t.Fatalf("stale token succeeded: %v",err)}
 w.execute(ctx,second)
 _,found,err=store.Claim(ctx,"default",time.Second)
 if err!=nil||found||calls!=2{t.Fatalf("retry did not finish: found=%v calls=%d err=%v",found,calls,err)}
}

func TestWorkerRetriesStopAtMaxAttempts(t *testing.T){
 ctx:=context.Background()
 store,err:=sqlite.Open(ctx,"sqlite3",filepath.Join(t.TempDir(),"terminal.db"))
 if err!=nil{t.Fatal(err)}
 defer store.Close()
 _,err=store.Enqueue(ctx,queueengine.Task{Queue:"default",Name:"always-fails"})
 if err!=nil{t.Fatal(err)}
 w:=&worker{store:store,lease:time.Second,maxAttempts:2,retryDelay:0,handlers:map[string]Handler{
  "always-fails":func(context.Context,[]byte)error{return errors.New("persistent failure")},
 }}
 for i:=0;i<2;i++ {
  claim,found,err:=store.Claim(ctx,"default",time.Second)
  if err!=nil||!found{t.Fatalf("attempt %d: %v %v",i+1,found,err)}
  w.execute(ctx,claim)
 }
 _,found,err:=store.Claim(ctx,"default",time.Second)
 if err!=nil||found{t.Fatalf("exhausted task reclaimed: %v %v",found,err)}
}
