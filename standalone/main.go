// Queue Work Go standalone CLI. Experimental: local SQLite queue storage.
package main

import (
 "context"
 "encoding/json"
 "errors"
 "flag"
 "fmt"
 "log"
 "os"
 "os/signal"
 "syscall"
 "time"

 _ "github.com/mattn/go-sqlite3"
 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

func run(ctx context.Context, args []string) error {
 if len(args)==0 {return errors.New("usage: queue-standalone [serve|work|enqueue|claim|renew|complete|fail] [flags]")}
 command:=args[0]
 flags:=flag.NewFlagSet(command,flag.ContinueOnError)
 dbPath:=flags.String("db","queue.db","local SQLite file")
 queue:=flags.String("queue","default","queue name")
 name:=flags.String("name","","task name")
 payload:=flags.String("payload","","task payload")
 id:=flags.String("id","","task ID")
 token:=flags.String("token","","ownership token")
 reason:=flags.String("reason","","failure reason")
 lease:=flags.Duration("lease",30*time.Second,"lease duration")
 addr:=flags.String("listen","127.0.0.1:8088","loopback HTTP address")
 apiKey:=flags.String("api-key","","API key (prefer QUEUE_API_KEY environment variable)")
 poll:=flags.Duration("poll",500*time.Millisecond,"worker polling interval")
 maxAttempts:=flags.Int("max-attempts",3,"maximum total claims before permanent failure")
 retryDelay:=flags.Duration("retry-delay",time.Second,"initial retry backoff")
 if err:=flags.Parse(args[1:]);err!=nil{return err}
 switch command {case "serve","work","enqueue","claim","renew","complete","fail": default:return fmt.Errorf("unknown command %q",command)}
 if command=="serve" {
  key:=os.Getenv("QUEUE_API_KEY")
  if key=="" {key=*apiKey}
  return serve(ctx,*dbPath,*addr,key)
 }
 store,err:=sqlite.Open(ctx,"sqlite3",*dbPath)
 if err!=nil{return err}
 defer store.Close()
 if command=="work" {
  handlers,err:=defaultHandlers()
  if err!=nil{return err}
  return (&worker{store:store,queue:*queue,handlers:handlers,lease:*lease,poll:*poll,maxAttempts:*maxAttempts,retryDelay:*retryDelay,logger:log.Default()}).run(ctx)
 }
 switch command {
 case "enqueue":
  if *name=="" {return errors.New("-name is required")}
  taskID,err:=store.Enqueue(ctx,queueengine.Task{Queue:*queue,Name:*name,Payload:[]byte(*payload)})
  if err!=nil{return err}
  return json.NewEncoder(os.Stdout).Encode(map[string]string{"id":taskID,"status":"pending"})
 case "claim":
  l,found,err:=store.Claim(ctx,*queue,*lease)
  if err!=nil{return err}
  if !found{return json.NewEncoder(os.Stdout).Encode(map[string]bool{"found":false})}
  return json.NewEncoder(os.Stdout).Encode(l)
 case "renew":
  return store.Renew(ctx,*id,*token,*lease)
 case "complete":
  return store.Complete(ctx,*id,*token)
 case "fail":
  return store.Fail(ctx,*id,*token,*reason)
 }
 return nil
}

func main(){
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM)
 defer stop()
 if err:=run(ctx,os.Args[1:]);err!=nil{
  fmt.Fprintln(os.Stderr,"queue-standalone:",err)
  os.Exit(1)
 }
}
