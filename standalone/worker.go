package main

import (
 "context"
 "errors"
 "fmt"
 "log"
 "sync"
 "time"

 "go-queue/internal/queueengine"
)

// Handler is an explicitly registered job implementation. Untrusted task
// names or payloads must never be evaluated as shell commands.
type Handler func(context.Context, []byte) error

type worker struct {
 store queueengine.Store
 queue string
 handlers map[string]Handler
 lease time.Duration
 poll time.Duration
 logger *log.Logger
}

func (w *worker) run(ctx context.Context) error {
 if w.store==nil{return errors.New("worker store is required")}
 if w.queue==""{return errors.New("worker queue is required")}
 if w.lease<time.Second{return errors.New("worker lease must be at least one second")}
 if w.poll<=0{return errors.New("worker poll interval must be positive")}
 for {
  if err:=ctx.Err();err!=nil{return nil}
  claim,found,err:=w.store.Claim(ctx,w.queue,w.lease)
  if err!=nil{
   if ctx.Err()!=nil{return nil}
   return fmt.Errorf("claim task: %w",err)
  }
  if !found {
   timer:=time.NewTimer(w.poll)
   select {case <-ctx.Done():if !timer.Stop(){<-timer.C};return nil;case <-timer.C:}
   continue
  }
  w.execute(ctx,claim)
 }
}

func (w *worker) execute(ctx context.Context,claim queueengine.Lease) {
 handler,ok:=w.handlers[claim.Task.Name]
 if !ok {
  if err:=w.store.Fail(ctx,claim.Task.ID,claim.Token,"unknown task handler");err!=nil{w.logf("failed to reject unknown job %s: %v",claim.Task.ID,err)}
  return
 }
 jobCtx,cancel:=context.WithCancel(ctx)
 defer cancel()
 finished:=make(chan error,1)
 go func(){
  defer func(){if p:=recover();p!=nil{finished<-fmt.Errorf("handler panic: %v",p)}}()
  finished<-handler(jobCtx,claim.Task.Payload)
 }()
 ticker:=time.NewTicker(w.lease/3)
 defer ticker.Stop()
 var err error
 for {
  select {
  case err=<-finished:
   if ctx.Err()!=nil{return}
   if err==nil {
    if ackErr:=w.store.Complete(ctx,claim.Task.ID,claim.Token);ackErr!=nil{w.logf("complete task %s: %v",claim.Task.ID,ackErr)}
   }else{
    if failErr:=w.store.Fail(ctx,claim.Task.ID,claim.Token,err.Error());failErr!=nil{w.logf("fail task %s: %v",claim.Task.ID,failErr)}
   }
   return
  case <-ticker.C:
   if renewErr:=w.store.Renew(ctx,claim.Task.ID,claim.Token,w.lease);renewErr!=nil{
    w.logf("lease renewal failed for %s: %v",claim.Task.ID,renewErr)
    cancel()
    // The handler may ignore cancellation. Never acknowledge after losing ownership.
    return
   }
  case <-ctx.Done():
   cancel()
   return
  }
 }
}

func (w *worker) logf(format string,args ...any) {
 if w.logger!=nil{w.logger.Printf(format,args...)}
}

// registry is safe for concurrent registration before worker start.
type registry struct {mu sync.RWMutex; handlers map[string]Handler}
func (r *registry) register(name string,handler Handler) error {
 if name==""||handler==nil{return errors.New("handler name and function required")}
 r.mu.Lock();defer r.mu.Unlock()
 if r.handlers==nil{r.handlers=make(map[string]Handler)}
 if _,exists:=r.handlers[name];exists{return fmt.Errorf("duplicate handler %q",name)}
 r.handlers[name]=handler
 return nil
}
func (r *registry) snapshot()map[string]Handler{
 r.mu.RLock();defer r.mu.RUnlock()
 out:=make(map[string]Handler,len(r.handlers))
 for name,handler:=range r.handlers{out[name]=handler}
 return out
}
