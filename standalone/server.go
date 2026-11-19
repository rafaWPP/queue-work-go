package main

import (
 "context"
 "crypto/subtle"
 "encoding/json"
 "errors"
 "fmt"
 "log"
 "net"
 "net/http"
 "strings"
 "time"

 "go-queue/internal/queueengine"
 "go-queue/internal/queueengine/sqlite"
)

type apiServer struct {
 store queueengine.Store
 key string
}

func (a *apiServer) ServeHTTP(w http.ResponseWriter,r *http.Request) {
 w.Header().Set("Cache-Control","no-store")
 if r.URL.Path=="/healthz" {
  if r.Method!=http.MethodGet {http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  w.Header().Set("Content-Type","application/json")
  _,_=w.Write([]byte(`{"status":"ok"}`))
  return
 }
 if r.Method!=http.MethodPost {http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
 supplied:=strings.TrimPrefix(r.Header.Get("Authorization"),"Bearer ")
 if len(supplied)!=len(a.key)||subtle.ConstantTimeCompare([]byte(supplied),[]byte(a.key))!=1 {
  http.Error(w,"unauthorized",http.StatusUnauthorized);return
 }
 r.Body=http.MaxBytesReader(w,r.Body,1<<20)
 defer r.Body.Close()
 var request struct {
  Queue string `json:"queue"`
  Name string `json:"name"`
  Payload string `json:"payload"`
  ID string `json:"id"`
  Token string `json:"token"`
  Reason string `json:"reason"`
  LeaseSeconds int `json:"lease_seconds"`
 }
 decoder:=json.NewDecoder(r.Body)
 decoder.DisallowUnknownFields()
 if err:=decoder.Decode(&request);err!=nil {http.Error(w,"invalid JSON",http.StatusBadRequest);return}
 var extra any
 if err:=decoder.Decode(&extra);!errors.Is(err,context.Canceled) && err==nil {
  http.Error(w,"unexpected JSON after request",http.StatusBadRequest);return
 } else if err!=nil && err.Error()!="EOF" {
  http.Error(w,"invalid trailing JSON",http.StatusBadRequest);return
 }
 if request.Queue==""{request.Queue="default"}
 if request.LeaseSeconds==0{request.LeaseSeconds=30}
 if request.LeaseSeconds<1||request.LeaseSeconds>3600{http.Error(w,"lease_seconds must be between 1 and 3600",http.StatusBadRequest);return}
 lease:=time.Duration(request.LeaseSeconds)*time.Second
 ctx:=r.Context()
 var result any
 var err error
 switch r.URL.Path {
 case "/v1/tasks":
  if request.Name=="" {http.Error(w,"name is required",400);return}
  var id string
  id,err=a.store.Enqueue(ctx,queueengine.Task{Queue:request.Queue,Name:request.Name,Payload:[]byte(request.Payload)})
  result=map[string]string{"id":id,"status":"pending"}
 case "/v1/claim":
  var item queueengine.Lease
  var found bool
  item,found,err=a.store.Claim(ctx,request.Queue,lease)
  result=map[string]any{"found":found,"lease":item}
 case "/v1/renew":
  err=a.store.Renew(ctx,request.ID,request.Token,lease)
  result=map[string]bool{"ok":err==nil}
 case "/v1/complete":
  err=a.store.Complete(ctx,request.ID,request.Token)
  result=map[string]bool{"ok":err==nil}
 case "/v1/fail":
  err=a.store.Fail(ctx,request.ID,request.Token,request.Reason)
  result=map[string]bool{"ok":err==nil}
 default:
  http.NotFound(w,r);return
 }
 if err!=nil {
  if errors.Is(err,queueengine.ErrNotOwner){http.Error(w,"lease not owned or expired",http.StatusConflict);return}
  log.Printf("queue API error: %v",err)
  http.Error(w,"internal queue error",http.StatusInternalServerError);return
 }
 w.Header().Set("Content-Type","application/json")
 if r.URL.Path=="/v1/tasks"{w.WriteHeader(http.StatusCreated)}
 _=json.NewEncoder(w).Encode(result)
}

func serve(ctx context.Context,dbPath,addr,key string) error {
 if len(key)<32 {return errors.New("API key must contain at least 32 characters")}
 host,_,err:=net.SplitHostPort(addr)
 if err!=nil{return fmt.Errorf("invalid listen address: %w",err)}
 if host!="127.0.0.1"&&host!="localhost"&&host!="::1" {
  return errors.New("API binds only to loopback; use an authenticated TLS reverse proxy for remote access")
 }
 store,err:=sqlite.Open(ctx,"sqlite3",dbPath)
 if err!=nil{return err}
 defer store.Close()
 server:=&http.Server{Addr:addr,Handler:&apiServer{store:store,key:key},ReadHeaderTimeout:5*time.Second,ReadTimeout:10*time.Second,WriteTimeout:15*time.Second,IdleTimeout:30*time.Second}
 listener,err:=net.Listen("tcp",addr)
 if err!=nil{return err}
 defer listener.Close()
 log.Printf("experimental standalone API listening on %s",listener.Addr())
 stop:=make(chan struct{})
 go func(){select {case <-ctx.Done():shutdownCtx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel();_=server.Shutdown(shutdownCtx);case <-stop:}}()
 err=server.Serve(listener)
 close(stop)
 if errors.Is(err,http.ErrServerClosed){return nil}
 return err
}
