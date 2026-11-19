// Package sqlite implements a durable, single-node SQLite queue backend.
// A SQLite database/sql driver must be registered by the embedding program.
package sqlite

import (
 "context"
 "crypto/rand"
 "database/sql"
 "encoding/hex"
 "errors"
 "fmt"
 "strings"
 "time"

 "go-queue/internal/queueengine"
)

type Store struct { db *sql.DB }

// Open uses a registered SQLite database/sql driver. It never touches MariaDB.
// Pass a private SQLite file path, not a shared network filesystem.
func Open(ctx context.Context, driverName, dsn string) (*Store, error) {
 if driverName == "" || dsn == "" { return nil, errors.New("SQLite driver and DSN are required") }
 db, err := sql.Open(driverName, dsn)
 if err != nil { return nil, err }
 // SQLite has one writer; serialize access and avoid per-connection PRAGMA drift.
 db.SetMaxOpenConns(1)
 db.SetMaxIdleConns(1)
 if err := db.PingContext(ctx); err != nil { db.Close(); return nil, err }
 for _, pragma := range []string{"PRAGMA busy_timeout = 5000", "PRAGMA journal_mode = WAL", "PRAGMA synchronous = FULL", "PRAGMA foreign_keys = ON"} {
  if _, err := db.ExecContext(ctx, pragma); err != nil { db.Close(); return nil, fmt.Errorf("configure SQLite: %w", err) }
 }
 for _, statement := range schemaStatements {
  if _, err := db.ExecContext(ctx, statement); err != nil { db.Close(); return nil, fmt.Errorf("initialize SQLite queue: %w", err) }
 }
 return &Store{db:db}, nil
}

var schemaStatements = []string{
 `CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY NOT NULL,
 queue TEXT NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','completed','failed')),
 available_at_ms INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 processing_token TEXT, lease_expires_at_ms INTEGER, last_error TEXT,
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 CHECK ((status='processing' AND processing_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
 OR (status<>'processing' AND processing_token IS NULL AND lease_expires_at_ms IS NULL)))`,
 `CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(queue,status,available_at_ms,id)`,
 `CREATE INDEX IF NOT EXISTS idx_tasks_expired_lease ON tasks(status,lease_expires_at_ms) WHERE status='processing'`,
}

func randomID() (string,error) {
 var b [16]byte
 if _,err:=rand.Read(b[:]); err!=nil {return "",err}
 return hex.EncodeToString(b[:]),nil
}

func (s *Store) Enqueue(ctx context.Context, t queueengine.Task) (string,error) {
 if strings.TrimSpace(t.Queue)=="" || strings.TrimSpace(t.Name)=="" {return "",errors.New("queue and task name are required")}
 if t.ID=="" { id,err:=randomID();if err!=nil{return "",err};t.ID=id }
 now:=time.Now().UnixMilli()
 available:=t.AvailableAt.UnixMilli()
 if t.AvailableAt.IsZero(){available=now}
 payload:=t.Payload
 if payload==nil {payload=[]byte{}}
 _,err:=s.db.ExecContext(ctx,`INSERT INTO tasks(id,queue,name,payload,status,available_at_ms,attempts,created_at_ms,updated_at_ms)
 VALUES(?,?,?,?,'pending',?,?,?,?)`,t.ID,t.Queue,t.Name,payload,available,0,now,now)
 return t.ID,err
}

func (s *Store) Claim(ctx context.Context, queue string, duration time.Duration) (queueengine.Lease,bool,error) {
 var lease queueengine.Lease
 if duration<=0 {return lease,false,errors.New("lease duration must be positive")}
 if strings.TrimSpace(queue)=="" {return lease,false,errors.New("queue is required")}
 token,err:=randomID();if err!=nil{return lease,false,err}
 now:=time.Now()
 expiry:=now.Add(duration)
 // Single atomic UPDATE with RETURNING also reclaims expired leases.
 // The old token is replaced before any new worker receives the task.
 row:=s.db.QueryRowContext(ctx,`UPDATE tasks SET status='processing',
 processing_token=?, lease_expires_at_ms=?, attempts=attempts+1, updated_at_ms=?
 WHERE id=(SELECT id FROM tasks WHERE queue=? AND available_at_ms<=?
 AND (status='pending' OR (status='processing' AND lease_expires_at_ms<=?))
 ORDER BY available_at_ms,id LIMIT 1)
 AND (status='pending' OR (status='processing' AND lease_expires_at_ms<=?))
 RETURNING id,queue,name,payload,available_at_ms,attempts`,
 token,expiry.UnixMilli(),now.UnixMilli(),queue,now.UnixMilli(),now.UnixMilli(),now.UnixMilli())
 var available int64
 err=row.Scan(&lease.Task.ID,&lease.Task.Queue,&lease.Task.Name,&lease.Task.Payload,&available,&lease.Task.Attempts)
 if errors.Is(err,sql.ErrNoRows){return queueengine.Lease{},false,nil}
 if err!=nil{return queueengine.Lease{},false,err}
 lease.Task.AvailableAt=time.UnixMilli(available)
 lease.Token=token
 lease.ExpiresAt=expiry
 return lease,true,nil
}

func (s *Store) Renew(ctx context.Context, id, token string, duration time.Duration) error {
 if id=="" || token=="" || duration<=0 {return errors.New("valid task, token and duration are required")}
 now:=time.Now()
 result,err:=s.db.ExecContext(ctx,`UPDATE tasks SET lease_expires_at_ms=?,updated_at_ms=?
 WHERE id=? AND status='processing' AND processing_token=? AND lease_expires_at_ms>?`,
 now.Add(duration).UnixMilli(),now.UnixMilli(),id,token,now.UnixMilli())
 return ownerResult(result,err)
}

func (s *Store) Complete(ctx context.Context,id,token string) error {
 if id==""||token=="" {return queueengine.ErrNotOwner}
 now:=time.Now().UnixMilli()
 result,err:=s.db.ExecContext(ctx,`UPDATE tasks SET status='completed',processing_token=NULL,
 lease_expires_at_ms=NULL,updated_at_ms=?,last_error=NULL
 WHERE id=? AND status='processing' AND processing_token=? AND lease_expires_at_ms>?`,now,id,token,now)
 return ownerResult(result,err)
}

func (s *Store) Fail(ctx context.Context,id,token,reason string) error {
 if id==""||token=="" {return queueengine.ErrNotOwner}
 now:=time.Now().UnixMilli()
 result,err:=s.db.ExecContext(ctx,`UPDATE tasks SET status='failed',processing_token=NULL,
 lease_expires_at_ms=NULL,updated_at_ms=?,last_error=?
 WHERE id=? AND status='processing' AND processing_token=? AND lease_expires_at_ms>?`,now,reason,id,token,now)
 return ownerResult(result,err)
}

func ownerResult(result sql.Result,err error) error {
 if err!=nil{return err}
 count,err:=result.RowsAffected()
 if err!=nil{return err}
 if count!=1{return queueengine.ErrNotOwner}
 return nil
}

func (s *Store) Close() error {return s.db.Close()}

var _ queueengine.Store = (*Store)(nil)
