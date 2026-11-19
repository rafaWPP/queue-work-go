package sqlite

import (
 "context"
 "database/sql"
 "errors"
 "time"
)

type TaskInfo struct {
 ID string `json:"id"`
 Queue string `json:"queue"`
 Name string `json:"name"`
 Status string `json:"status"`
 Attempts int `json:"attempts"`
 LastError string `json:"last_error,omitempty"`
 AvailableAtMS int64 `json:"available_at_ms"`
 LeaseExpiresAtMS *int64 `json:"lease_expires_at_ms,omitempty"`
}

func scanTaskInfo(scanner interface{Scan(...any)error})(TaskInfo,error){
 var t TaskInfo
 var last sql.NullString
 var expiry sql.NullInt64
 err:=scanner.Scan(&t.ID,&t.Queue,&t.Name,&t.Status,&t.Attempts,&last,&t.AvailableAtMS,&expiry)
 if last.Valid{t.LastError=last.String}
 if expiry.Valid{t.LeaseExpiresAtMS=&expiry.Int64}
 return t,err
}

func (s *Store) ListTasks(ctx context.Context,queue,status string,limit int)([]TaskInfo,error){
 if limit<1||limit>100{return nil,errors.New("limit must be between 1 and 100")}
 if status!=""&&status!="pending"&&status!="processing"&&status!="completed"&&status!="failed"{
  return nil,errors.New("invalid status")
 }
 rows,err:=s.db.QueryContext(ctx,`SELECT id,queue,name,status,attempts,last_error,available_at_ms,lease_expires_at_ms
 FROM tasks WHERE (?='' OR queue=?) AND (?='' OR status=?)
 ORDER BY updated_at_ms DESC,id DESC LIMIT ?`,queue,queue,status,status,limit)
 if err!=nil{return nil,err}
 defer rows.Close()
 tasks:=make([]TaskInfo,0)
 for rows.Next(){
  t,err:=scanTaskInfo(rows)
  if err!=nil{return nil,err}
  tasks=append(tasks,t)
 }
 return tasks,rows.Err()
}

func (s *Store) GetTask(ctx context.Context,id string)(TaskInfo,error){
 return scanTaskInfo(s.db.QueryRowContext(ctx,`SELECT id,queue,name,status,attempts,last_error,available_at_ms,lease_expires_at_ms FROM tasks WHERE id=?`,id))
}

// ReplayFailed requeues only a terminally failed task. It does not reset the
// historical attempt counter; this is a deliberate operator action.
func (s *Store) ReplayFailed(ctx context.Context,id string)error{
 if id==""{return errors.New("task ID is required")}
 now:=time.Now().UnixMilli()
 result,err:=s.db.ExecContext(ctx,`UPDATE tasks SET status='pending',available_at_ms=?,last_error=NULL,
 processing_token=NULL,lease_expires_at_ms=NULL,updated_at_ms=?
 WHERE id=? AND status='failed'`,now,now,id)
 if err!=nil{return err}
 n,err:=result.RowsAffected()
 if err!=nil{return err}
 if n!=1{return sql.ErrNoRows}
 return nil
}
