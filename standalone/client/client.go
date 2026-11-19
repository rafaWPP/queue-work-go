// Package client submits jobs to a Queue Work Go standalone server.
// Applications may use this package without accessing the SQLite file.
package client

import (
 "bytes"
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "net/http"
 "strings"
 "time"
)

type Client struct {
 URL string
 APIKey string
 HTTP *http.Client
}

type EnqueueRequest struct {
 Queue string `json:"queue,omitempty"`
 Name string `json:"name"`
 Payload string `json:"payload,omitempty"`
}
type EnqueueResponse struct {
 ID string `json:"id"`
 Status string `json:"status"`
}

func (c *Client) Enqueue(ctx context.Context,task EnqueueRequest)(EnqueueResponse,error){
 var out EnqueueResponse
 if c==nil||c.URL==""||c.APIKey==""{return out,errors.New("client URL and API key required")}
 if task.Name==""{return out,errors.New("task name required")}
 raw,err:=json.Marshal(task)
 if err!=nil{return out,err}
 req,err:=http.NewRequestWithContext(ctx,http.MethodPost,strings.TrimRight(c.URL,"/")+"/v1/tasks",bytes.NewReader(raw))
 if err!=nil{return out,err}
 req.Header.Set("Authorization","Bearer "+c.APIKey)
 req.Header.Set("Content-Type","application/json")
 httpClient:=c.HTTP
 if httpClient==nil{httpClient=&http.Client{Timeout:10*time.Second}}
 resp,err:=httpClient.Do(req)
 if err!=nil{return out,err}
 defer resp.Body.Close()
 body,err:=io.ReadAll(io.LimitReader(resp.Body,4096))
 if err!=nil{return out,err}
 if resp.StatusCode!=http.StatusCreated{return out,fmt.Errorf("queue API returned HTTP %d: %s",resp.StatusCode,strings.TrimSpace(string(body)))}
 if err:=json.Unmarshal(body,&out);err!=nil{return out,err}
 if out.ID==""{return out,errors.New("queue API returned empty task ID")}
 return out,nil
}
