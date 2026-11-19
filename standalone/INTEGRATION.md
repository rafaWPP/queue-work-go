# Integrating applications with Queue Work Go

The standalone server accepts jobs over HTTP. Producers can be written in any
language that can make authenticated HTTP requests. A job's `name` must match
a handler registered in the worker; enqueueing a name does **not** automatically
install or execute a new handler.

## Go producer

Import `go-queue/standalone/client` from the standalone module:

```go
producer := &client.Client{
    URL:    "http://127.0.0.1:8088",
    APIKey: os.Getenv("QUEUE_API_KEY"),
}
job, err := producer.Enqueue(ctx, client.EnqueueRequest{
    Queue:   "default",
    Name:    "builtin/log",
    Payload: "hello",
})
if err != nil {
    return err
}
fmt.Println(job.ID)
```

## PHP producer (framework-independent)

```php
<?php
$payload = json_encode([
    'queue' => 'default',
    'name' => 'builtin/log',
    'payload' => json_encode(['customer_id' => 42]),
], JSON_THROW_ON_ERROR);

$ch = curl_init('http://127.0.0.1:8088/v1/tasks');
curl_setopt_array($ch, [
    CURLOPT_POST => true,
    CURLOPT_POSTFIELDS => $payload,
    CURLOPT_HTTPHEADER => [
        'Content-Type: application/json',
        'Authorization: Bearer ' . getenv('QUEUE_API_KEY'),
    ],
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT => 10,
]);
$body = curl_exec($ch);
$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
if ($body === false || $status !== 201) {
    throw new RuntimeException('Queue submission failed: HTTP ' . $status);
}
$job = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
echo $job['id'];
```

## Python producer (standard library)

```python
import json
import os
import urllib.request

data = json.dumps({
    "queue": "default",
    "name": "builtin/log",
    "payload": json.dumps({"customer_id": 42}),
}).encode()

request = urllib.request.Request(
    "http://127.0.0.1:8088/v1/tasks",
    data=data,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + os.environ["QUEUE_API_KEY"],
    },
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    print(json.load(response)["id"])
```

The default server binds only to loopback. To submit jobs from another
machine, deploy a secured TLS reverse proxy or private network gateway.
Do not expose the development API directly to the public internet.

**Current limitation:** the standalone binary includes only
`builtin/noop` and `builtin/log` handlers. Implementing arbitrary
application jobs requires building a worker with registered handlers.
The producer SDK does not execute jobs.
