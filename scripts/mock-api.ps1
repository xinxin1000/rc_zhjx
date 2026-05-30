param(
    [string]$Prefix = "http://localhost:9090/"
)

$listener = [System.Net.HttpListener]::new()
$listener.Prefixes.Add($Prefix)
$listener.Start()

Write-Host "mock api listening on $Prefix"
Write-Host "Use Ctrl+C to stop."

try {
    while ($listener.IsListening) {
        $ctx = $listener.GetContext()
        $reader = [System.IO.StreamReader]::new($ctx.Request.InputStream)
        $body = $reader.ReadToEnd()

        if ($body -like "*timeout*") {
            Start-Sleep -Seconds 5
        }

        if ($body -like "*http_500*") {
            $ctx.Response.StatusCode = 500
            $resp = '{"code":500,"message":"mock http error"}'
        } elseif ($body -like "*biz_fail*") {
            $ctx.Response.StatusCode = 200
            $resp = '{"code":1001,"message":"mock business error"}'
        } else {
            $ctx.Response.StatusCode = 200
            $resp = '{"code":0,"message":"ok"}'
        }

        try {
            $statusCode = $ctx.Response.StatusCode
            $bytes = [Text.Encoding]::UTF8.GetBytes($resp)
            $ctx.Response.ContentType = "application/json"
            $ctx.Response.OutputStream.Write($bytes, 0, $bytes.Length)
            $ctx.Response.Close()

            Write-Host "$($ctx.Request.HttpMethod) $($ctx.Request.RawUrl) => $statusCode $resp"
        } catch {
            Write-Host "$($ctx.Request.HttpMethod) $($ctx.Request.RawUrl) => client disconnected before mock response"
        }
    }
} finally {
    $listener.Stop()
    $listener.Close()
}
