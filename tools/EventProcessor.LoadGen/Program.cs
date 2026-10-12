// Load generator: posts batches of events to /api/events/batch from many concurrent senders
// and reports how many events per second the API accepted.
//
// Settings (environment variables or --key=value):
//   TARGET_URL   http://api/api/events/batch
//   TOTAL_EVENTS 1000000
//   BATCH_SIZE   1000
//   CONCURRENCY  32
//   EVENT_TYPE   load.test
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

var settings = Environment.GetCommandLineArgs().Skip(1)
    .Select(a => a.TrimStart('-').Split('=', 2))
    .Where(kv => kv.Length == 2)
    .ToDictionary(kv => kv[0].ToUpperInvariant().Replace('-', '_'), kv => kv[1]);

string Get(string key, string fallback) =>
    settings.TryGetValue(key, out var v) ? v : Environment.GetEnvironmentVariable(key) ?? fallback;

var url = Get("TARGET_URL", "http://localhost:8080/api/events/batch");
var total = long.Parse(Get("TOTAL_EVENTS", "1000000"));
var batchSize = int.Parse(Get("BATCH_SIZE", "1000"));
var concurrency = int.Parse(Get("CONCURRENCY", "32"));
var eventType = Get("EVENT_TYPE", "load.test");

var batches = (int)Math.Ceiling(total / (double)batchSize);
Console.WriteLine($"Sending {total:N0} events to {url} in {batches:N0} batches of {batchSize} with {concurrency} concurrent senders");

using var http = new HttpClient(new SocketsHttpHandler
{
    MaxConnectionsPerServer = concurrency,
    PooledConnectionLifetime = TimeSpan.FromMinutes(5),
    AutomaticDecompression = DecompressionMethods.None
})
{ Timeout = TimeSpan.FromSeconds(60) };

long nextBatch = -1, accepted = 0, retries = 0, failed = 0;
var sw = Stopwatch.StartNew();

async Task Sender()
{
    while (true)
    {
        var index = Interlocked.Increment(ref nextBatch);
        if (index >= batches) return;

        var count = (int)Math.Min(batchSize, total - index * batchSize);
        var now = DateTime.UtcNow;
        var events = new object[count];
        for (var i = 0; i < count; i++)
        {
            events[i] = new { eventId = Guid.NewGuid(), type = eventType, payload = "{\"seq\":" + (index * batchSize + i) + "}", createdAt = now };
        }
        var body = JsonSerializer.SerializeToUtf8Bytes(events);

        for (var attempt = 1; ; attempt++)
        {
            using var content = new ByteArrayContent(body);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            try
            {
                using var response = await http.PostAsync(url, content);
                if (response.IsSuccessStatusCode)
                {
                    Interlocked.Add(ref accepted, count);
                    break;
                }
                // 429 (rate limit) and 503 (backpressure) mean "slow down", not "failed"
                if ((response.StatusCode is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable) && attempt < 50)
                {
                    Interlocked.Increment(ref retries);
                    await Task.Delay(Math.Min(50 * attempt, 1000));
                    continue;
                }
                Console.WriteLine($"Batch {index} failed: {(int)response.StatusCode}");
            }
            catch (Exception ex) when (attempt < 5)
            {
                Interlocked.Increment(ref retries);
                Console.WriteLine($"Batch {index} attempt {attempt}: {ex.Message}");
                await Task.Delay(500);
                continue;
            }
            Interlocked.Add(ref failed, count);
            break;
        }
    }
}

using var progress = new Timer(_ =>
    Console.WriteLine($"  {sw.Elapsed.TotalSeconds,6:N1}s  accepted {Interlocked.Read(ref accepted),12:N0}  ({Interlocked.Read(ref accepted) / Math.Max(sw.Elapsed.TotalSeconds, 0.001),9:N0}/s)"),
    null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

await Task.WhenAll(Enumerable.Range(0, concurrency).Select(_ => Task.Run(Sender)));
sw.Stop();

var rate = accepted / sw.Elapsed.TotalSeconds;
Console.WriteLine($"LOADGEN_RESULT accepted={accepted} failed={failed} retries={retries} seconds={sw.Elapsed.TotalSeconds:F2} rate={rate:F0}");
return failed == 0 ? 0 : 1;
