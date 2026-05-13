using System.Threading.Channels;

var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

var result = await Enumerable.Range(1, 20)
    .ToAsync(cts.Token)
    .Pipe(parallelism: 4, capacity: 8, Work, cts.Token)
    .AggregateAsync(0, (sum, item) => sum + item.Value, cts.Token);

Console.WriteLine(result);

static async ValueTask<Job> Work(int value, CancellationToken ct)
{
    await Task.Delay(Random.Shared.Next(20, 80), ct);
    return new Job(value, value * value);
}

record Job(int Input, int Value);

static class AsyncFlow
{
    public static async IAsyncEnumerable<T> ToAsync<T>(
        this IEnumerable<T> source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in source)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return item;
        }
    }

    public static IAsyncEnumerable<TOut> Pipe<TIn, TOut>(
        this IAsyncEnumerable<TIn> source,
        int parallelism,
        int capacity,
        Func<TIn, CancellationToken, ValueTask<TOut>> transform,
        CancellationToken ct = default)
    {
        var input = Channel.CreateBounded<TIn>(capacity);
        var output = Channel.CreateBounded<TOut>(capacity);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(ct))
                    await input.Writer.WriteAsync(item, ct);

                input.Writer.Complete();
            }
            catch (Exception ex)
            {
                input.Writer.Complete(ex);
            }
        }, ct);

        var workers = Enumerable.Range(0, parallelism).Select(_ => Task.Run(async () =>
        {
            await foreach (var item in input.Reader.ReadAllAsync(ct))
                await output.Writer.WriteAsync(await transform(item, ct), ct);
        }, ct)).ToArray();

        _ = Task.WhenAll(workers).ContinueWith(t =>
        {
            if (t.Exception is null) output.Writer.Complete();
            else output.Writer.Complete(t.Exception.InnerException);
        }, CancellationToken.None);

        return output.Reader.ReadAllAsync(ct);
    }

    public static async Task<TState> AggregateAsync<T, TState>(
        this IAsyncEnumerable<T> source,
        TState seed,
        Func<TState, T, TState> reduce,
        CancellationToken ct = default)
    {
        var state = seed;

        await foreach (var item in source.WithCancellation(ct))
            state = reduce(state, item);

        return state;
    }
}
