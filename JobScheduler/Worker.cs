using Schedulers.Utils;

namespace Schedulers;

/// <summary>
/// Represents a thread which processes <see cref="JobHandle"/>s from all registered schedulers.
/// Steals <see cref="JobHandle"/>s from other workers if it has nothing more to do.
/// </summary>
internal class Worker
{
    private readonly int _workerId;
    private readonly Thread _thread;
    private readonly WorkerPool _pool;
    private volatile CancellationTokenSource _cancellationToken;
    private AutoResetEvent _workAvailable = new AutoResetEvent(false);

    /// <summary>
    /// Creates a new <see cref="Worker"/>.
    /// </summary>
    /// <param name="pool">The <see cref="WorkerPool"/> that owns this worker.</param>
    /// <param name="id">Worker ID (index into per-scheduler queue arrays).</param>
    public Worker(WorkerPool pool, int id)
    {
        _workerId = id;
        _pool = pool;
        _cancellationToken = new();
        _thread = new(() => Run(_cancellationToken.Token));
    }

    /// <summary>
    /// Starts this instance.
    /// </summary>
    public void Start()
    {
        _thread.Start();
    }

    /// <summary>
    /// Stops this instance.
    /// </summary>
    public void Stop()
    {
        _cancellationToken.Cancel();
    }

    /// <summary>
    /// Signals that this thread now has work available for consumption.
    /// </summary>
    public void Wake()
    {
        _workAvailable.Set();
    }

    /// <summary>
    /// Runs this instance to process <see cref="JobHandle"/>s from all registered schedulers.
    /// Steals from other <see cref="Worker"/>s if its own queues are empty.
    /// </summary>
    private void Run(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var didWork = false;

                // Iterate all registered schedulers
                var schedulers = _pool.RegisteredSchedulers;
                foreach (var qs in schedulers)
                {
                    // Transfer incoming jobs to local deque
                    var queue = qs.Queues[_workerId];
                    var incoming = qs.IncomingQueues[_workerId];
                    while (queue.Size() < 32 && incoming.TryDequeue(out var jobHandle))
                    {
                        queue.PushBottom(jobHandle);
                    }

                    // Process own queue
                    if (queue.TryPopBottom(out var job))
                    {
                        job.Job.Execute();
                        qs.Scheduler.Finish(job);
                        didWork = true;
                    }
                }

                if (didWork)
                {
                    continue;
                }

                // Try to steal from other workers' queues (across all schedulers)
                var stolen = false;
                foreach (var qs in schedulers)
                {
                    var workerCount = qs.Queues.Length;
                    for (var i = 0; i < workerCount; i++)
                    {
                        if (i == _workerId)
                        {
                            continue;
                        }

                        if (qs.Queues[i].TrySteal(out var stolenJob))
                        {
                            stolenJob.Job.Execute();
                            qs.Scheduler.Finish(stolenJob);
                            stolen = true;
                            break;
                        }
                    }

                    if (stolen)
                    {
                        break;
                    }
                }

                if (!stolen)
                {
                    _workAvailable.WaitOne(1);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
