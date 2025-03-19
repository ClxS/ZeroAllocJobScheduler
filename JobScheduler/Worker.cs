﻿using Schedulers.Utils;

namespace Schedulers;

/// <summary>
/// Represents a thread which has a <see cref="WorkStealingDeque{T}"/> and processes <see cref="JobHandle"/>s.
/// Steals <see cref="JobHandle"/>s from other workers if it has nothing more to do.
/// </summary>
internal class Worker
{
    private readonly int _workerId;
    private readonly Thread _thread;

    private readonly UnorderedThreadSafeQueue<JobHandle> _incomingQueue;
    private readonly WorkStealingDeque<JobHandle> _queue;

    private readonly JobScheduler _jobScheduler;
    private volatile CancellationTokenSource _cancellationToken;

    private AutoResetEvent _workAvailable = new AutoResetEvent(false);

    /// <summary>
    /// Creates a new <see cref="Worker"/>.
    /// </summary>
    /// <param name="jobScheduler">Its <see cref="JobScheduler"/>.</param>
    /// <param name="id">Its <see cref="id"/>.</param>
    public Worker(JobScheduler jobScheduler, int id)
    {
        _workerId = id;

        _incomingQueue = new();
        _queue = new(32);

        _jobScheduler = jobScheduler;
        _cancellationToken = new();

        _thread = new(() => Run(_cancellationToken.Token));
    }

    /// <summary>
    /// Its <see cref="UnorderedThreadSafeQueue{T}"/> with <see cref="JobHandle"/>s which are transferred into the <see cref="Queue"/>.
    /// </summary>
    public UnorderedThreadSafeQueue<JobHandle> IncomingQueue
    {
        get => _incomingQueue;
    }

    /// <summary>
    /// Its <see cref="WorkStealingDeque{T}"/> with <see cref="JobHandle"/>s to process.
    /// </summary>
    public WorkStealingDeque<JobHandle> Queue
    {
        get => _queue;
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
    /// Signals that this thread now has work available for consumption
    /// </summary>
    public void Wake()
    {
        _workAvailable.Set();
    }

    /// <summary>
    /// Runs this instance to process its <see cref="JobHandle"/>s.
    /// Steals from other <see cref="Worker"/>s if its own <see cref="_queue"/> is empty.
    /// </summary>
    /// <param name="token"></param>
    private void Run(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                // Pass jobs to the local queue
                while (_queue.Size() < 32 && _incomingQueue.TryDequeue(out var jobHandle))
                {
                    _queue.PushBottom(jobHandle);
                }

                // Process job in own queue
                var exists = _queue.TryPopBottom(out var job);
                if (exists)
                {
                    job.Job.Execute();
                    _jobScheduler.Finish(job);
                }
                else
                {
                    // Try to steal job from different queue
                    for (var i = 0; i < _jobScheduler.Queues.Count; i++)
                    {
                        if (i == _workerId)
                        {
                            continue;
                        }

                        exists = _jobScheduler.Queues[i].TrySteal(out job);
                        if (!exists)
                        {
                            continue;
                        }

                        job.Job.Execute();
                        _jobScheduler.Finish(job);
                        break;
                    }

                    if (!exists)
                    {
                        _workAvailable.WaitOne(1);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            //Console.WriteLine("Operation was canceled");
        }
        finally
        {
            //Console.WriteLine("Worker thread is cleaning up and exiting");
        }
    }
}
