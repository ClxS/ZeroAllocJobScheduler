using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Schedulers.Utils;

[assembly: CLSCompliant(true)]

namespace Schedulers;

/// <summary>
///     A <see cref="JobScheduler"/> schedules and processes <see cref="IJob"/>s asynchronously.
///     Multiple schedulers can share a <see cref="WorkerPool"/> — each scheduler's
///     <see cref="Wait"/> only steals from its own queues, preventing cross-scheduler contamination.
/// </summary>
public partial class JobScheduler : IDisposable
{
    private readonly SchedulerQueueSet _queueSet;
    private readonly bool _ownsPool;

    /// <summary>
    ///     The <see cref="WorkerPool"/> this scheduler is attached to.
    /// </summary>
    public WorkerPool Pool { get; }

    /// <summary>
    ///     Creates a scheduler with its own dedicated <see cref="WorkerPool"/>.
    ///     The pool is disposed when this scheduler is disposed.
    /// </summary>
    /// <param name="threads">Worker thread count. 0 = processor count.</param>
    public JobScheduler(int threads = 0) : this(new WorkerPool(threads), ownsPool: true)
    {
    }

    /// <summary>
    ///     Creates a scheduler that shares an existing <see cref="WorkerPool"/>.
    ///     The pool is NOT disposed when this scheduler is disposed.
    /// </summary>
    /// <param name="pool">The shared worker pool.</param>
    public JobScheduler(WorkerPool pool) : this(pool, ownsPool: false)
    {
    }

    private JobScheduler(WorkerPool pool, bool ownsPool)
    {
        this.Pool = pool;
        this._ownsPool = ownsPool;
        this._queueSet = new SchedulerQueueSet(this, pool.ThreadCount);
        pool.RegisterScheduler(this._queueSet);
    }

    // Kept for backward compat with Arch internals that read these directly.
    internal List<WorkStealingDeque<JobHandle>> Queues
    {
        get
        {
            // Return a view over this scheduler's queues.
            var list = new List<WorkStealingDeque<JobHandle>>(_queueSet.Queues.Length);
            list.AddRange(_queueSet.Queues);
            return list;
        }
    }

    internal List<Worker> Workers => this.Pool.Workers;

    /// <summary>
    /// An index pointing towards the next worker being used to process the next flushed <see cref="IJob"/>.
    /// </summary>
    internal int NextWorkerIndex { get; set; }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/> from a <see cref="IJob"/>.
    /// </summary>
    public JobHandle Schedule(IJob iJob)
    {
        return JobHandle.Pool.RentJobHandle(iJob);
    }

    /// <summary>
    /// This is similar to wait but doesn't require a job handle, and allow limiting execution to a certain amount of jobs.
    /// </summary>
    /// <param name="max">The maximum amount of jobs that will be executed, 0 means unlimited</param>
    public void TryToExecuteRemainingJobs(int max = 0)
    {
        var totalExecuted = 0;
        var queues = _queueSet.Queues;
        for (var i = 0; i < queues.Length; i++)
        {
            var nextJob = queues[i].TrySteal(out var stolenJob);
            if (!nextJob)
            {
                continue;
            }

            stolenJob.Job.Execute();
            Finish(stolenJob);
            totalExecuted++;
            if (max > 0 && totalExecuted >= max)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Executes every job still queued on this scheduler, in its incoming and work-stealing queues, on the calling
    /// thread. Unlike <see cref="TryToExecuteRemainingJobs"/> this includes flushed jobs no worker has picked up yet, so
    /// a scheduler can be drained before <see cref="Dispose"/> even while every worker is blocked elsewhere.
    /// </summary>
    /// <returns>The number of jobs executed.</returns>
    public int ExecuteQueuedJobs()
    {
        var executed = 0;
        foreach (var incoming in _queueSet.IncomingQueues)
        {
            while (incoming.TryDequeue(out var job))
            {
                job.Job.Execute();
                Finish(job);
                executed++;
            }
        }

        foreach (var queue in _queueSet.Queues)
        {
            while (queue.TrySteal(out var job))
            {
                job.Job.Execute();
                Finish(job);
                executed++;
            }
        }

        return executed;
    }

    /// <summary>
    /// Creates a new <see cref="JobHandle"/> from a <see cref="IJob"/> with a <see cref="JobHandle"/> as a parent.
    /// </summary>
    public JobHandle Schedule(IJob iJob, JobHandle parent)
    {
        Interlocked.Increment(ref parent.UnfinishedJobs);
        var job = JobHandle.Pool.RentJobHandle(iJob);
        job.Parent = parent.Index;
        return job;
    }

    /// <summary>
    /// Creates a dependency between two <see cref="JobHandle"/>s.
    /// </summary>
    public void AddDependency(JobHandle dependency, JobHandle dependOn)
    {
        dependOn.GetDependencies().Add(dependency);
    }

    /// <summary>
    /// Transfers a <see cref="JobHandle"/> to the workers so that it can be executed.
    /// Uses this scheduler's own incoming queues (not shared with other schedulers).
    /// </summary>
    public void Flush(JobHandle job)
    {
        var workerCount = Pool.Workers.Count;
        var workerIndex = NextWorkerIndex;

        while (!_queueSet.IncomingQueues[workerIndex].TryEnqueue(job))
        {
            NextWorkerIndex = (NextWorkerIndex + 1) % workerCount;
        }

        Pool.Workers[workerIndex].Wake();
        NextWorkerIndex = (NextWorkerIndex + 1) % workerCount;
    }

    /// <summary>
    /// Wait until a <see cref="JobHandle"/> and all its children have been completed.
    /// Steals work ONLY from this scheduler's queues — not from other schedulers sharing the pool.
    /// </summary>
    public void Wait(JobHandle job)
    {
        var queues = _queueSet.Queues;
        while (job.UnfinishedJobs > 0)
        {
            for (var i = 0; i < queues.Length; i++)
            {
                var nextJob = queues[i].TrySteal(out var stolenJob);
                if (!nextJob)
                {
                    continue;
                }

                stolenJob.Job.Execute();
                Finish(stolenJob);
            }
        }
    }

    /// <summary>
    /// Completely ends a <see cref="JobHandle"/> and his children by cleaning it up.
    /// </summary>
    internal void Finish(JobHandle job)
    {
        var unfinishedJobs = Interlocked.Decrement(ref job.UnfinishedJobs);
        if (unfinishedJobs != 0)
        {
            return;
        }

        if (job.Parent != ushort.MaxValue)
        {
            Finish(new(job.Parent));
        }

        if (job.HasDependencies())
        {
            for (var index = 0; index < job.GetDependencies().Count; index++)
            {
                var nextJob = job.GetDependencies()[index];
                Flush(nextJob);
            }
        }

        if(job.UnfinishedJobs <0)
        {
            throw new InvalidOperationException("Unfinished jobs cannot be negative");
        }

        JobHandle.Pool.ReturnHandle(job);
    }

    /// <summary>
    /// Unregisters this scheduler's queues. If this scheduler owns the pool, disposes it too.
    /// </summary>
    public void Dispose()
    {
        Pool.UnregisterScheduler(_queueSet);

        if (_ownsPool)
        {
            Pool.Dispose();
        }
    }
}
