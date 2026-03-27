using Schedulers.Utils;

namespace Schedulers;

/// <summary>
///     Owns the OS worker threads shared across one or more <see cref="JobScheduler"/> instances.
///     Each scheduler registers its own queue set so workers can process jobs from all schedulers,
///     while <see cref="JobScheduler.Wait"/> only steals from its own queues (scheduler isolation).
/// </summary>
public class WorkerPool : IDisposable
{
    internal List<Worker> Workers { get; } = [];

    /// <summary>
    ///     Number of worker threads.
    /// </summary>
    public int ThreadCount { get; }

    /// <summary>
    ///     All registered scheduler queue sets. Workers iterate these to find work.
    ///     Copy-on-write: the array reference is replaced atomically on register/unregister.
    /// </summary>
    internal volatile SchedulerQueueSet[] RegisteredSchedulers = [];

    /// <summary>
    ///     Creates a worker pool with the given number of threads.
    /// </summary>
    /// <param name="threads">Number of worker threads. 0 = processor count.</param>
    public WorkerPool(int threads = 0)
    {
        this.ThreadCount = threads == 0 ? Environment.ProcessorCount : threads;

        for (var index = 0; index < this.ThreadCount; index++)
        {
            var worker = new Worker(this, index);
            this.Workers.Add(worker);
        }

        foreach (var worker in this.Workers)
        {
            worker.Start();
        }
    }

    /// <summary>
    ///     Registers a scheduler's queue set so workers can process its jobs.
    /// </summary>
    internal void RegisterScheduler(SchedulerQueueSet queueSet)
    {
        lock (this.Workers)
        {
            var current = this.RegisteredSchedulers;
            var updated = new SchedulerQueueSet[current.Length + 1];
            current.CopyTo(updated, 0);
            updated[current.Length] = queueSet;
            this.RegisteredSchedulers = updated;
        }
    }

    /// <summary>
    ///     Unregisters a scheduler's queue set.
    /// </summary>
    internal void UnregisterScheduler(SchedulerQueueSet queueSet)
    {
        lock (this.Workers)
        {
            var current = this.RegisteredSchedulers;
            var updated = new SchedulerQueueSet[current.Length - 1];
            var writeIndex = 0;
            foreach (var qs in current)
            {
                if (qs != queueSet)
                {
                    updated[writeIndex++] = qs;
                }
            }
            this.RegisteredSchedulers = updated;
        }
    }

    /// <summary>
    ///     Stops all worker threads.
    /// </summary>
    public void Dispose()
    {
        foreach (var worker in this.Workers)
        {
            worker.Stop();
        }
    }
}

/// <summary>
///     Per-scheduler queue pair. Each <see cref="JobScheduler"/> creates one of these
///     with an incoming queue and work-stealing deque per worker.
/// </summary>
internal class SchedulerQueueSet
{
    public JobScheduler Scheduler { get; }
    public UnorderedThreadSafeQueue<JobHandle>[] IncomingQueues { get; }
    public WorkStealingDeque<JobHandle>[] Queues { get; }

    public SchedulerQueueSet(JobScheduler scheduler, int workerCount)
    {
        this.Scheduler = scheduler;
        this.IncomingQueues = new UnorderedThreadSafeQueue<JobHandle>[workerCount];
        this.Queues = new WorkStealingDeque<JobHandle>[workerCount];

        for (var i = 0; i < workerCount; i++)
        {
            this.IncomingQueues[i] = new UnorderedThreadSafeQueue<JobHandle>();
            this.Queues[i] = new WorkStealingDeque<JobHandle>(32);
        }
    }
}
