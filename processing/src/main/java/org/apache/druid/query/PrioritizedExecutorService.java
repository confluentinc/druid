/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A prioritized executor backed by one or more independent thread pools ("shards"), each with its own
 * {@link PriorityBlockingQueue}. Sharding exists to relieve contention on the single queue lock: with one queue,
 * every producer (submit) and every worker (take) serializes on one {@link java.util.concurrent.locks.ReentrantLock},
 * which becomes a bottleneck under very high task rates (e.g. one task per segment scan). Splitting into
 * {@code druid.processing.numThreadPools} shards means each queue lock only sees ~1/N of the traffic and ~1/N of the
 * worker threads contending on it.
 *
 * Tasks are routed to a shard at submit time using {@link ThreadLocalRandom} (no shared counter, so the router adds
 * no contention of its own). The trade-off is that priority ordering is per-shard rather than global; for the
 * high-throughput per-segment workload this pool serves, that is an acceptable exchange for the reduced contention.
 * With {@code numThreadPools == 1} (the default) this behaves exactly like the previous single-pool implementation.
 */
public class PrioritizedExecutorService extends AbstractExecutorService implements ListeningExecutorService
{
  public static PrioritizedExecutorService create(Lifecycle lifecycle, DruidProcessingConfig config)
  {
    final int numPools = Math.max(1, config.getNumThreadPools());
    final int totalThreads = config.getNumThreads();
    final ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build();

    final ThreadPoolExecutor[] shards = new ThreadPoolExecutor[numPools];
    for (int p = 0; p < numPools; p++) {
      // Spread the configured total thread count across the pools, giving the remainder to the first pools.
      final int threadsForPool = totalThreads / numPools + (p < (totalThreads % numPools) ? 1 : 0);
      shards[p] = new ThreadPoolExecutor(
          threadsForPool,
          threadsForPool,
          0L,
          TimeUnit.MILLISECONDS,
          new PriorityBlockingQueue<>(),
          threadFactory
      );
    }

    final PrioritizedExecutorService service = new PrioritizedExecutorService(shards, config);

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {
          }

          @Override
          public void stop()
          {
            service.shutdownNow();
          }
        }
    );

    return service;
  }

  private final AtomicLong queuePosition = new AtomicLong(Long.MAX_VALUE);
  private final ThreadPoolExecutor[] shards;
  private final boolean allowRegularTasks;
  private final int defaultPriority;
  private final DruidProcessingConfig config;
  final ThreadPoolExecutor threadPoolExecutor; // == shards[0]; retained for unit tests

  public PrioritizedExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      DruidProcessingConfig config
  )
  {
    this(new ThreadPoolExecutor[]{threadPoolExecutor}, false, 0, config);
  }

  public PrioritizedExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      boolean allowRegularTasks,
      int defaultPriority,
      DruidProcessingConfig config
  )
  {
    this(new ThreadPoolExecutor[]{threadPoolExecutor}, allowRegularTasks, defaultPriority, config);
  }

  public PrioritizedExecutorService(
      ThreadPoolExecutor[] shards,
      DruidProcessingConfig config
  )
  {
    this(shards, false, 0, config);
  }

  public PrioritizedExecutorService(
      ThreadPoolExecutor[] shards,
      boolean allowRegularTasks,
      int defaultPriority,
      DruidProcessingConfig config
  )
  {
    Preconditions.checkArgument(shards != null && shards.length > 0, "need at least one thread pool");
    this.shards = shards;
    this.threadPoolExecutor = shards[0];
    this.allowRegularTasks = allowRegularTasks;
    this.defaultPriority = defaultPriority;
    this.config = config;
  }

  @Override
  protected <T> PrioritizedListenableFutureTask<T> newTaskFor(Runnable runnable, T value)
  {
    Preconditions.checkArgument(
        allowRegularTasks || runnable instanceof PrioritizedRunnable,
        "task does not implement PrioritizedRunnable"
    );
    return PrioritizedListenableFutureTask.create(
        ListenableFutureTask.create(runnable, value),
        runnable instanceof PrioritizedRunnable
        ? ((PrioritizedRunnable) runnable).getPriority()
        : defaultPriority,
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  @Override
  protected <T> PrioritizedListenableFutureTask<T> newTaskFor(Callable<T> callable)
  {
    Preconditions.checkArgument(
        allowRegularTasks || callable instanceof PrioritizedCallable,
        "task does not implement PrioritizedCallable"
    );
    return PrioritizedListenableFutureTask.create(
        ListenableFutureTask.create(callable),
        callable instanceof PrioritizedCallable
        ? ((PrioritizedCallable) callable).getPriority()
        : defaultPriority,
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  @Override
  public ListenableFuture<?> submit(Runnable task)
  {
    return (ListenableFuture<?>) super.submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, @Nullable T result)
  {
    return (ListenableFuture<T>) super.submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task)
  {
    return (ListenableFuture<T>) super.submit(task);
  }

  @Override
  public void execute(final Runnable runnable)
  {
    final Runnable task = (runnable instanceof PrioritizedListenableFutureTask)
                          ? runnable
                          : newTaskFor(runnable, null);
    // Route to a shard. ThreadLocalRandom is contention-free (no shared counter/cache line), and spreads
    // homogeneous high-rate tasks evenly enough that per-shard queue depths stay balanced.
    final int shard = shards.length == 1 ? 0 : ThreadLocalRandom.current().nextInt(shards.length);
    shards[shard].execute(task);
  }

  @Override
  public void shutdown()
  {
    for (ThreadPoolExecutor shard : shards) {
      shard.shutdown();
    }
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    final List<Runnable> pending = new ArrayList<>();
    for (ThreadPoolExecutor shard : shards) {
      pending.addAll(shard.shutdownNow());
    }
    return pending;
  }

  @Override
  public boolean isShutdown()
  {
    for (ThreadPoolExecutor shard : shards) {
      if (!shard.isShutdown()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isTerminated()
  {
    for (ThreadPoolExecutor shard : shards) {
      if (!shard.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    for (ThreadPoolExecutor shard : shards) {
      final long remainingNanos = deadlineNanos - System.nanoTime();
      if (!shard.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Total number of queued (not-yet-running) tasks across all shards. Summed on demand; only called by the
   * periodic metrics monitor, never on the task hot path.
   */
  public int getQueueSize()
  {
    int total = 0;
    for (ThreadPoolExecutor shard : shards) {
      total += shard.getQueue().size();
    }
    return total;
  }

  /**
   * Returns the approximate number of tasks being run by the thread pools currently, summed across shards.
   */
  public int getActiveTasks()
  {
    int total = 0;
    for (ThreadPoolExecutor shard : shards) {
      total += shard.getActiveCount();
    }
    return total;
  }
}

class PrioritizedListenableFutureTask<V> implements RunnableFuture<V>,
    ListenableFuture<V>,
    PrioritizedRunnable,
    Comparable<PrioritizedListenableFutureTask>
{
  // NOTE: For priority HIGHER numeric value means more priority. As such we swap left and right in the compares
  private static final Comparator<PrioritizedListenableFutureTask> PRIORITY_COMPARATOR = new Ordering<PrioritizedListenableFutureTask>()
  {
    @Override
    public int compare(PrioritizedListenableFutureTask left, PrioritizedListenableFutureTask right)
    {
      return Integer.compare(right.getPriority(), left.getPriority());
    }
  }.compound(
      new Ordering<>()
      {
        @Override
        public int compare(PrioritizedListenableFutureTask left, PrioritizedListenableFutureTask right)
        {
          return Long.compare(right.getInsertionPlace(), left.getInsertionPlace());
        }
      }
  );

  public static <V> PrioritizedListenableFutureTask<V> create(ListenableFutureTask<V> task, int priority, long position)
  {
    return new PrioritizedListenableFutureTask<>(task, priority, position);
  }

  private final ListenableFutureTask<V> delegate;
  private final int priority;
  private final long insertionPlace;

  PrioritizedListenableFutureTask(ListenableFutureTask<V> delegate, int priority, long position)
  {
    this.delegate = delegate;
    this.priority = priority;
    this.insertionPlace = position; // Long.MAX_VALUE will always be "highest"
  }

  @Override
  public void run()
  {
    delegate.run();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning)
  {
    return delegate.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled()
  {
    return delegate.isCancelled();
  }

  @Override
  public boolean isDone()
  {
    return delegate.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException
  {
    return delegate.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
  {
    return delegate.get(timeout, unit);
  }

  @Override
  public void addListener(Runnable listener, Executor executor)
  {
    delegate.addListener(listener, executor);
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  protected long getInsertionPlace()
  {
    return insertionPlace;
  }

  @Override
  public int compareTo(PrioritizedListenableFutureTask otherTask)
  {
    return PRIORITY_COMPARATOR.compare(this, otherTask);
  }
}
