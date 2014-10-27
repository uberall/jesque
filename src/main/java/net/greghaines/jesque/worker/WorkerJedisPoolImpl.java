/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque.worker;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.worker.JobExecutor.State.*;
import static net.greghaines.jesque.worker.WorkerEvent.*;

/**
 * Basic implementation of the Worker interface. Obeys the contract of a Resque
 * worker in Redis.
 *
 * @author Greg Haines
 * @author Animesh Kumar
 */
public class WorkerJedisPoolImpl implements Worker {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerJedisPoolImpl.class);
    private static final AtomicLong WORKER_COUNTER = new AtomicLong(0);
    protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms
    protected static final long RECONNECT_SLEEP_TIME = 5000; // 5 sec
    protected static final int RECONNECT_ATTEMPTS = 120; // Total time: 10 min

    // Set the thread name to the message for debugging
    private static volatile boolean threadNameChangingEnabled = false;

    /**
     * @return true if worker threads names will change during normal operation
     */
    public static boolean isThreadNameChangingEnabled() {
        return threadNameChangingEnabled;
    }

    /**
     * Enable/disable worker thread renaming during normal operation. (Disabled
     * by default)
     * <p/>
     * <strong>Warning: Enabling this feature is very expensive
     * CPU-wise!</strong><br>
     * This feature is designed to assist in debugging worker state but should
     * be disabled in production environments for performance reasons.
     *
     * @param enabled whether threads' names should change during normal operation
     */
    public static void setThreadNameChangingEnabled(final boolean enabled) {
        threadNameChangingEnabled = enabled;
    }

    /**
     * Verify that the given queues are all valid.
     *
     * @param queues the given queues
     */
    protected static void checkQueues(final Iterable<String> queues) {
        if (queues == null) {
            throw new IllegalArgumentException("queues must not be null");
        }
        for (final String queue : queues) {
            if (queue == null || "".equals(queue)) {
                throw new IllegalArgumentException("queues' members must not be null: " + queues);
            }
        }
    }

    protected final Config config;
    protected final Pool<Jedis> jedisPool;
    private Jedis currentJedis = null;
    protected final String namespace;
    protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>();
    private final String name;
    protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean processingJob = new AtomicBoolean(false);
    private final long workerId = WORKER_COUNTER.getAndIncrement();
    private final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef =
            new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler());
    private final JobFactory jobFactory;

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.
     * The worker will only listen to the supplied queues and execute jobs
     * that are provided by the given job factory.
     *
     * @param config     used to create a connection to Redis and the package prefix
     *                   for incoming jobs
     * @param queues     the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedisPool  the connection pool to Redis
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    public WorkerJedisPoolImpl(final Config config, final Collection<String> queues,
                               final JobFactory jobFactory, final Pool<Jedis> jedisPool) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (jobFactory == null) {
            throw new IllegalArgumentException("jobFactory must not be null");
        }
        if (jedisPool == null) {
            throw new IllegalArgumentException("jedis must not be null");
        }
        checkQueues(queues);
        this.config = config;
        this.jobFactory = jobFactory;
        this.namespace = config.getNamespace();
        this.jedisPool = jedisPool;
        setQueues(queues);
        this.name = createName();
    }

    /**
     * @return this worker's identifier
     */
    public long getWorkerId() {
        return this.workerId;
    }

    /**
     * Starts this worker. Registers the worker in Redis and begins polling the
     * queues for jobs. Stop this worker by calling end() on any thread.
     */
    @Override
    public void run() {
        final Worker self = this;
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) {
                    if (state.compareAndSet(NEW, RUNNING)) {
                        try {
                            renameThread("RUNNING");
                            threadRef.set(Thread.currentThread());
                            jedis.sadd(key(WORKERS), name);
                            jedis.set(key(WORKER, name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
                            listenerDelegate.fireEvent(WORKER_START, self, null, null, null, null, null);
                            poll();
                        } finally {
                            renameThread("STOPPING");
                            listenerDelegate.fireEvent(WORKER_STOP, self, null, null, null, null, null);
                            jedis.srem(key(WORKERS), name);
                            jedis.del(key(WORKER, name), key(WORKER, name, STARTED), key(STAT, FAILED, name),
                                    key(STAT, PROCESSED, name));
                            jedis.quit();
                            threadRef.set(null);
                        }
                    } else {
                        if (RUNNING.equals(state.get())) {
                            throw new IllegalStateException("This WorkerImpl is already running");
                        } else {
                            throw new IllegalStateException("This WorkerImpl is shutdown");
                        }
                    }
                    return null;
                }
            });

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Shutdown this Worker.<br>
     * <b>The worker cannot be started again; create a new worker in this
     * case.</b>
     *
     * @param now if true, an effort will be made to stop any job in progress
     */
    @Override
    public void end(final boolean now) {
        if (now) {
            this.state.set(SHUTDOWN_IMMEDIATE);
            final Thread workerThread = this.threadRef.get();
            if (workerThread != null) {
                workerThread.interrupt();
            }
        } else {
            this.state.set(SHUTDOWN);
        }
        togglePause(false); // Release any threads waiting in checkPaused()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return SHUTDOWN.equals(this.state.get()) || SHUTDOWN_IMMEDIATE.equals(this.state.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPaused() {
        return this.paused.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProcessingJob() {
        return this.processingJob.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePause(final boolean paused) {
        this.paused.set(paused);
        synchronized (this.paused) {
            this.paused.notifyAll();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.listenerDelegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getQueues() {
        return Collections.unmodifiableCollection(this.queueNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(final String queueName) {
        if (queueName == null || "".equals(queueName)) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
        }
        this.queueNames.add(queueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(final String queueName, final boolean all) {
        if (queueName == null || "".equals(queueName)) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
        }
        if (all) { // Remove all instances
            boolean tryAgain = true;
            while (tryAgain) {
                tryAgain = this.queueNames.remove(queueName);
            }
        } else { // Only remove one instance
            this.queueNames.remove(queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllQueues() {
        this.queueNames.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueues(final Collection<String> queues) {

        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    checkQueues(queues);
                    queueNames.clear();
                    queueNames.addAll((queues == ALL_QUEUES) // Using object equality on purpose
                            ? jedis.smembers(key(QUEUES)) // Like '*' in other clients
                            : queues);
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobFactory getJobFactory() {
        return this.jobFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionHandler getExceptionHandler() {
        return this.exceptionHandlerRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("exceptionHandler must not be null");
        }
        this.exceptionHandlerRef.set(exceptionHandler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void join(final long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get();
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis);
        }
    }

    /**
     * @return the number of times this Worker will attempt to reconnect to
     * Redis before giving up
     */
    protected int getReconnectAttempts() {
        return RECONNECT_ATTEMPTS;
    }

    /**
     * Polls the queues for jobs and executes them.
     */
    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        while (RUNNING.equals(this.state.get())) {
            try {
                if (threadNameChangingEnabled) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                }
                curQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
                if (curQueue != null) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused();
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = pop(curQueue);
                        if (payload != null) {
                            final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
                            process(job, curQueue);
                            missCount = 0;
                        } else if (++missCount >= this.queueNames.size() && RUNNING.equals(this.state.get())) {
                            // Keeps worker from busy-spinning on empty queues
                            missCount = 0;
                            Thread.sleep(EMPTY_QUEUE_SLEEP_TIME);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                if (!isShutdown()) {
                    recoverFromException(curQueue, ie);
                }
            } catch (JsonParseException | JsonMappingException e) {
                // If the job JSON is not deserializable, we never want to submit it again...
                removeInFlight(curQueue);
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    /**
     * Remove a job from the given queue.
     *
     * @param curQueue the queue to remove a job from
     * @return a JSON string of a job or null if there was nothing to de-queue
     */
    protected String pop(final String curQueue) {
        final StringBuffer stringBuffer = new StringBuffer();
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    final String key = key(QUEUE, curQueue);
                    // If a delayed queue, peek and remove from ZSET
                    if (JedisUtils.isDelayedQueue(jedis, key)) {
                        final long now = System.currentTimeMillis();
                        // Peek ==> is there any item scheduled to run between -INF and now?
                        final Set<String> payloadSet = jedis.zrangeByScore(key, -1, now, 0, 1);
                        if (payloadSet != null && !payloadSet.isEmpty()) {
                            final String tmp = payloadSet.iterator().next();
                            // Try to acquire this job
                            if (jedis.zrem(key, tmp) == 1) {
                                stringBuffer.append(tmp);
                            }
                        }
                    } else if (JedisUtils.isRegularQueue(jedis, key)) { // If a regular queue, pop from it
                        stringBuffer.append(lpoplpush(key, key(INFLIGHT, name, curQueue)));
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return stringBuffer.toString();

    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}
     *
     * @param curQueue the name of the queue that was being processed when the
     *                 exception was thrown
     * @param e        the exception that was thrown
     */
    protected void recoverFromException(final String curQueue, final Exception e) {
        final Worker self = this;
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    final RecoveryStrategy recoveryStrategy = exceptionHandlerRef.get().onException(self, e, curQueue);
                    switch (recoveryStrategy) {
                        case RECONNECT:
                            LOG.info("Reconnecting to Redis in response to exception", e);
                            final int reconAttempts = getReconnectAttempts();
                            if (!JedisUtils.reconnect(jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
                                LOG.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", e);
                                end(false);
                            } else {
                                LOG.info("Reconnected to Redis");
                            }
                            break;
                        case TERMINATE:
                            LOG.warn("Terminating in response to exception", e);
                            end(false);
                            break;
                        case PROCEED:
                            listenerDelegate.fireEvent(WORKER_ERROR, self, curQueue, null, null, null, e);
                            break;
                        default:
                            LOG.error("Unknown RecoveryStrategy: " + recoveryStrategy
                                    + " while attempting to recover from the following exception; worker proceeding...", e);
                            break;
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    /**
     * Checks to see if worker is paused. If so, wait until unpaused.
     *
     * @throws java.io.IOException if there was an error creating the pause message
     */
    protected void checkPaused() throws IOException {
        final String pauseMessage = pauseMsg();
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    if (paused.get()) {
                        synchronized (paused) {
                            if (paused.get()) {
                                jedis.set(key(WORKER, name), pauseMessage);
                            }
                            while (paused.get()) {
                                try {
                                    paused.wait();
                                } catch (InterruptedException ie) {
                                    LOG.warn("Worker interrupted", ie);
                                }
                            }
                            jedis.del(key(WORKER, name));
                        }
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Materializes and executes the given job.
     *
     * @param job      the Job to process
     * @param curQueue the queue the payload came from
     */
    protected void process(final Job job, final String curQueue) {
        final Worker self = this;
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    try {
                        processingJob.set(true);
                        if (threadNameChangingEnabled) {
                            renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
                        }
                        listenerDelegate.fireEvent(JOB_PROCESS, self, curQueue, job, null, null, null);
                        jedis.set(key(WORKER, name), statusMsg(curQueue, job));
                        final Object instance = jobFactory.materializeJob(job);
                        final Object result = execute(job, curQueue, instance);
                        success(job, instance, result, curQueue);
                    } catch (Throwable t) {
                        failure(t, job, curQueue);
                    } finally {
                        removeInFlight(curQueue);
                        jedis.del(key(WORKER, name));
                        processingJob.set(false);
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void removeInFlight(final String curQueue) {
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    if (SHUTDOWN_IMMEDIATE.equals(state.get())) {
                        lpoplpush(key(INFLIGHT, name, curQueue), key(QUEUE, curQueue));
                    } else {
                        jedis.lpop(key(INFLIGHT, name, curQueue));
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Executes the given job.
     *
     * @param job      the job to execute
     * @param curQueue the queue the job came from
     * @param instance the materialized job
     * @return result of the execution
     * @throws Exception if the instance is a {@link java.util.concurrent.Callable} and throws an exception
     */
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this);
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null);
        final Object result;
        if (instance instanceof Callable) {
            result = ((Callable<?>) instance).call(); // The job is executing!
        } else if (instance instanceof Runnable) {
            ((Runnable) instance).run(); // The job is executing!
            result = null;
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException("Instance must be a Runnable or a Callable: " + instance.getClass().getName()
                    + " - " + instance);
        }
        return result;
    }

    /**
     * Update the status in Redis on success.
     *
     * @param job      the Job that succeeded
     * @param runner   the materialized Job
     * @param result   the result of the successful execution of the Job
     * @param curQueue the queue the Job came from
     */
    protected void success(final Job job, final Object runner, final Object result, final String curQueue) {
        final Worker self = this;
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    try {
                        jedis.incr(key(STAT, PROCESSED));
                        jedis.incr(key(STAT, PROCESSED, name));
                    } catch (JedisException je) {
                        LOG.warn("Error updating success stats for job=" + job, je);
                    }
                    listenerDelegate.fireEvent(JOB_SUCCESS, self, curQueue, job, runner, result, null);
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        // The job may have taken a long time; make an effort to ensure the
        // connection is OK
    }

    /**
     * Update the status in Redis on failure
     *
     * @param t        the Throwable that occurred
     * @param job      the Job that failed
     * @param curQueue the queue the Job came from
     */
    protected void failure(final Throwable t, final Job job, final String curQueue) {
        final Worker self = this;
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    try {
                        jedis.incr(key(STAT, FAILED));
                        jedis.incr(key(STAT, FAILED, name));
                        jedis.rpush(key(FAILED), failMsg(t, curQueue, job));
                    } catch (JedisException je) {
                        LOG.warn("Error updating failure stats for throwable=" + t + " job=" + job, je);
                    } catch (IOException ioe) {
                        LOG.warn("Error serializing failure payload for throwable=" + t + " job=" + job, ioe);
                    }
                    listenerDelegate.fireEvent(JOB_FAILURE, self, curQueue, job, null, null, t);
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Create and serialize a JobFailure.
     *
     * @param t     the Throwable that occurred
     * @param queue the queue the job came from
     * @param job   the Job that failed
     * @return the JSON representation of a new JobFailure
     * @throws java.io.IOException if there was an error serializing the JobFailure
     */
    protected String failMsg(final Throwable t, final String queue, final Job job) throws IOException {
        final JobFailure failure = new JobFailure();
        failure.setFailedAt(new Date());
        failure.setWorker(this.name);
        failure.setQueue(queue);
        failure.setPayload(job);
        failure.setThrowable(t);
        return ObjectMapperFactory.get().writeValueAsString(failure);
    }

    /**
     * Create and serialize a WorkerStatus.
     *
     * @param queue the queue the Job came from
     * @param job   the Job currently being processed
     * @return the JSON representation of a new WorkerStatus
     * @throws java.io.IOException if there was an error serializing the WorkerStatus
     */
    protected String statusMsg(final String queue, final Job job) throws IOException {
        final WorkerStatus status = new WorkerStatus();
        status.setRunAt(new Date());
        status.setQueue(queue);
        status.setPayload(job);
        return ObjectMapperFactory.get().writeValueAsString(status);
    }

    /**
     * Create and serialize a WorkerStatus for a pause event.
     *
     * @return the JSON representation of a new WorkerStatus
     * @throws java.io.IOException if there was an error serializing the WorkerStatus
     */
    protected String pauseMsg() throws IOException {
        final WorkerStatus status = new WorkerStatus();
        status.setRunAt(new Date());
        status.setPaused(isPaused());
        return ObjectMapperFactory.get().writeValueAsString(status);
    }

    /**
     * Creates a unique name, suitable for use with Resque.
     *
     * @return a unique name for this worker
     */
    protected String createName() {
        final StringBuilder buf = new StringBuilder(128);
        try {
            buf.append(InetAddress.getLocalHost().getHostName()).append(COLON)
                    .append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
                    .append('-').append(this.workerId).append(COLON).append(JAVA_DYNAMIC_QUEUES);
            for (final String queueName : this.queueNames) {
                buf.append(',').append(queueName);
            }
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
        return buf.toString();
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     *
     * @param parts the key parts to be joined
     * @return an assembled String key
     */
    protected String key(final String... parts) {
        return JesqueUtils.createKey(this.namespace, parts);
    }

    /**
     * Rename the current thread with the given message.
     *
     * @param msg the message to add to the thread name
     */
    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg);
    }

    protected String lpoplpush(final String from, final String to) {
        final StringBuffer stringBuffer = new StringBuffer();
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
                @Override
                public Void doWork(final Jedis jedis) {
                    while (JedisUtils.isRegularQueue(jedis, from)) {
                        jedis.watch(from);
                        // Get the leftmost value of the 'from' list. If it does not exist, there is nothing to pop.
                        String val = null;
                        if (JedisUtils.isRegularQueue(jedis, from)) {
                            val = jedis.lindex(from, 0);
                        }
                        if (val == null) {
                            jedis.unwatch();
                            break;
                        }
                        final Transaction tx = jedis.multi();
                        tx.lpop(from);
                        tx.lpush(to, val);
                        if (tx.exec() != null) {
                            stringBuffer.append(val);
                            break;
                        }
                        // If execution of the transaction failed, this means that 'from'
                        // was modified while we were watching it and the transaction was
                        // not executed. We simply retry the operation.
                    }
                    return null;
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if(stringBuffer.length() == 0)
            return null;
        return stringBuffer.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + COLON + WORKER + COLON + this.name;
    }


}
