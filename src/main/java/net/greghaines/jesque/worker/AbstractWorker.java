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
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import net.greghaines.jesque.utils.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

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
 * Common logic for Worker implementations.
 */
public abstract class AbstractWorker implements Worker {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractWorker.class);
    protected static final AtomicLong WORKER_COUNTER = new AtomicLong(0);
    protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms
    protected static final String LPOPLPUSH_LUA = "/workerScripts/jesque_lpoplpush.lua";
    protected static final String POP_LUA = "/workerScripts/jesque_pop.lua";
    protected static final String POP_FROM_MULTIPLE_PRIO_QUEUES = "/workerScripts/fromMultiplePriorityQueues.lua";

    // Set the thread name to the message for debugging
    protected static volatile boolean threadNameChangingEnabled = false;

    /**
     * @return true if worker threads names will change during normal operation
     */
    public static boolean isThreadNameChangingEnabled() {
        return threadNameChangingEnabled;
    }

    /**
     * Enable/disable worker thread renaming during normal operation. (Disabled by default)
     * <p>
     * <strong>Warning: Enabling this feature is very expensive CPU-wise!</strong><br>
     * This feature is designed to assist in debugging worker state but should be disabled in production environments
     * for performance reasons.
     * </p>
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
    protected final String namespace;
    protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>();
    protected final String name;
    protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    protected final AtomicBoolean paused = new AtomicBoolean(false);
    protected final AtomicBoolean processingJob = new AtomicBoolean(false);
    protected final AtomicReference<String> popScriptHash = new AtomicReference<>(null);
    protected final AtomicReference<String> lpoplpushScriptHash = new AtomicReference<>(null);
    protected final AtomicReference<String> multiPriorityQueuesScriptHash = new AtomicReference<>(null);
    protected final long workerId = WORKER_COUNTER.getAndIncrement();
    protected final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
    protected final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    protected final AtomicReference<ExceptionHandler> exceptionHandlerRef = new AtomicReference<ExceptionHandler>(
            new DefaultExceptionHandler());
    protected final AtomicReference<FailQueueStrategy> failQueueStrategyRef;
    protected final JobFactory jobFactory;
    protected final NextQueueStrategy nextQueueStrategy;

    /**
     * Creates a new Worker.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     *
     * @param config            used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues            the list of queues to poll
     * @param jobFactory        the job factory that materializes the jobs
     * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    protected AbstractWorker(final Config config, final Collection<String> queues, final JobFactory jobFactory,
                             final NextQueueStrategy nextQueueStrategy) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (jobFactory == null) {
            throw new IllegalArgumentException("jobFactory must not be null");
        }
        if (nextQueueStrategy == null) {
            throw new IllegalArgumentException("nextQueueStrategy must not be null");
        }
        checkQueues(queues);
        this.nextQueueStrategy = nextQueueStrategy;
        this.config = config;
        this.jobFactory = jobFactory;
        this.namespace = config.getNamespace();
        this.failQueueStrategyRef = new AtomicReference<FailQueueStrategy>(
                new DefaultFailQueueStrategy(this.namespace));
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
     * Starts this worker. Registers the worker in Redis and begins polling the queues for jobs.<br>
     * Stop this worker by calling end() on any thread.
     */
    @Override
    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                renameThread("RUNNING");
                this.threadRef.set(Thread.currentThread());
                listenerDelegate.fireEvent(WORKER_START, AbstractWorker.this, null, null, null, null, null);
                doRun();
                poll();
            } catch (Exception ex) {
                LOG.error("Uncaught exception in worker run-loop!", ex);
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, ex);
            } finally {
                renameThread("STOPPING");
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                doRunFinally();
                this.threadRef.set(null);
            }
        } else if (RUNNING.equals(this.state.get())) {
            throw new IllegalStateException("This WorkerImpl is already running");
        } else {
            throw new IllegalStateException("This WorkerImpl is shutdown");
        }
    }

    protected abstract void doRun() throws Exception;

    protected void doRun(final Jedis jedis) throws IOException {
        popScriptHash.set(jedis.scriptLoad(ScriptUtils.readScript(POP_LUA)));
        lpoplpushScriptHash.set(jedis.scriptLoad(ScriptUtils.readScript(LPOPLPUSH_LUA)));
        multiPriorityQueuesScriptHash.set(jedis.scriptLoad(ScriptUtils.readScript(POP_FROM_MULTIPLE_PRIO_QUEUES)));
        jedis.sadd(key(WORKERS), this.name);
        jedis.set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
    }

    protected abstract void doRunFinally();

    protected void doRunFinally(final Jedis jedis) {
        jedis.srem(key(WORKERS), this.name);
        jedis.del(key(WORKER, this.name), key(WORKER, this.name, STARTED), key(STAT, FAILED, this.name),
                key(STAT, PROCESSED, this.name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueues(final Collection<String> queues) {
        checkQueues(queues);
        this.queueNames.clear();
        if (queues == ALL_QUEUES) { // Using object equality on purpose
            // Like '*' in other clients
            this.queueNames.addAll(getAllQueues());
        } else {
            this.queueNames.addAll(queues);
        }
    }

    protected abstract Set<String> getAllQueues();

    protected Set<String> doGetAllQueues(final Jedis jedis) {
        return jedis.smembers(key(QUEUES));
    }

    /**
     * Shutdown this Worker.<br>
     * <b>The worker cannot be started again; create a new worker in this case.</b>
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
     * @return the current FailQueueStrategy
     */
    public FailQueueStrategy getFailQueueStrategy() {
        return this.failQueueStrategyRef.get();
    }

    /**
     * @param failQueueStrategy the new FailQueueStrategy to use
     */
    public void setFailQueueStrategy(final FailQueueStrategy failQueueStrategy) {
        if (failQueueStrategy == null) {
            throw new IllegalArgumentException("failQueueStrategy must not be null");
        }
        this.failQueueStrategyRef.set(failQueueStrategy);
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
                curQueue = getNextQueue();
                if (curQueue != null) {
                    checkPaused();
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = pop(curQueue);
                        if (payload != null) {
                            process(ObjectMapperFactory.get().readValue(payload, Job.class), curQueue);
                            missCount = 0;
                        } else {
                            missCount++;
                            if (shouldSleep(missCount) && RUNNING.equals(this.state.get())) {
                                // Keeps worker from busy-spinning on empty queues
                                missCount = 0;
                                Thread.sleep(EMPTY_QUEUE_SLEEP_TIME);
                            }
                        }
                    }
                }
            } catch (InterruptedException ie) {
                if (!isShutdown()) {
                    recoverFromException(curQueue, ie);
                }
            } catch (JsonParseException | JsonMappingException e) {
                // If the job JSON is not deserializable, we never want to submit it again...
                final String fCurQueue = curQueue;
                doPollFinally(fCurQueue);
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    protected boolean shouldSleep(final int missCount) {
        return (NextQueueStrategy.RESET_TO_HIGHEST_PRIORITY.equals(this.nextQueueStrategy)
                || (missCount >= this.queueNames.size()));
    }

    protected String getNextQueue() throws InterruptedException {
        final String nextQueue;
        switch (this.nextQueueStrategy) {
            case DRAIN_WHILE_MESSAGES_EXISTS:
                final String nextPollQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
                if (nextPollQueue != null) {
                    // Rotate the queues
                    this.queueNames.add(nextPollQueue);
                }
                nextQueue = nextPollQueue;
                break;
            case RESET_TO_HIGHEST_PRIORITY:
                nextQueue = JesqueUtils.join(",", this.queueNames);
                break;
            default:
                throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
        }
        return nextQueue;
    }

    /**
     * Checks to see if worker is paused. If so, wait until unpaused.
     *
     * @throws IOException if there was an error creating the pause message
     */
    protected void checkPaused() throws IOException {
        if (this.paused.get()) {
            synchronized (this.paused) {
                if (this.paused.get()) {
                    doSetWorkerToPaused();
                }
                while (this.paused.get()) {
                    try {
                        this.paused.wait();
                    } catch (InterruptedException ie) {
                        LOG.warn("Worker interrupted", ie);
                    }
                }
                doRemoveWorker();
            }
        }
    }

    protected abstract void doSetWorkerStatus(final String msg);

    protected static void doSetWorkerStatus(final Jedis jedis, final String workerName, String msg) {
        jedis.set(namespacedKey(WORKER, workerName), msg);
    }

    protected abstract void doSetWorkerToPaused() throws IOException;

    protected static void doSetWorkerToPaused(final Jedis jedis, final String workerName, final String pauseMsg) {
        doSetWorkerStatus(jedis, workerName, pauseMsg);
    }

    protected abstract void doRemoveWorker();

    protected static void doRemoveWorker(final Jedis jedis, final String workerName) {
        jedis.del(namespacedKey(WORKER, workerName));
    }

    protected abstract void doPollFinally(final String fCurQueue);

    protected void doPollFinally(final Jedis jedis, final String fCurQueue) {
        removeInFlight(jedis, fCurQueue);
    }

    protected abstract String doCallPopScript(final String key, final String workerName, final String curQueue);

    protected abstract String doCallMultiPriorityQueuesScript(final String key, final String workerName, final String curQueue);

    /**
     * Remove a job from the given queue.
     *
     * @param curQueue the queue to remove a job from
     * @return a JSON string of a job or null if there was nothing to de-queue
     */
    protected String pop(final String curQueue) {
        final String key = key(QUEUE, curQueue);
        switch (nextQueueStrategy) {
            case DRAIN_WHILE_MESSAGES_EXISTS:
                return doCallPopScript(key, this.name, curQueue);
            case RESET_TO_HIGHEST_PRIORITY:
                return doCallMultiPriorityQueuesScript(key, this.name, curQueue);
            default:
                throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
        }
    }

    protected void removeInFlight(final Jedis jedis, final String curQueue) {
        if (SHUTDOWN_IMMEDIATE.equals(this.state.get())) {
            lpoplpush(jedis, key(INFLIGHT, this.name, curQueue), key(QUEUE, curQueue));
        } else {
            jedis.lpop(key(INFLIGHT, this.name, curQueue));
        }
    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}.
     *
     * @param curQueue the name of the queue that was being processed when the exception was thrown
     * @param ex       the exception that was thrown
     */
    protected abstract void recoverFromException(final String curQueue, final Exception ex);

    /**
     * Materializes and executes the given job.
     *
     * @param job      the Job to process
     * @param curQueue the queue the payload came from
     */
    protected void process(final Job job, final String curQueue) {
        try {
            this.processingJob.set(true);
            if (threadNameChangingEnabled) {
                renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
            }
            this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
            doSetWorkerStatus(statusMsg(curQueue, job));
            final Object instance = this.jobFactory.materializeJob(job);
            final Object result = execute(job, curQueue, instance);
            success(job, instance, result, curQueue);
        } catch (Throwable thrwbl) {
            failure(thrwbl, job, curQueue);
        } finally {
            doProcessFinally(curQueue);
            this.processingJob.set(false);
        }
    }

    protected abstract void doProcessFinally(final String curQueue);

    /**
     * Executes the given job.
     *
     * @param job      the job to execute
     * @param curQueue the queue the job came from
     * @param instance the materialized job
     * @return result of the execution
     * @throws Exception if the instance is a {@link Callable} and throws an exception
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
            throw new ClassCastException(
                    "Instance must be a Runnable or a Callable: " + instance.getClass().getName() + " - " + instance);
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
        try {
            doSuccess();
        } catch (JedisException je) {
            LOG.warn("Error updating success stats for job=" + job, je);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.listenerDelegate.fireEvent(JOB_SUCCESS, this, curQueue, job, runner, result, null);
    }

    protected abstract void doSuccess() throws Exception;

    protected static void doSuccess(Jedis jedis, String workerName) {
        jedis.incr(namespacedKey(STAT, PROCESSED));
        jedis.incr(namespacedKey(STAT, PROCESSED, workerName));
    }

    /**
     * Update the status in Redis on failure.
     *
     * @param thrwbl   the Throwable that occurred
     * @param job      the Job that failed
     * @param curQueue the queue the Job came from
     */
    protected void failure(final Throwable thrwbl, final Job job, final String curQueue) {
        try {
            doFailure(thrwbl, job, curQueue);
        } catch (JedisException je) {
            LOG.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je);
        } catch (IOException ioe) {
            LOG.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, thrwbl);
    }

    protected abstract void doFailure(final Throwable thrwbl, final Job job, final String curQueue) throws Exception;

    protected static void doFailure(final Jedis jedis, final String workerName, final String curQueue, final Throwable thrwbl,
                                    final Job job, final FailQueueStrategy strategy, final String msg) {
        jedis.incr(namespacedKey(STAT, FAILED));
        jedis.incr(namespacedKey(STAT, FAILED, workerName));
        final String failQueueKey = strategy.getFailQueueKey(thrwbl, job, curQueue);
        if (failQueueKey != null) {
            final int failQueueMaxItems = strategy.getFailQueueMaxItems(curQueue);
            if (failQueueMaxItems > 0) {
                handleFailQueueMaxItems(jedis, failQueueKey, failQueueMaxItems, msg);
            } else {
                jedis.rpush(failQueueKey, msg);
            }
        }
    }

    protected static void handleFailQueueMaxItems(final Jedis jedis, final String failQueueKey, final int failQueueMaxItems, final String msg) {
        Long currentItems = jedis.llen(failQueueKey);
        if (currentItems >= failQueueMaxItems) {
            Transaction tx = jedis.multi();
            tx.ltrim(failQueueKey, 1, -1);
            tx.rpush(failQueueKey, msg);
            tx.exec();
        }
    }

    /**
     * Create and serialize a JobFailure.
     *
     * @param thrwbl the Throwable that occurred
     * @param queue  the queue the job came from
     * @param job    the Job that failed
     * @return the JSON representation of a new JobFailure
     * @throws IOException if there was an error serializing the JobFailure
     */
    protected String failMsg(final Throwable thrwbl, final String queue, final Job job) throws IOException {
        final JobFailure failure = new JobFailure();
        failure.setFailedAt(new Date());
        failure.setWorker(this.name);
        failure.setQueue(queue);
        failure.setPayload(job);
        failure.setThrowable(thrwbl);
        return ObjectMapperFactory.get().writeValueAsString(failure);
    }

    /**
     * Create and serialize a WorkerStatus.
     *
     * @param queue the queue the Job came from
     * @param job   the Job currently being processed
     * @return the JSON representation of a new WorkerStatus
     * @throws IOException if there was an error serializing the WorkerStatus
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
     * @throws IOException if there was an error serializing the WorkerStatus
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

    protected static String namespacedKey(final String namespace, final String... parts) {
        return JesqueUtils.createKey(namespace, parts);
    }

    /**
     * Rename the current thread with the given message.
     *
     * @param msg the message to add to the thread name
     */
    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg);
    }

    protected String lpoplpush(final Jedis jedis, final String from, final String to) {
        return (String) jedis.evalsha(this.lpoplpushScriptHash.get(), 2, from, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + COLON + WORKER + COLON + this.name;
    }
}
