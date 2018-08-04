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

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;

/**
 * WorkerImpl is an implementation of the Worker interface. Obeys the contract of a Resque worker in Redis.
 */
public class WorkerImpl extends AbstractWorker {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerImpl.class);
    protected static final long RECONNECT_SLEEP_TIME = 5000; // 5 sec
    protected static final int RECONNECT_ATTEMPTS = 120; // Total time: 10 min

    protected final Jedis jedis;

    /**
     * Creates a new WorkerImpl, which creates it's own connection to Redis using values from the config.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @throws IllegalArgumentException if either config, queues or jobFactory is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory) {
        this(config, queues, jobFactory, new Jedis(config.getHost(), config.getPort(), config.getTimeout()));
    }

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * Uses the DRAIN_WHILE_MESSAGES_EXISTS NextQueueStrategy.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedis the connection to Redis
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
            final Jedis jedis) {
        this(config, queues, jobFactory, jedis, NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS);
    }

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedis the connection to Redis
     * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
     * @throws IllegalArgumentException if either config, queues, jobFactory, jedis or nextQueueStrategy is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
            final Jedis jedis, final NextQueueStrategy nextQueueStrategy) {
        super(config, queues, jobFactory, nextQueueStrategy);
        if (jedis == null) {
            throw new IllegalArgumentException("jedis must not be null");
        }
        this.jedis = jedis;
        authenticateAndSelectDB();
        setQueues(queues);
<<<<<<< ours
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
                this.jedis.sadd(key(WORKERS), this.name);
                this.jedis.set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
                this.listenerDelegate.fireEvent(WORKER_START, this, null, null, null, null, null);
                loadRedisScripts();
                poll();
            } catch (Exception ex) {
                LOG.error("Uncaught exception in worker run-loop!", ex);
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, ex);
            } finally {
                renameThread("STOPPING");
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                this.jedis.srem(key(WORKERS), this.name);
                this.jedis.del(key(WORKER, this.name), key(WORKER, this.name, STARTED), key(STAT, FAILED, this.name),
                        key(STAT, PROCESSED, this.name));
                this.jedis.quit();
                this.threadRef.set(null);
            }
        } else if (RUNNING.equals(this.state.get())) {
            throw new IllegalStateException("This WorkerImpl is already running");
        } else {
            throw new IllegalStateException("This WorkerImpl is shutdown");
        }
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
=======
>>>>>>> theirs
    }

    @Override
    protected void doRun() throws Exception {
        doRun(this.jedis);
    }

    @Override
    protected void doRunFinally() {
        doRunFinally(this.jedis);
        this.jedis.quit();
    }

    @Override
    protected Set<String> getAllQueues() {
        return doGetAllQueues(this.jedis);
    }

    @Override
    protected void doSetWorkerStatus(final String msg) {
        doSetWorkerStatus(this.jedis, this.name, msg);
    }

    @Override
    protected void doSetWorkerToPaused() throws IOException {
        doSetWorkerToPaused(this.jedis, this.name, pauseMsg());
    }

    @Override
    protected void doRemoveWorker() {
        doRemoveWorker(this.jedis, this.name);
    }

    @Override
    protected void doPollFinally(final String fCurQueue) {
        doPollFinally(this.jedis, fCurQueue);
    }

    @Override
    protected String doCallPopScript(final String key, final String workerName, final String curQueue) {
        return (String) this.jedis.evalsha(this.popScriptHash.get(), 3, key, key(INFLIGHT, this.name, curQueue),
                JesqueUtils.createRecurringHashKey(key), Long.toString(System.currentTimeMillis()));
    }

    @Override
    protected String doCallMultiPriorityQueuesScript(final String key, final String workerName, final String curQueue) {
        return (String) this.jedis.evalsha(this.multiPriorityQueuesScriptHash.get(), 1, curQueue,
                Long.toString(System.currentTimeMillis()));
    }

    /**
     * @return the number of times this Worker will attempt to reconnect to Redis before giving up
     */
    protected int getReconnectAttempts() {
        return RECONNECT_ATTEMPTS;
    }

    /**
<<<<<<< ours
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
                removeInFlight(curQueue);
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    private boolean shouldSleep(final int missCount) {
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
     * Remove a job from the given queue.
     * 
     * @param curQueue the queue to remove a job from
     * @return a JSON string of a job or null if there was nothing to de-queue
     */
    protected String pop(final String curQueue) {
        final String key = key(QUEUE, curQueue);
        final String now = Long.toString(System.currentTimeMillis());
        final String inflightKey = key(INFLIGHT, this.name, curQueue);
        switch (nextQueueStrategy) {
            case DRAIN_WHILE_MESSAGES_EXISTS:
                return (String) this.jedis.evalsha(this.popScriptHash.get(), 3, key, inflightKey,
                        JesqueUtils.createRecurringHashKey(key), now);
            case RESET_TO_HIGHEST_PRIORITY:
                return (String) this.jedis.evalsha(this.multiPriorityQueuesScriptHash.get(), 2, curQueue, inflightKey, now);
            default:
                throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
        }
    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}.
     * 
     * @param curQueue the name of the queue that was being processed when the exception was thrown
     * @param ex the exception that was thrown
=======
     * {@inheritDoc}
>>>>>>> theirs
     */
    protected void recoverFromException(final String curQueue, final Exception ex) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, ex, curQueue);
        switch (recoveryStrategy) {
        case RECONNECT:
            LOG.info("Reconnecting to Redis in response to exception", ex);
            final int reconAttempts = getReconnectAttempts();
            if (!JedisUtils.reconnect(this.jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
                LOG.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", ex);
                end(false);
            } else {
                authenticateAndSelectDB();
                LOG.info("Reconnected to Redis");
                try {
                    loadRedisScripts();
                } catch (IOException e) {
                    LOG.error("Failed to reload Lua scripts after reconnect", e);
                }
            }
            break;
        case TERMINATE:
            LOG.warn("Terminating in response to exception", ex);
            end(false);
            break;
        case PROCEED:
            this.listenerDelegate.fireEvent(WORKER_ERROR, this, curQueue, null, null, null, ex);
            break;
        default:
            LOG.error("Unknown RecoveryStrategy: " + recoveryStrategy
                    + " while attempting to recover from the following exception; worker proceeding...", ex);
            break;
        }
    }

    @Override
    protected void doProcessFinally(final String curQueue) {
        removeInFlight(this.jedis, curQueue);
        this.jedis.del(key(WORKER, this.name));
    }

    private void authenticateAndSelectDB() {
        if (this.config.getPassword() != null) {
            this.jedis.auth(this.config.getPassword());
        }
        this.jedis.select(this.config.getDatabase());
    }

    @Override
    protected void doSuccess() {
        // The job may have taken a long time; make an effort to ensure the
        // connection is OK
        JedisUtils.ensureJedisConnection(this.jedis);
        this.jedis.incr(key(STAT, PROCESSED));
        this.jedis.incr(key(STAT, PROCESSED, this.name));
    }

    @Override
    protected void doFailure(final Throwable thrwbl, final Job job, final String curQueue) throws Exception {
        JedisUtils.ensureJedisConnection(this.jedis);
<<<<<<< ours
        try {
            this.jedis.incr(key(STAT, FAILED));
            this.jedis.incr(key(STAT, FAILED, this.name));
            final FailQueueStrategy strategy = this.failQueueStrategyRef.get();
            final String failQueueKey = strategy.getFailQueueKey(thrwbl, job, curQueue);
            if (failQueueKey != null) {
                final int failQueueMaxItems = strategy.getFailQueueMaxItems(curQueue);
                if (failQueueMaxItems > 0) {
                    Long currentItems = this.jedis.llen(failQueueKey);
                    if (currentItems >= failQueueMaxItems) {
                        Transaction tx = this.jedis.multi();
                        tx.ltrim(failQueueKey, 1, -1);
                        tx.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                        tx.exec();
                    } else {
                        this.jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                    }
                } else {
                    this.jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                }
            }
        } catch (JedisException je) {
            LOG.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je);
        } catch (IOException ioe) {
            LOG.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe);
        }
        this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, thrwbl);
    }

    /**
     * Create and serialize a JobFailure.
     * 
     * @param thrwbl the Throwable that occurred
     * @param queue the queue the job came from
     * @param job the Job that failed
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
     * @param job the Job currently being processed
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

    /**
     * Rename the current thread with the given message.
     * 
     * @param msg the message to add to the thread name
     */
    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg);
    }

    protected String lpoplpush(final String from, final String to) {
        return (String) this.jedis.evalsha(this.lpoplpushScriptHash.get(), 2, from, to);
    }

    private void loadRedisScripts() throws IOException {
        this.popScriptHash.set(this.jedis.scriptLoad(ScriptUtils.readScript(POP_LUA)));
        this.lpoplpushScriptHash.set(this.jedis.scriptLoad(ScriptUtils.readScript(LPOPLPUSH_LUA)));
        this.multiPriorityQueuesScriptHash
                .set(this.jedis.scriptLoad(ScriptUtils.readScript(POP_FROM_MULTIPLE_PRIO_QUEUES)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + COLON + WORKER + COLON + this.name;
    }
=======
        doFailure(this.jedis, this.name, curQueue, thrwbl, job, this.failQueueStrategyRef.get(), failMsg(thrwbl, curQueue, job));
    }

>>>>>>> theirs
}
