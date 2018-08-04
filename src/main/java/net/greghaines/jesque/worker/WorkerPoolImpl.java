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
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static net.greghaines.jesque.utils.ResqueConstants.INFLIGHT;
import static net.greghaines.jesque.utils.ResqueConstants.WORKER;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;

/**
 * WorkerPoolImpl is an implementation of the Worker interface that uses a connection pool. Obeys the contract of a
 * Resque worker in Redis.
 */
public class WorkerPoolImpl extends AbstractWorker {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerPoolImpl.class);

    protected final Pool<Jedis> jedisPool;

    /**
     * Creates a new WorkerPoolImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * Uses the DRAIN_WHILE_MESSAGES_EXISTS NextQueueStrategy.
     *
     * @param config     used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues     the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedisPool  the Redis connection pool
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    public WorkerPoolImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
                          final Pool<Jedis> jedisPool) {
        this(config, queues, jobFactory, jedisPool, NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS);
    }

    /**
     * Creates a new WorkerPoolImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedisPool the Redis connection pool
     * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    public WorkerPoolImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
                          final Pool<Jedis> jedisPool, final NextQueueStrategy nextQueueStrategy) {
        super(config, queues, jobFactory, nextQueueStrategy);
        if (jedisPool == null) {
            throw new IllegalArgumentException("jedisPool must not be null");
        }
        this.jedisPool = jedisPool;
    }

    @Override
    protected void doRun() throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws IOException {
                doRun(jedis);
                return null;
            }
        });
    }

    @Override
    protected void doRunFinally() {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doRunFinally(jedis);
                return null;
            }
        });
    }

    @Override
    protected Set<String> getAllQueues() {
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Set<String>>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Set<String> doWork(final Jedis jedis) {
                return doGetAllQueues(jedis);
            }
        });
    }

    @Override
    protected void doSetWorkerStatus(final String msg) {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doSetWorkerStatus(jedis, name, msg);
                return null;
            }
        });
    }

    @Override
    protected void doSetWorkerToPaused() throws IOException {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws IOException {
                doSetWorkerToPaused(jedis, name, pauseMsg());
                return null;
            }
        });
    }

    @Override
    protected void doRemoveWorker() {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doRemoveWorker(jedis, name);
                return null;
            }
        });
    }

    @Override
    protected void doPollFinally(final String fCurQueue) {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doPollFinally(jedis, fCurQueue);
                return null;
            }
        });
    }

    @Override
    protected String doCallPopScript(final String key, final String workerName, final String curQueue) {
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, String>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public String doWork(final Jedis jedis) {
                return (String) jedis.evalsha(popScriptHash.get(), 3, key, key(INFLIGHT, name, curQueue),
                        JesqueUtils.createRecurringHashKey(key), Long.toString(System.currentTimeMillis()));
            }
        });
    }

<<<<<<< ours
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
=======
    @Override
    protected String doCallMultiPriorityQueuesScript(final String key, final String workerName, final String curQueue) {
>>>>>>> theirs
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, String>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public String doWork(final Jedis jedis) {
<<<<<<< ours
                switch (nextQueueStrategy) {
                    case DRAIN_WHILE_MESSAGES_EXISTS:
                        return (String) jedis.evalsha(popScriptHash.get(), 3, key, inflightKey,
                                JesqueUtils.createRecurringHashKey(key), now);
                    case RESET_TO_HIGHEST_PRIORITY:
                        return (String) jedis.evalsha(multiPriorityQueuesScriptHash.get(), 2, curQueue, inflightKey, now);
                    default:
                        throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
                }
=======
                return (String) jedis.evalsha(multiPriorityQueuesScriptHash.get(), 1, curQueue,
                        Long.toString(System.currentTimeMillis()));
>>>>>>> theirs
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    protected void recoverFromException(final String curQueue, final Exception ex) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, ex, curQueue);
        switch (recoveryStrategy) {
            case RECONNECT:
                LOG.info("Ignoring RECONNECT strategy in response to exception because this is a pool", ex);
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
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                removeInFlight(jedis, curQueue);
                jedis.del(key(WORKER, name));
                return null;
            }
        });
    }

    @Override
    protected void doSuccess() throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doSuccess(jedis, name);
                return null;
            }
<<<<<<< ours
            this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
            PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) throws IOException {
                    jedis.set(key(WORKER, name), statusMsg(curQueue, job));
                    return null;
                }
            });
            final Object instance = this.jobFactory.materializeJob(job);
            final Object result = execute(job, curQueue, instance);
            success(job, instance, result, curQueue);
        } catch (Throwable thrwbl) {
            failure(thrwbl, job, curQueue);
        } finally {
            PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) {
                    removeInFlight(jedis, curQueue);
                    jedis.del(key(WORKER, name));
                    return null;
                }
            });
            this.processingJob.set(false);
        }
    }

    private void removeInFlight(final Jedis jedis, final String curQueue) {
        if (SHUTDOWN_IMMEDIATE.equals(this.state.get())) {
            lpoplpush(jedis, key(INFLIGHT, this.name, curQueue), key(QUEUE, curQueue));
        } else {
            jedis.lpop(key(INFLIGHT, this.name, curQueue));
        }
    }

    /**
     * Executes the given job.
     * 
     * @param job the job to execute
     * @param curQueue the queue the job came from
     * @param instance the materialized job
     * @throws Exception if the instance is a {@link Callable} and throws an exception
     * @return result of the execution
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
     * @param job the Job that succeeded
     * @param runner the materialized Job
     * @param result the result of the successful execution of the Job
     * @param curQueue the queue the Job came from
     */
    protected void success(final Job job, final Object runner, final Object result, final String curQueue) {
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) {
                    jedis.incr(key(STAT, PROCESSED));
                    jedis.incr(key(STAT, PROCESSED, name));
                    return null;
                }
            });
        } catch (JedisException je) {
            LOG.warn("Error updating success stats for job=" + job, je);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.listenerDelegate.fireEvent(JOB_SUCCESS, this, curQueue, job, runner, result, null);
    }

    /**
     * Update the status in Redis on failure.
     * 
     * @param thrwbl the Throwable that occurred
     * @param job the Job that failed
     * @param curQueue the queue the Job came from
     */
    protected void failure(final Throwable thrwbl, final Job job, final String curQueue) {
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) throws IOException {
                    jedis.incr(key(STAT, FAILED));
                    jedis.incr(key(STAT, FAILED, name));
                    final FailQueueStrategy strategy = failQueueStrategyRef.get();
                    final String failQueueKey = strategy.getFailQueueKey(thrwbl, job, curQueue);
                    if (failQueueKey != null) {
                        final int failQueueMaxItems = strategy.getFailQueueMaxItems(curQueue);
                        if (failQueueMaxItems > 0) {
                            Long currentItems = jedis.llen(failQueueKey);
                            if (currentItems >= failQueueMaxItems) {
                                Transaction tx = jedis.multi();
                                tx.ltrim(failQueueKey, 1, -1);
                                tx.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                                tx.exec();
                            } else {
                                jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                            }
                        } else {
                            jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                        }
                    }
                    return null;
                }
            });
        } catch (JedisException je) {
            LOG.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je);
        } catch (IOException ioe) {
            LOG.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
=======
        });
>>>>>>> theirs
    }

    @Override
    protected void doFailure(final Throwable thrwbl, final Job job, final String curQueue) throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws IOException {
                doFailure(jedis, name, curQueue, thrwbl, job, failQueueStrategyRef.get(), failMsg(thrwbl, curQueue, job));
                return null;
            }
        });
    }

}
