/*
 * Copyright 2012 Greg Haines
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
package net.greghaines.jesque.admin;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * AdminClientImpl publishes jobs to channels.
 *
 * @author Greg Haines
 */
public class AdminClientJedisPoolImpl extends AbstractAdminClient {

    /**
     * The default behavior for checking connection validity before use.
     */
    public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;

    private final Config config;
    private final boolean checkConnectionBeforeUse;
    private final Pool<Jedis> jedisPool;

    /**
     * Create a new AdminClientImpl, which creates it's own connection to Redis
     * using values from the config. It will not verify the connection before
     * use.
     *
     * @param config used to create a connection to Redis
     */
    public AdminClientJedisPoolImpl(final Config config, final Pool<Jedis> jedisPool) throws Exception{
        this(config, jedisPool, DEFAULT_CHECK_CONNECTION_BEFORE_USE);
    }

    /**
     * Create a new AdminClientImpl, which creates it's own connection to Redis
     * using values from the config.
     *
     * @param config                   used to create a connection to Redis
     * @param checkConnectionBeforeUse check to make sure the connection is alive before using it
     * @throws IllegalArgumentException if the config is null
     */
    public AdminClientJedisPoolImpl(final Config config, final Pool<Jedis> jedisPool, final boolean checkConnectionBeforeUse) throws Exception{
        super(config);
        this.config = config;
        this.jedisPool = jedisPool;
        authenticateAndSelectDB();
        this.checkConnectionBeforeUse = checkConnectionBeforeUse;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doPublish(final String queue, final String jobJson) throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                try {
                ensureJedisConnection(jedis);
                doPublish(jedis, getNamespace(), queue, jobJson);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        try {
            PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Void doWork(final Jedis jedis) {
                    jedis.quit();
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void authenticateAndSelectDB() throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                if (config.getPassword() != null) {
                    jedis.auth(config.getPassword());
                }
                jedis.select(config.getDatabase());
                return null;
            }
        });
    }

    private void ensureJedisConnection(Jedis jedis) throws Exception {
        if (this.checkConnectionBeforeUse && !JedisUtils.ensureJedisConnection(jedis)) {
            authenticateAndSelectDB();
        }
    }
}
