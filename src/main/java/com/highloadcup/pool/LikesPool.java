package com.highloadcup.pool;

import com.highloadcup.model.Account;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.MultithreadConcurrentQueueCollection;

import java.util.concurrent.atomic.AtomicInteger;

public class LikesPool {

    private static final Logger logger = LogManager.getLogger(LikesPool.class);

    static PoolService<Account.Like> pool;

    public static AtomicInteger count;

    static {
        count = new AtomicInteger(0);
        try {
            pool = new ConcurrentPool<>(
                    new MultithreadConcurrentQueueCollection<>(100), new LikeFactory(), 100, 100, false);
        } catch (Exception e) {
            logger.error("pool error", e);
            System.exit(3);
        }
    }


    public static Account.Like getLike() {
        count.incrementAndGet();
        //logger.error("get Like - {}", count.incrementAndGet());
        return new Account.Like();
        //return pool.take();
    }

    public static void returnLike(Account.Like like) {
        count.decrementAndGet();
        //logger.error("return Like - {}", count.decrementAndGet());
        //like.id = 0;
        //like.ts = 0;
        //pool.restore(like);
    }

    public static class LikeFactory implements PoolObjectFactory<Account.Like> {

        @Override
        public Account.Like create() {
            //count.incrementAndGet();
            //logger.error("create like - {}", count.incrementAndGet());
            return new Account.Like();
        }

        @Override
        public boolean readyToTake(Account.Like obj) {
            return true;
        }

        @Override
        public boolean readyToRestore(Account.Like obj) {
            return true;
        }

        @Override
        public void destroy(Account.Like obj) {

        }
    }


}
