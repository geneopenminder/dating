package com.highloadcup.pool;

import com.highloadcup.BitMapHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.MultithreadConcurrentQueueCollection;

public class BitmapsPool {

    private static final Logger logger = LogManager.getLogger(BitmapsPool.class);

    static final long[] zeroArray = new long[2_000_000 / 64 + 1];

    static PoolService<BitMapHolder> pool;

    static {
        try {
            pool = new ConcurrentPool<>(
                    new MultithreadConcurrentQueueCollection<>(100), new BitMapHolderFactory(), 300, 300, false);
        } catch (Exception e) {
            System.exit(3);
        }
    }

    public static BitMapHolder getBitMap() {
        logger.error("");
        return pool.take();
    }

    public static void returnBitmap(BitMapHolder holder) {
        System.arraycopy(zeroArray, 0, holder.bitmap, 0, holder.bitmap.length);
        pool.restore(holder);
    }

    public static class BitMapHolderFactory implements PoolObjectFactory<BitMapHolder> {

        @Override
        public BitMapHolder create() {
            return BitMapHolder.create();
        }

        @Override
        public boolean readyToTake(BitMapHolder obj) {
            return false;
        }

        @Override
        public boolean readyToRestore(BitMapHolder obj) {
            return false;
        }

        @Override
        public void destroy(BitMapHolder obj) {

        }
    }

}
