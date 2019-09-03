package com.highloadcup.pool;

import com.highloadcup.App;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.yield;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

//-Xmx2000m -server -XX:+AggressiveOpts -XX:+UseFastAccessorMethods  -XX:-RestrictContended
public final class RelictumPool<T> {

    private static final Logger logger = LogManager.getLogger(RelictumPool.class);

    public static final int BUSY = 0x00000001;
    public static final int FREE = 0x00000000;

    ThreadLocal<PoolEntry> lastUsed = new ThreadLocal<PoolEntry>() {

        @Override
        protected PoolEntry initialValue() {
            return null;
        }

    };

    int poolSize;

    private PoolEntry[] entries;

    //private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");

    private final SynchronousQueue<PoolEntry> queue = new SynchronousQueue<PoolEntry>(true);

    public static interface PoolObjectFactory<T> {
        public T getNew();
    };

    /*static class PoolEntry<T> extends UniversalPadding64 { 

        private final T obj;

        @sun.misc.Contended
        private volatile int state;

        public PoolEntry(T obj) {
            this.obj = obj;
        }

        public boolean cas(int expect, int update) {
            return stateUpdater.compareAndSet(this, expect, update);
        }

        public void setState(int update) {
            stateUpdater.set(this, update);
        }

        public int getState() {
            return stateUpdater.get(this);
        }

    };*/

    private RelictumPool(int poolSize) {
        this.poolSize = poolSize;
        this.entries = new PoolEntry[poolSize];
    }

    public static RelictumPool getFixed(int size, PoolObjectFactory factory) {
        RelictumPool pool = new RelictumPool(size);
        for (int i = 0; i < size; i++) {
            pool.add(new PoolEntry(factory.getNew()), i);
        }
        return pool;
    }

    private void add(PoolEntry entry, int index) {
        entries[index] = entry;
    }

    //****************************

    public static AtomicLong totalUsed = new AtomicLong(0);


    public PoolEntry<T> take(long timeout, final TimeUnit timeUnit) {
        long used = totalUsed.incrementAndGet();

        if (used > 2000) {
            //logger.error("pool total take - {}, thread - {}", totalUsed.incrementAndGet(), Thread.currentThread().getId());
        }
        PoolEntry last = lastUsed.get();
        if (last != null) {
            if (last.cas(FREE, BUSY)) {
                return last;
            } else {
                lastUsed.set(null);
            }
        }

        for (PoolEntry obj : entries) {
            if (obj.cas(FREE, BUSY)) {
                return obj;
            }
        }

        timeout = timeUnit.toNanos(timeout);

        try {
            do {
                final long start = System.nanoTime();
                final PoolEntry bagEntry = queue.poll(timeout, NANOSECONDS);
                if (bagEntry == null) {
                    throw new BorrowTimeoutException();
                }
                if (bagEntry.cas(FREE, BUSY)) {
                    return bagEntry;
                }
                timeout -= (start - System.nanoTime());
            } while (timeout > 10_000); //TODO
        } catch (InterruptedException ignored) {
            Thread.currentThread().isInterrupted(); //TODO
        }
        throw new BorrowTimeoutException();
    }

    public void free(PoolEntry poolEntry) {
        totalUsed.decrementAndGet();
        //logger.error("pool total free - {}, thread - {}", totalUsed.decrementAndGet(), Thread.currentThread().getId());
        lastUsed.set(poolEntry);
        poolEntry.setState(FREE);
        if (queue.offer(poolEntry)) {
        //if (poolEntry.getState() == FREE && queue.offer(poolEntry)) { //partially fair
            yield();
        }
    }

}
