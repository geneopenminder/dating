package com.highloadcup.pool;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class PoolEntry<T> extends UniversalPadding64 {

    private final T obj;

    private volatile int state;

    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");

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

    public T getObj() {
        return obj;
    }
};