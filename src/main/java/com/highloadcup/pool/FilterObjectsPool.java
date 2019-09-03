package com.highloadcup.pool;

import com.highloadcup.BitMapHolder;

public class FilterObjectsPool {

    public static final RelictumPool BITMAP_POOL = RelictumPool.getFixed(5, new RelictumPool.PoolObjectFactory() {
        public BitMapHolder getNew() {
            return BitMapHolder.create();
        }
    });


}
