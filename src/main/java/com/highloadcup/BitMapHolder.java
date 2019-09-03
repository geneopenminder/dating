package com.highloadcup;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;

public class BitMapHolder {

    public long[] bitmap;

    public BitMapHolder(long[] bitmap) {
        this.bitmap = bitmap;
    }

    public static BitMapHolder create() {
        long[] arr = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        return new BitMapHolder(arr);
    }
}
