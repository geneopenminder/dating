package com.highloadcup.filters;

import com.highloadcup.BitMapHolder;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;

import static com.highloadcup.pool.FilterObjectsPool.BITMAP_POOL;

public class GroupItem implements Comparable<GroupItem> {

    public String[] keys;
    public int keyOrder = 0;
    public int count;

    public int sortOrder;

    PoolEntry<BitMapHolder> set;

    public GroupItem(int sortOrder) {
        this.sortOrder = sortOrder;
        this.keys = new String[5];
        this.keyOrder = 0;
    }

    public void resetKeysOrder() {
        this.keyOrder = 0;
    }

    public GroupItem(GroupItem g, int sortOrder) {
        this.sortOrder = sortOrder;
        this.keyOrder = g.keyOrder;
        this.keys = new String[g.keys.length];
        System.arraycopy(g.keys, 0, this.keys, 0, this.keys.length);
    }

    public GroupItem(String key, int count, int sortOrder) {
        this.sortOrder = sortOrder;
        if (keys == null) {
            keys = new String[5];
        }
        keys[keyOrder++] = key;
        this.count = count;
    }


    public GroupItem(String key, PoolEntry<BitMapHolder> set, int sortOrder) {
        this.sortOrder = sortOrder;
        if (keys == null) {
            keys = new String[5];
        }
        keys[keyOrder++] = key;
        this.set = set;
    }

    public GroupItem(String key, PoolEntry<BitMapHolder> set, int count, int sortOrder) {
        this.sortOrder = sortOrder;
        this.count = count;
        if (keys == null) {
            keys = new String[5];
        }
        keys[keyOrder++] = key;
        this.set = set;
    }

    public int calculateCount() {
        if (count > 0) {
            return count;
        }
        if (set == null) {
            return count;
        } else {
            count = DirectMemoryAllocator.getBitsCounts(set.getObj().bitmap);
        }
        return count;
    }

    public int getCount() {
        if (count > 0) {
            return count;
        }
        if (set == null) {
            return count;
        } else {
            count = DirectMemoryAllocator.getBitsCounts(set.getObj().bitmap);
        }
        return count;
    }

    public void updateSet(PoolEntry<BitMapHolder> set) {
        this.set = set;
    }


    public void addKey(String key) {
        if (keys == null) {
            keys = new String[5];
        }
        this.keys[keyOrder++] = key;
    }

    public void recycleBitmap() {
        if (set != null) {
            BITMAP_POOL.free(set);
            set = null;
        }
    }
    //TODO
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (set != null) {
            FilterObjectsPool.BITMAP_POOL.free(set);
        }
    }

    @Override
    public int compareTo(GroupItem o2) {
        if (sortOrder == 1) {
            int countCompare = Integer.compare(this.count, o2.count);
            if (countCompare != 0) {
                return countCompare;
            } else {
                if (this.keys[0] != null && o2.keys[0] != null) {
                    return this.keys[0].compareTo(o2.keys[0]);
                } else if (this.keys[0] == null) {
                    return 1;
                } else {
                    return -1;
                }
            }
        } else {
            int countCompare = Integer.compare(o2.count, this.count);
            if (countCompare != 0) {
                return countCompare;
            } else {
                if (o2.keys[0] != null && this.keys[0] != null) {
                    return o2.keys[0].compareTo(this.keys[0]);
                } else if (o2.keys[0] == null) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }
    }

}