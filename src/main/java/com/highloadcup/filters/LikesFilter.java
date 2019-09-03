package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.Suggester;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TreeSet;

import static com.highloadcup.ArrayDataAllocator.*;

public class LikesFilter extends BaseFilter {

    private static final Logger logger = LogManager.getLogger(LikesFilter.class);

    static ThreadLocal<LikesFilter> lTL = ThreadLocal.withInitial(LikesFilter::new);

    private int[] accounts;

    public LikesFilter() {
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void processNew(InternalAccount a) {
        //all done by updateLikes
    }

    public static void updateLikes(int liker) {

    }

    @Override
    public void calculatePriority() {
        calculatedPriority = 0;
        for (int id: accounts) {
            if (Suggester.likesForUser[id] != null) {
                calculatedPriority += Suggester.likesForUser[id].ids.length;
            }
        }
        //calculatedPriority = (int)Arrays.stream(accounts).flatMap(a -> Arrays.stream(likesForUser[a].array)).count();
    }

    static ThreadLocal<long[]> bitmapForNewSetThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<long[]> bitmapForIntersectThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<long[]> bitmapForIntersect2ThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<TreeSet<Integer>> retainSetTL = new ThreadLocal<TreeSet<Integer>>() {
        @Override
        protected TreeSet<Integer> initialValue() {
            return new TreeSet<Integer>();
        }
    };

    static ThreadLocal<TreeSet<Integer>> retainTMPSetTL = new ThreadLocal<TreeSet<Integer>>() {
        @Override
        protected TreeSet<Integer> initialValue() {
            return new TreeSet<Integer>();
        }
    };


    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        TreeSet<Integer> finalSet = null;

        if (accounts == null || accounts.length == 0) {
            return null;
        }

        if (accounts.length == 1) {

            int[] aaa = Suggester.likesForUser[accounts[0]].ids;

            if (aaa == null) {
                return null;
            }

            int[] a = new int[aaa.length];
            for (int i = 0; i < aaa.length; i++) {
                int likeAccId = aaa[i] & 0x00FFFFFF;
                a[i] = likeAccId;
            }

            long[] finalMap = bitmapForNewSetThreadLocal.get();
            DirectMemoryAllocator.clear(finalMap);
            for (int j = 0; j < a.length; j++) {
                int likelyUser = a[j];
                DirectMemoryAllocator.setBit(finalMap, likelyUser);
            }

            if (currentSet == null) {
                return finalMap;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, finalMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, finalMap);
                }
                return currentSet;
            }

        }

        for (int s: accounts) {
            if (finalSet != null && finalSet.isEmpty()) {
                return null;
            }

            if (Suggester.likesForUser[s] == null) {
                continue;
            }

            int[] aaa = Suggester.likesForUser[s].ids;

            int[] a = new int[aaa.length];
            for (int i = 0; i < aaa.length; i++) {
                int likeAccId = aaa[i] & 0x00FFFFFF;
                a[i] = likeAccId;
            }

            TreeSet<Integer> tmpSet = finalSet == null ? retainSetTL.get() : retainTMPSetTL.get();
            tmpSet.clear();

            for (int j = 0; j < a.length; j++) {
                int likelyUser = a[j];
                tmpSet.add(likelyUser);
            }

            if (finalSet == null) {
                finalSet = tmpSet;
            } else {
                finalSet.retainAll(tmpSet);
            }

        }

        if (finalSet.isEmpty()) {
            return null;
        }


        long[] finalMap = bitmapForNewSetThreadLocal.get();
        DirectMemoryAllocator.clear(finalMap);

        finalSet.forEach( i -> {
            DirectMemoryAllocator.setBit(finalMap, i);
        });

        if (currentSet == null) {
            return finalMap;
        } else {
            if (isLast) {
                DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, finalMap, limit);
            } else {
                DirectMemoryAllocator.intersectArrays(currentSet, finalMap);
            }
        }

        return currentSet;
    }

    @Override
    public void intersect(long[] bitSet) {
        int[] aaa = Suggester.likesForUser[Integer.parseInt(value)].ids;
        this.accounts = new int[] {Integer.parseInt(value)};
        DirectMemoryAllocator.intersectArrays(bitSet, aaa);
    }

    @Override
    public LikesFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        LikesFilter filter = lTL.get();
        filter.fill(predicate, value, limit);
        String[] lVals = value[0].split(",");
        if (lVals.length == 0) {
            filter.accounts = new int[0];
        } else {
            int[] newAcc = new int[lVals.length];
            for (int i = 0; i < lVals.length; i++) {
                newAcc[i] = (Integer.parseInt(lVals[i]));
            }
            filter.accounts = newAcc;
        }
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()
                || value[0].split(",").length == 0
                || !value[0].replaceAll(",", "").chars().allMatch( Character::isDigit)) {
            return false;
        } else if (value[0].split(",").length == 0) {
            return false;
        }
        if (predicat.equalsIgnoreCase("likes_contains") || predicat.equalsIgnoreCase("likes")) {
            return true;
        } else {
            return false;
        }
    }
}
