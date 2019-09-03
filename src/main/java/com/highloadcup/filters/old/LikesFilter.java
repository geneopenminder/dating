package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TreeSet;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.likesGb;
import static com.highloadcup.DirectMemoryAllocator.TripleIntArray;

@Deprecated
public class LikesFilter extends BaseFilter {

    private static final Logger logger = LogManager.getLogger(LikesFilter.class);

    static ThreadLocal<LikesFilter> lTL = ThreadLocal.withInitial(LikesFilter::new);

    private int[] accounts;

    public static TripleIntArray[] likesForUser = null;
    public static TripleIntArray[] likesByUser = null;

    public LikesFilter() {
    }

    @Override
    public void init() {
        super.init();
        likesForUser = new TripleIntArray[MAX_ACCOUNTS];
        likesByUser = new TripleIntArray[MAX_ACCOUNTS];

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            //convert to internal acc id
            int[] internalIdLikes = likesGb[i];
            if (internalIdLikes != null && internalIdLikes.length > 0) {
                int[] intIdArr = new int[internalIdLikes.length];
                //map to local account ids
                for (int j = 0; j < internalIdLikes.length; j++) {
                    intIdArr[j] = internalIdLikes[j] & 0x00FFFFFF;
                }
                likesByUser[i] = DirectMemoryAllocator.allocTripleIntArrAndFill(intIdArr.length, intIdArr);

                for (int x = 0; x < internalIdLikes.length; x++) {
                    int usr = DirectMemoryAllocator.getFromTriple(likesByUser[i].data, x);
                    if (usr != (intIdArr[x] & 0x00FFFFFF)) {
                        throw new RuntimeException();
                    }
                }


                //DirectMemoryAllocator.getFromTriple(likesByUser[i].data, (29377));

                for (int lu = 0; lu < intIdArr.length; lu++) {
                    int likeToAcc = intIdArr[lu];
                    TripleIntArray a = likesForUser[likeToAcc];
                    if (a == null) {
                        a = DirectMemoryAllocator.allocTripleIntArr(10);
                        a.position = 0;
                        likesForUser[likeToAcc] = a;
                    } else if (a.position + 1 == a.size()) {
                        DirectMemoryAllocator.reallocTripleArr(a, a.size() + 10); //realloc(a.array, a.array.length + 10);
                    }
                    DirectMemoryAllocator.putToTriple(a, a.position++, i);
                }
            }
        }

        for (TripleIntArray a: likesForUser) {
            if (a != null && a.data != null && a.data.length > 0) {
                DirectMemoryAllocator.reallocTripleArr(a, a.position); //realloc(a.array, a.array.length + 10);
            }
        }
    }

    @Override
    public void processNew(InternalAccount a) {
        //all done by updateLikes
    }

    public static void updateLikes(int liker) {
        try {
            int[] newLikes = likesGb[liker];
            TripleIntArray oldLikes = likesByUser[liker];

            if (oldLikes == null) {
                return; //TODO
            }

            int[] newInstersect = new int[newLikes.length + oldLikes.size()];

            int newInstersectOffs = 0;

            //logger.error("oldLikes size - {}", oldLikes.size());
            int unionArraySize = 0;
            for (int newL : newLikes) {
                int newDemasker = newL & 0x00FFFFFF;
                boolean exist = false;
                for (int x = 0; x < oldLikes.size(); x++) {
                    int oldLike = DirectMemoryAllocator.getFromTriple(oldLikes.data, x);
                    if (newDemasker == oldLike) {
                        //logger.error("new - {}; old - {}", newDemasker, oldLike);
                        exist = true;
                    }
                }
                if (exist) {
                    unionArraySize++;
                    newInstersect[newInstersectOffs++] = newDemasker;
                }
            }
            unionArraySize += (newLikes.length - unionArraySize) + (oldLikes.size() - unionArraySize);

            byte[] newArr = new byte[unionArraySize * 3];

            int tripleCount = 0;

            for (int j = 0; j < newLikes.length; j++) {
                int newDemasker = newLikes[j] & 0x00FFFFFF;
                boolean equals = false;
                for (int i = 0; i < newInstersectOffs; i++) {
                    if (newDemasker == newInstersect[i]) {
                        equals = true;
                    }
                }
                if (!equals) {
                    DirectMemoryAllocator.putToTriple(newArr, tripleCount++, newDemasker);
                }
            }

            for (int j = 0; j < oldLikes.size(); j++) {
                int oldLike = DirectMemoryAllocator.getFromTriple(oldLikes.data, j);
                boolean equals = false;
                for (int i = 0; i < newInstersectOffs; i++) {
                    if (oldLike == newInstersect[i]) {
                        equals = true;
                    }
                }
                if (!equals) {
                    DirectMemoryAllocator.putToTriple(newArr, tripleCount++, oldLike);
                }
            }


            for (int lu = 0; lu < newArr.length / 3; lu++) {
                int likeToAcc = DirectMemoryAllocator.getFromTriple(newArr, lu);

                TripleIntArray a = likesForUser[likeToAcc];
                if (a == null) {
                    a = DirectMemoryAllocator.allocTripleIntArr(10);
                    a.position = 0;
                    likesForUser[likeToAcc] = a;
                } else {
                    DirectMemoryAllocator.reallocTripleArr(a, a.size() + 10); //realloc(a.array, a.array.length + 10);
                }
                DirectMemoryAllocator.putToTriple(a, a.position++, liker);
            }

            //TODO realloc after all updates
            for (TripleIntArray a : likesForUser) {
                if (a != null && a.data != null && a.data.length > 0 && a.size() > a.position) {
                    DirectMemoryAllocator.reallocTripleArr(a, a.position); //realloc(a.array, a.array.length + 10);
                }
            }
        } catch (RuntimeException e) {
            logger.error("updateLikes err", e);
        }

    }

    @Override
    public void calculatePriority() {
        calculatedPriority = 0;
        for (int id: accounts) {
            calculatedPriority += likesForUser[id].size();
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

            TripleIntArray a = likesForUser[accounts[0]];

            if (a == null || a.data == null || a.size() == 0) {
                return null;
            }

            if (currentSet == null) {
                long[] finalMap = bitmapForNewSetThreadLocal.get();
                DirectMemoryAllocator.clear(finalMap);
                for (int j = 0; j < a.size(); j++) {
                    int likelyUser = DirectMemoryAllocator.getFromTriple(a.data, j);
                    DirectMemoryAllocator.setBit(finalMap, likelyUser);
                }
                return finalMap;
            } else {
                for (int j = 0; j < a.size(); j++) {
                    int likelyUser = DirectMemoryAllocator.getFromTriple(a.data, j);
                    DirectMemoryAllocator.setBitAnd(currentSet, likelyUser);
                }
                return currentSet;
            }

        }

        for (int s: accounts) {
            if (finalSet != null && finalSet.isEmpty()) {
                return null;
            }
            TripleIntArray a = likesForUser[s];

            TreeSet<Integer> tmpSet = finalSet == null ? retainSetTL.get() : retainTMPSetTL.get();
            tmpSet.clear();

            for (int j = 0; j < a.size(); j++) {
                int likelyUser = DirectMemoryAllocator.getFromTriple(a.data, j);
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

        /*

        long[] finalMap = currentSet == null ? bitmapForNewSetThreadLocal.get() : bitmapForIntersectThreadLocal.get();
        DirectMemoryAllocator.clear(finalMap);



        long[] bp = bitmapForIntersect2ThreadLocal.get();
        DirectMemoryAllocator.clear(bp);
        DirectMemoryAllocator.transformSetToBitMap(bp, accounts, likesForUser.length);
        int count = DirectMemoryAllocator.getBitsCounts(bp);
        //boolean isSet = DirectMemoryAllocator.isBitSet(bp, 15834);
        //boolean isSetEx = DirectMemoryAllocator.isBitSet(bp, (13178));


            //TODO add limit for bisection
        for (int s: accounts) {
            TripleIntArray a = likesForUser[s];

            for (int j = 0; j < a.size(); j++) {
                int likelyUser = DirectMemoryAllocator.getFromTriple(a.data, j);
                TripleIntArray b = likesByUser[likelyUser];
                long[] bm = DirectMemoryAllocator.transformTripleSetToBitMap(b.data, likesForUser.length);
                DirectMemoryAllocator.intersectArrays(bm, bp);
                int aCount = DirectMemoryAllocator.getBitsCounts(bm);
                if (count == aCount) {
                    DirectMemoryAllocator.setBit(finalMap, likelyUser);
                }
            }
        }

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
        */
    }

    @Override
    public void intersect(long[] bitSet) {
        TripleIntArray a = likesForUser[Integer.parseInt(value)];
        this.accounts = new int[] {Integer.parseInt(value)};
        DirectMemoryAllocator.intersectArrays(bitSet, a);
    }

    /*
    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        long[] bitMap = DirectMemoryAllocator.allocWithFill(likesForUser.length, 0xffffffffffffffffL);
        //TODO add limit for bisection
        Arrays.stream(accounts).forEach( s -> {
            long[] b = DirectMemoryAllocator.transformSetToPoolEntry<BitMapHolder>(likesForUser[s].array, likesForUser.length);
            DirectMemoryAllocator.intersectArrays(bitMap, b);
        });

        if (currentSet == null) {
            return bitMap;
        } else {
            if (isLast) {
                DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitMap, limit);
            } else {
                DirectMemoryAllocator.intersectArrays(currentSet, bitMap);
            }
        }
        return currentSet;
    }
    */

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (currentSet == null) {
            return Arrays.stream(accounts).flatMap(a -> Arrays.stream(likesForUser[a].array)).toArray();
        } else {
            return Arrays.stream(currentSet)
                    .filter(a -> a != Integer.MAX_VALUE)
                    .flatMap(a -> Arrays.stream(likesForUser[a].array))
                    .toArray();
        }
    }
    */

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
