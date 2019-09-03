package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;

import static com.highloadcup.ArrayDataAllocator.*;

public class PremiumFilter extends BaseFilter {

    static ThreadLocal<PremiumFilter> pTL = ThreadLocal.withInitial(PremiumFilter::new);

    public static int nullCount = 0;
    public static int notNullCount = 0;
    public static int nowCount = 0;

    public static long[] nowPremiumBitMap = null;
    public static long[] nullPremiumBitMap = null;
    public static long[] existPremiumBitMap = null;

    public PremiumFilter() {
    }

    public static boolean isPremium(long start, long finish) {

        return (start <= NOW_TIMESTAMP && finish >= NOW_TIMESTAMP);
        /*
        LocalDateTime startLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(start), ZoneId.of("UTC"));
        LocalDateTime finishLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(finish), ZoneId.of("UTC"));

        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        int nowYear = now.getYear();
        int nowDay = now.getDayOfYear();

        int startYear = startLDT.getYear();
        int startDay = startLDT.getDayOfYear();

        int finishYear = finishLDT.getYear();
        int finishDay = finishLDT.getDayOfYear();

        return (now.isAfter(startLDT)) && ((now.isBefore(finishLDT) && nowDay <= finishDay) || (now.isBefore(finishLDT.plusDays(1)) && nowDay == finishDay));
        */
    }

    @Override
    public void init() {
        super.init();
        nowPremiumBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        nullPremiumBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        existPremiumBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        //premiumsExists = new int[premiumStart.length];

        //ArrayDataAllocator.IntArray arr = new ArrayDataAllocator.IntArray();
        //arr.array = new int[100000];
        //arr.position = 0;

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {

            int extId = (i);

            if (premiumStart[i] == 0) {
                DirectMemoryAllocator.setBit(nullPremiumBitMap, i);
                //premiumsExists[i] = 0;
                nullCount++;
            } else if (isPremium(premiumStart[i], premiumFinish[i])){
                DirectMemoryAllocator.setBit(nowPremiumBitMap, i);
                DirectMemoryAllocator.setBit(existPremiumBitMap, i);
                //premiumsExists[i] = 1;
                notNullCount++;
                nowCount++;

                /*if (arr.position + 1 == arr.array.length) {
                    arr.array = realloc(arr.array, arr.array.length + 1000);
                    arr.position += 1;
                }
                arr.array[++arr.position] = i;*/
            } else {
                DirectMemoryAllocator.setBit(existPremiumBitMap, i);
                //premiumsExists[i] = 2;
            }
        }
    }

    @Override
    public void processNew(InternalAccount account) {
        if (account.premiumStart == 0) {
            DirectMemoryAllocator.setBit(nullPremiumBitMap, account.id);
            //premiumsExists[i] = 0;
            nullCount++;
        } else if (isPremium(account.premiumStart, account.premiumFinish)){
            DirectMemoryAllocator.setBit(nowPremiumBitMap, account.id);
            DirectMemoryAllocator.setBit(existPremiumBitMap, account.id);
            //premiumsExists[i] = 1;
            notNullCount++;
            nowCount++;
        } else {
            DirectMemoryAllocator.setBit(existPremiumBitMap, account.id);
            //premiumsExists[i] = 2;
        }

    }

    @Override
    public void processUpdate(InternalAccount account) {
        long oldPremiumStart = premiumStart[account.id];

        boolean wasPremium = false;

        if (oldPremiumStart == 0) {
            nullCount--;
            notNullCount++;
            DirectMemoryAllocator.unsetBit(nullPremiumBitMap, account.id);
        } else {
            if (isPremium(premiumStart[account.id], premiumFinish[account.id])) {
                wasPremium = true;
            }
        }

        if (isPremium(account.premiumStart, account.premiumFinish)) {
            if (!wasPremium) {
                DirectMemoryAllocator.setBit(nowPremiumBitMap, account.id);
                nowCount++;
            }
        } else {
            if (wasPremium) {
                DirectMemoryAllocator.unsetBit(nowPremiumBitMap, account.id);
                nowCount--;
            }
        }

        DirectMemoryAllocator.setBit(existPremiumBitMap, account.id);
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("premium_now")) {
            calculatedPriority = nowCount;
        } else if (predicate.equalsIgnoreCase("premium_null")) {
            if (value.equalsIgnoreCase("0")) {
                calculatedPriority = notNullCount;
            } else if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullCount;
            }
        }
    }

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("premium_now")) {
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(nowPremiumBitMap, 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nowPremiumBitMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, nowPremiumBitMap);
                }
            }
        } else if (predicate.equalsIgnoreCase("premium_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(existPremiumBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, existPremiumBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, existPremiumBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(nullPremiumBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullPremiumBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullPremiumBitMap);
                    }
                }
            }
        }
        return currentSet;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("premium_now")) {
            if (currentSet == null) {
                return copySet(nowPremium);
            } else {
                invalidateSetNonEquals(currentSet, premiumsExists, 1);
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("premium_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    int[] set = new int[nullCount];
                    int position = 0;
                    for (int i = 0; i < premiumsExists.length; i++) {
                        if (premiumsExists[i] == 0) {
                            set[position++] = i;
                        }
                    }
                    return set;
                } else {
                    invalidateSetEquals(currentSet, premiumsExists, 0);
                    return currentSet;
                }
            } else if (value.equalsIgnoreCase("1")) {
                if (currentSet == null) {
                    int[] set = new int[nullCount];
                    int position = 0;
                    for (int i = 0; i < premiumsExists.length; i++) {
                        if (premiumsExists[i] != 0) {
                            set[position++] = i;
                        }
                    }
                    return set;
                } else {
                    invalidateSetNonEquals(currentSet, premiumsExists, 0);
                    return currentSet;
                }
            }
        }
        return currentSet;
    }
    */

    @Override
    public PremiumFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        PremiumFilter filter = pTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (predicat.equalsIgnoreCase("premium_now")) {
            return true;
        } else if (predicat.equalsIgnoreCase("premium_null")) {
            if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
                return false;
            }
            if (value[0].equalsIgnoreCase("0") || value[0].equalsIgnoreCase("1")) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
