package com.highloadcup.filters;

import com.highloadcup.DirectMemoryAllocator;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;
import static com.highloadcup.ArrayDataAllocator.MAX_ACC_ID_ADDED;

public class FullScanCompoundFilter {

    boolean enabled = false;

    boolean birthLT = false;
    boolean birthGT = false;
    int birthToCompare;

    boolean emailIdxLT = false;
    boolean emailIdxGT = false;
    long emailIdxToCompare;

    public void disableAndClear() {
        enabled = false;

        birthLT = false;
        birthGT = false;
        birthToCompare = 0;

        emailIdxLT = false;
        emailIdxGT = false;
        emailIdxToCompare = 0;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void addPredicate(String predicate, String value) {
        if (predicate.equalsIgnoreCase("birth_lt")) {
            birthLT = true;
            birthToCompare = Integer.parseInt(value);
        } else if (predicate.equalsIgnoreCase("birth_gt")) {
            birthGT = true;
            birthToCompare = Integer.parseInt(value);
        } else if (predicate.equalsIgnoreCase("email_lt")) {
            emailIdxLT = true;
            emailIdxToCompare = EmailFilter.getEmailIndex(value);
        } else if (predicate.equalsIgnoreCase("email_gt")) {
            emailIdxGT = true;
            emailIdxToCompare = EmailFilter.getEmailIndex(value);

        }

    }

    static ThreadLocal<int[]> bpToSetTL = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[64];
        }
    };

    public boolean compareWithCompoundFilter(int accId) {
        boolean result = false;
        if (birthLT || birthGT) {
            if (birthLT) {
                if (DirectMemoryAllocator.getBirthDateFromFS(accId) < birthToCompare) {
                    result = true;
                }
            } else if (birthGT) {
                if (DirectMemoryAllocator.getBirthDateFromFS(accId) > birthToCompare) {
                    result = true;
                }
            }

            if (!result) {
                return false;
            }
        }


        if (emailIdxLT || emailIdxGT) {
            if (emailIdxLT) {
                if (DirectMemoryAllocator.getEmailIdxFromFS(accId) < emailIdxToCompare) {
                    result = true;
                }
            } else if (emailIdxGT) {
                if (DirectMemoryAllocator.getEmailIdxFromFS(accId) > emailIdxToCompare) {
                    result = true;
                }
            }
        }
        return result;
    }

    public int[] transformBitMapToSet(long[] finalBitSet, int limit) {
        if (finalBitSet == null) {
            return null;
        } else {
            int totalCount = DirectMemoryAllocator.getBitsCounts(finalBitSet);
            if (totalCount == 0) {
                return null;
            }
            int[] finalSet = bpToSetTL.get();
            DirectMemoryAllocator.clear(finalSet);

            int num = 0;

            for (int i = MAX_ACC_ID_ADDED.get(); i > 0; i--) {

                int dataIndex = i >>> 6; // x/5
                if (finalBitSet[dataIndex] == 0) {
                    int offset = i % 64;
                    i -= offset == 0 ? 63 : offset;
                    continue;
                }

                if (DirectMemoryAllocator.isBitSet(finalBitSet, i)) {
                    if (compareWithCompoundFilter(i)) {
                        finalSet[num++] = i;
                    }
                    //precalBitCount--;
                    //if (precalBitCount == 0) {
                    //    trySkip = true;
                    //}
                }
                if (num >= limit || num >= totalCount) {
                    //logger.error("skip count - {}", skipCount);
                    return finalSet;
                }
            }
            //logger.error("skip count - {}", skipCount);
            return finalSet;
        }
    }


}
