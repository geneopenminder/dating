package com.highloadcup.filters.tree;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.filters.SexFilter;

import java.util.TreeSet;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;


public class SexFilterTree extends BaseFilter {

    static ThreadLocal<SexFilterTree> sfThreadLocal = ThreadLocal.withInitial(SexFilterTree::new);

    //m - 1; f - 2
    TreeSet<Integer> maleSet = new TreeSet<>();
    TreeSet<Integer> femaleSet = new TreeSet<>();

    public static int totalM = 0;
    public static int totalF = 0;

    @Override
    public void init() {
        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            if (ArrayDataAllocator.sexGlobal[i] == 1) {
                maleSet.add((i));
            } else {
                femaleSet.add((i));
            }
        }

        totalM = maleSet.size();
        totalF = femaleSet.size();
    }

    public SexFilterTree() {
    }

    public SexFilterTree(String predicate, String[] value, int limit) {
        super(predicate, value, limit);
    }


    @Override
    public void fill(String predicate, String[] value, int limit) {
    }


    @Override
    public SexFilterTree clone(String predicate, String[] value, int limit) {
        super.reset();
        SexFilterTree filter = sfThreadLocal.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void calculatePriority() {
        if (value.equalsIgnoreCase("m")) {
            calculatedPriority = totalM;
        } else {
            calculatedPriority = totalF;
        }
    }

    @Override
    public int[] getFilteredSet(int[] currentSet) {
        return currentSet;
    }

    @Override
    public int[] getFilteredSet(int[] currentSet, int limit) {
        return currentSet;
    }

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        return super.getBitmapFilteredSet(currentSet);
    }

    @Override
    public void fillBitSetForGroup(long[] bitSet) {
    }

    @Override
    public void intersect(long[] bitSet) {
        //super.intersect(bitSet);
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        return false;
    }

}
