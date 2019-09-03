package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.filters.BaseFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.DirectMemoryAllocator.BAD_ACCOUNT_IDX;

@Deprecated
public class InterestsFilter extends BaseFilter {

    static ThreadLocal<InterestsFilter> stringBuilderThreadLocal = ThreadLocal.withInitial(InterestsFilter::new);

    short[] interests;

    static Map<Integer, ArrayDataAllocator.IntArray> accountArrayByInterest = new HashMap<>();

    public InterestsFilter() {
    }

    @Override
    public void init() {
        /*
        for (int i = 0; i < accountsInterests.length; i++) {
            int[] interests = accountsInterests[i];
            if (interests != null) {
                for (int interest : interests) {
                    IntArray a = accountArrayByInterest.get(interest);
                    if (a == null) {
                        a = new IntArray();
                        a.array = new int[30000];
                        a.position = 0;
                        accountArrayByInterest.put(interest, a);
                    } else if (a.position + 1 == a.array.length) {
                        a.array = realloc(a.array, a.array.length + 10000);
                    }
                    a.array[a.position++] = i;
                }
            }
        }
        accountArrayByInterest.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        */
    }

    @Override
    public void calculatePriority() {
        calculatedPriority = 0;
        /*
        if (predicate.equalsIgnoreCase("interests_contains")) {
            calculatedPriority = Arrays.stream(interests)
                    .filter(i -> accountArrayByInterest.containsKey(i))
                    .map(i -> accountArrayByInterest.get(i).array.length).sum() / interests.length; //TODO
        } else if (predicate.equalsIgnoreCase("interests_any")) {
            calculatedPriority = Arrays.stream(interests)
                    .filter(i -> accountArrayByInterest.containsKey(i))
                    .map(i -> accountArrayByInterest.get(i).array.length).sum(); //TODO
        }
        */
    }

    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("interests_contains")) {
            if (currentSet == null) {
                //TODO
                //List<Integer> finalSet = new ArrayList<Integer>();

                int count = 0;
                for (int i = 1; i < MAX_ACC_ID_ADDED.get(); i++) {
                    ArrayDataAllocator.ShortArray is = accountsInterests[i];
                    int localCount = 0;
                    for (short interest: is.array) {
                        if (interest == i) {
                            localCount++;
                        }
                    }
                    count += localCount;
                }

                int[] finalSet = new int[count];

                count = 0;
                for (int i = 1; i < MAX_ACC_ID_ADDED.get(); i++) {
                    ArrayDataAllocator.ShortArray is = accountsInterests[i];
                    for (short interest: is.array) {
                        if (interest == i) {
                            finalSet[count++] = i;
                        }
                    }
                }

                return finalSet;
            } else {
                for (int i = 0; i <  currentSet.length; i++) {
                    int accIdx = currentSet[i];
                    ArrayDataAllocator.ShortArray is = accountsInterests[accIdx];
                    int inNumber = 0;
                    for (short interest: is.array) {
                        for (short reqI: interests) {
                            if (reqI == interest) {
                                inNumber++;
                            }
                        }
                    }
                    if (inNumber != interests.length) {
                        currentSet[i] = BAD_ACCOUNT_IDX;
                    }
                }
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("interests_any")) {
            if (currentSet == null) {

                int totalSize = 0;

                for (int i: interests) {
                    totalSize += accountArrayByInterest.get(i).array.length;
                }

                int[] result = new int[totalSize];
                int counter = 0;

                //TODO distinct
                for (int i: interests) {
                    int[] arr = accountArrayByInterest.get(i).array;
                    for (int in: arr) {
                        result[counter++] = in;
                    }
                }
                return result;
            } else {
                for (int i = 0; i <  currentSet.length; i++) {
                    int accIdx = currentSet[i];
                    ArrayDataAllocator.ShortArray is = accountsInterests[accIdx];
                    boolean isInterested = false;
                    for (short interest: is.array) {
                        for (short reqI: interests) {
                            if (reqI == interest) {
                                isInterested = true;
                                break;
                            }
                        }
                        if (isInterested) {
                            break;
                        }
                    }
                    if (!isInterested) {
                        currentSet[i] = BAD_ACCOUNT_IDX;
                    }
                }
                return currentSet;
            }
        } else {
            return currentSet; //TODO
        }
    }

    @Override
    public InterestsFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        InterestsFilter filter = stringBuilderThreadLocal.get();
        filter.fill(predicate, value, limit);
        int count = 0;
        String[] interests = filter.value.split(",");
        for (String interest: interests) {
            if (Dictionaries.interests.containsKey(interest.trim())) {
                count++;
            }
        }
        filter.interests = new short[count];
        count = 0;
        for (String interest: interests) {
            if (Dictionaries.interests.containsKey(interest.trim())) {
                filter.interests[count++] = Dictionaries.interests.get(interest);
            }
        }
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty() ||
                value[0].split(",").length == 0) {
            return false;
        }
        if (predicat.equalsIgnoreCase("interests_contains")
                || predicat.equalsIgnoreCase("interests_any")) {
            return true;
        } else {
            return false;
        }
    }
}
