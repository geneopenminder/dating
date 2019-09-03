package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.model.InternalAccount;

import java.util.HashMap;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.DirectMemoryAllocator.BAD_ACCOUNT_IDX;

@Deprecated
public class SNameFilter extends BaseFilter {

    static ThreadLocal<SNameFilter> snTL = ThreadLocal.withInitial(SNameFilter::new);

    //public static Map<Integer, long[]> sNameAccountsBitMaps = new HashMap<Integer, long[]>();
    public static Map<Short, Integer> countBySName = new HashMap<Short, Integer>();

    public static long[] nullSNamesBitMap = null;
    public static long[] notNullSNamesBitMap = null;

    public static int nullSNamesCount = 0;
    public static int notnullSNamesCount = 0;


    //SNameFilter
    public static final Map<Short, IntArray> sNamesEqMap = new HashMap<>();
    //public static int[] sNamesNull = null;
    //public static int[] sNamesNotNull = null;

    //sname prefixes maps
    final static Map<String, IntArray> oneSymbolMap =new HashMap<>();
    final static Map<String, IntArray> twoSymbolMap =new HashMap<>();
    final static Map<String, IntArray> threeSymbolMap =new HashMap<>();
    final static Map<String, IntArray> fourSymbolMap =new HashMap<>();
    final static Map<String, IntArray> fiveSymbolMap =new HashMap<>();
    final static Map<String, IntArray> sixSymbolMap =new HashMap<>();
    final static Map<String, IntArray> sevenSymbolMap =new HashMap<>();


    public SNameFilter() {
        super();
    }

    public void init() {
        super.init();

        nullSNamesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullSNamesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            short nameCode = snameGlobal[i];
            if (nameCode != Short.MAX_VALUE) {
                notnullSNamesCount++;
                DirectMemoryAllocator.setBit(notNullSNamesBitMap, i);

                IntArray a = sNamesEqMap.get(nameCode);
                if (a == null) {
                    a = new IntArray();
                    a.array = new int[1000];
                    a.position = 0;
                    sNamesEqMap.put(nameCode, a);
                } else if (a.position + 1 == a.array.length) {
                    a.array = realloc(a.array, a.array.length + 10);
                }
                a.array[a.position++] = i;
            } else {
                nullSNamesCount++;
                DirectMemoryAllocator.setBit(nullSNamesBitMap, i);
            }
        }

        sNamesEqMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        /*
        Dictionaries.snames.forEach((key, value) -> {
            List<Integer> ids = new ArrayList<>(10000);
            for (int i = 0; i < snameGlobal.length; i++) {
                if (snameGlobal[i] == value) {
                    ids.add(i);
                }
            }

            IntArray arr = new IntArray();
            arr.array = ids.stream().mapToInt(Integer::intValue).toArray();
            sNamesEqMap.put(value, arr);
        });*/

        sNamesEqMap.forEach((key, value1) -> {
            countBySName.put(key, value1.array.length);
        });

        /*
        List<Integer> nullIds = new ArrayList<>(1000000);
        List<Integer> notNullIds = new ArrayList<>(1000000);
        for (int i = 0; i < snameGlobal.length; i++) {
            if (snameGlobal[i] == 0) {
                nullIds.add(i);
            } else {
                notNullIds.add(i);
            }
        }
        */
        //sNamesNull = nullIds.stream().mapToInt(Integer::intValue).toArray();
        //sNamesNotNull = notNullIds.stream().mapToInt(Integer::intValue).toArray();

        for (int id = 1; id < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; id++) {
            short idx = snameGlobal[id];
            if (idx == Short.MAX_VALUE) {
                continue;
            }

            fillPrefixArrays(id, idx);

        }

        oneSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        twoSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        threeSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        fourSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        fiveSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        sixSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
        sevenSymbolMap.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
    }

    public static void fillPrefixArrays(int id, short snameIdx) {
        String sname = Dictionaries.snamesById.get(snameIdx);

        if (sname.length() > 0) {
            String one = sname.substring(0, 1);
            addToArray(one, oneSymbolMap, id);
        }

        if (sname.length() > 1) {
            String two = sname.substring(0, 2);
            addToArray(two, twoSymbolMap, id);
        }

        if (sname.length() > 2) {
            String three = sname.substring(0, 3);
            addToArray(three, threeSymbolMap, id);
        }

        if (sname.length() > 3) {
            String four = sname.substring(0, 4);
            addToArray(four, fourSymbolMap, id);
        }

        if (sname.length() > 4) {
            String five = sname.substring(0, 5);
            addToArray(five, fiveSymbolMap, id);
        }

        if (sname.length() > 5) {
            String six = sname.substring(0,6);
            addToArray(six, sixSymbolMap, id);
        }

        if (sname.length() > 6) {
            String seven = sname.substring(0, 7);
            addToArray(seven, sevenSymbolMap, id);
        }
    }

    public static void removeFromPrefixArrays(int id, short snameIdx) {
        String sname = Dictionaries.snamesById.get(snameIdx);

        if (sname.length() > 0) {
            String one = sname.substring(0, 1);
            removeFromArray(one, oneSymbolMap, id);
        }

        if (sname.length() > 1) {
            String two = sname.substring(0, 2);
            removeFromArray(two, twoSymbolMap, id);
        }

        if (sname.length() > 2) {
            String three = sname.substring(0, 3);
            removeFromArray(three, threeSymbolMap, id);
        }

        if (sname.length() > 3) {
            String four = sname.substring(0, 4);
            removeFromArray(four, fourSymbolMap, id);
        }

        if (sname.length() > 4) {
            String five = sname.substring(0, 5);
            removeFromArray(five, fiveSymbolMap, id);
        }

        if (sname.length() > 5) {
            String six = sname.substring(0,6);
            removeFromArray(six, sixSymbolMap, id);
        }

        if (sname.length() > 6) {
            String seven = sname.substring(0, 7);
            removeFromArray(seven, sevenSymbolMap, id);
        }
    }
    //TODO store only name idx
    public static void addToArray(String prefix, Map<String, IntArray> map, int id) {
        IntArray oneArr = map.get(prefix);
        if (oneArr == null) {
            oneArr = new IntArray();
            oneArr.array = new int[50];
            oneArr.position = 0;
            map.put(prefix, oneArr);
        } else if (oneArr.position == oneArr.array.length) {
            oneArr.array = realloc(oneArr.array, oneArr.array.length + 1);
        }
        oneArr.array[oneArr.position++] = id;
    }

    public static void removeFromArray(String prefix, Map<String, IntArray> map, int id) {
        IntArray oneArr = map.get(prefix);
        if (oneArr != null) {
            for (int i = 0; i < oneArr.array.length; i++) {
                if (oneArr.array[i] == id) {
                    oneArr.array[i] = BAD_ACCOUNT_IDX;
                }
            }
        }
    }

    @Override
    public void processNew(InternalAccount account) {
        short nameCode = account.sname;
        if (nameCode != Short.MAX_VALUE) {
            notnullSNamesCount++;
            DirectMemoryAllocator.setBit(notNullSNamesBitMap, account.id);

            IntArray a = sNamesEqMap.get(nameCode);
            if (a == null) {
                a = new IntArray();
                a.array = new int[1];
                a.position = 0;
                sNamesEqMap.put(nameCode, a);
            } else if (a.position == a.array.length) {
                a.array = realloc(a.array, a.array.length + 1);
            }
            a.array[a.position++] = account.id;
            countBySName.merge(nameCode, 1, Integer::sum);
        } else {
            nullSNamesCount++;
            DirectMemoryAllocator.setBit(nullSNamesBitMap, account.id);
        }

        if (nameCode != Short.MAX_VALUE) {
            fillPrefixArrays(account.id, nameCode);
        }

        //TODO realoc after all post
    }

    @Override
    public void processUpdate(InternalAccount account) {
        short oldIdx = snameGlobal[account.id];

        short newNameIdx = account.sname;

        if (oldIdx == Short.MAX_VALUE) {
            //was null in past
            notnullSNamesCount++;
            DirectMemoryAllocator.setBit(notNullSNamesBitMap, account.id);
            DirectMemoryAllocator.unsetBit(nullSNamesBitMap, account.id);
            countBySName.merge(newNameIdx, 1, Integer::sum);
        } else {
            //name changed

            countBySName.merge(oldIdx, -1, Integer::sum);
            countBySName.merge(newNameIdx, 1, Integer::sum);

            //remove from old name index
            IntArray a = sNamesEqMap.get(oldIdx);
            for (int i = 0; i < a.array.length; i++) {
                if (a.array[i] == account.id) {
                    a.array[i] = BAD_ACCOUNT_IDX;
                }
            }

            removeFromPrefixArrays(account.id, oldIdx);
        }

        IntArray a = sNamesEqMap.get(newNameIdx);
        if (a == null) {
            a = new IntArray();
            a.array = new int[1];
            a.position = 0;
            sNamesEqMap.put(newNameIdx, a);
        } else if (a.position == a.array.length) {
            a.array = realloc(a.array, a.array.length + 1);
        }
        a.array[a.position++] = account.id;

        fillPrefixArrays(account.id, newNameIdx);

    }

    @Override
    public SNameFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        SNameFilter filter = snTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("sname_eq") || predicat.equalsIgnoreCase("sname")) {
            return true;
        } else if(predicat.equalsIgnoreCase("sname_null")) {
            if (value[0].equalsIgnoreCase("0") || value[0].equalsIgnoreCase("1")) {
                return true;
            } else {
                return false;
            }
        } else if (predicat.equalsIgnoreCase("sname_starts")) {
            return true; //TODO
        }
        return false;
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("sname_eq") || predicate.equalsIgnoreCase("sname")) {
            Short sNameIndex = Dictionaries.snames.get(value);
            if (sNameIndex == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = countBySName.get(sNameIndex);
            }
        } else if (predicate.equalsIgnoreCase("sname_null")) {
            if (value.equalsIgnoreCase("0") ) {
                calculatedPriority = notnullSNamesCount;
            } else if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullSNamesCount;
            } else {
                calculatedPriority = 0;
            }
        } else if (predicate.equalsIgnoreCase("sname_starts")) {
            calculatedPriority = 0;
            if (value.length() > 6) {
                IntArray sevenArr = sevenSymbolMap.get(value.substring(0, 7));
                if (sevenArr != null) {
                    calculatedPriority = sevenArr.array.length;
                } else {
                    calculatedPriority = 0;
                }
            } else if (value.length() > 4) {
                IntArray fiveArr = fiveSymbolMap.get(value.substring(0, 5));
                if (fiveArr != null) {
                    calculatedPriority = fiveArr.array.length;
                } else {
                    calculatedPriority = 0;
                }
            } else if (value.length() > 2) {
                IntArray threeArr = threeSymbolMap.get(value.substring(0, 3));
                if (threeArr != null) {
                    calculatedPriority = threeArr.array.length;
                } else {
                    calculatedPriority = 0;
                }
            } else if (value.length() > 0) {
                IntArray oneArr = oneSymbolMap.get(value.substring(0, 1));
                if (oneArr != null) {
                    calculatedPriority = oneArr.array.length;
                } else {
                    calculatedPriority = 0;
                }
            }
        }
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

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("sname_eq") || predicate.equalsIgnoreCase("sname")) {
            Short sNameIndex = Dictionaries.snames.get(value);
            if (sNameIndex == null) {
                return null;
            }
            long[] bitSet = currentSet == null ? bitmapForNewSetThreadLocal.get() : bitmapForIntersectThreadLocal.get();
            DirectMemoryAllocator.clear(bitSet);

            DirectMemoryAllocator.transformSetToBitMap(bitSet, sNamesEqMap.get(sNameIndex).array, snameGlobal.length);
            if (currentSet == null) {
                return bitSet;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitSet, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, bitSet);
                }
            }
        } else if (predicate.equalsIgnoreCase("sname_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bitmapForNewSetThreadLocal.get();
                    System.arraycopy(notNullSNamesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, notNullSNamesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, notNullSNamesBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] set = bitmapForNewSetThreadLocal.get();
                    System.arraycopy(nullSNamesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullSNamesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullSNamesBitMap);
                    }
                }
            }
        } else if (predicate.equalsIgnoreCase("sname_starts")) {

            IntArray ids = null;
            final int length = value.length();
            if (length > 7) {
                DirectMemoryAllocator.clear(currentSet); //TODO
            } else if (length == 7) {
                ids = sevenSymbolMap.get(value);
            } else if (length == 6) {
                ids = sixSymbolMap.get(value);
            } else if (length == 5) {
                ids = fiveSymbolMap.get(value);
            } else if (length == 4) {
                ids = fourSymbolMap.get(value);
            } else if (length == 3) {
                ids = threeSymbolMap.get(value);
            } else if (length == 2) {
                ids = twoSymbolMap.get(value);
            } else if (length == 1) {
                ids = oneSymbolMap.get(value);
            }


            if (ids == null) {
                DirectMemoryAllocator.clear(currentSet);
            } else {
                long[] bitMap = currentSet == null ? bitmapForNewSetThreadLocal.get() : bitmapForIntersectThreadLocal.get();
                DirectMemoryAllocator.clear(bitMap);
                DirectMemoryAllocator.transformSetToBitMap(bitMap, ids.array, bitMap.length);
                //boolean isSet = DirectMemoryAllocator.isBitSet(bitMap, 30799);
                if (currentSet == null) {
                    return bitMap;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, bitMap);
                    }
                }
            }
        }
        return currentSet;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (currentSet != null && currentSet.length == 0) {
            return currentSet;
        }
        if (predicate.equalsIgnoreCase("sname_eq")) {
            if (Dictionaries.snames.containsKey(value)) {
                int sNameIndex = Dictionaries.snames.get(value);
                //return returnIntersect(currentSet, sNamesEqMap.get(sNameIndex).array);

                if (currentSet == null) { //first filter
                    return copySet(sNamesEqMap.get(sNameIndex).array);
                } else {
                    invalidateSetNonEquals(currentSet, snameGlobal, sNameIndex);
                    return currentSet;
                }
            } else {
                return new int[0];
            }
        } else if (predicate.equalsIgnoreCase("sname_null")) {
            if (currentSet == null) {
                if (value.equalsIgnoreCase("0")) {
                    return copySet(sNamesNotNull);
                } else if (value.equalsIgnoreCase("1")) {
                    return copySet(sNamesNull);
                } else {
                    return new int[0];
                }
            } else {
                if (value.equalsIgnoreCase("0")) {
                    invalidateSetEquals(currentSet, snameGlobal, Integer.MAX_VALUE);
                    return currentSet;
                } else if (value.equalsIgnoreCase("1")) {
                    invalidateSetNonEquals(currentSet, snameGlobal, Integer.MAX_VALUE);
                    return currentSet;
                } else {
                    return new int[0];
                }
            }
        } else if (predicate.equalsIgnoreCase("sname_starts")) {
            List<String> snames = Dictionaries.snames.keySet().stream()
                    .filter( snameGlobal -> snameGlobal.startsWith(value))
                    .collect(Collectors.toList());

            Set<Integer> snameIndexes = Dictionaries.snames.keySet().stream()
                    .filter( snameGlobal -> snameGlobal.startsWith(value))
                    .map(Dictionaries.snames::get)
                    .collect(Collectors.toSet());

            if (snames.isEmpty()) {
                return new int[0];
            } else {
                if (currentSet == null) {
                    int finalArrSize = snames.stream().mapToInt(snameGlobal -> sNamesEqMap.get(Dictionaries.snames.get(snameGlobal)).array.length).sum();
                    int[] finalArr = new int[finalArrSize];
                    int position = 0;
                    for (String s : snames) {
                        int[] a = sNamesEqMap.get(Dictionaries.snames.get(s)).array;
                        System.arraycopy(a, 0, finalArr, position, a.length);
                        position += a.length;
                    }
                    return finalArr;
                } else {
                    for (int i = 0; i < currentSet.length; i++) {
                        if (currentSet[i] != Integer.MAX_VALUE && !snameIndexes.contains(snameGlobal[currentSet[i]])) {
                            currentSet[i] = Integer.MAX_VALUE;
                        }
                    }
                    return currentSet;
                }
            }
        } else {
            throw new RuntimeException("getFilteredSet SName oooops!");
        }
    }
    */

}
