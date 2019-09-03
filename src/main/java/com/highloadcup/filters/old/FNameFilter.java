package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.model.InternalAccount;

import java.util.HashMap;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.fnamesGlobal;

@Deprecated
public class FNameFilter extends BaseFilter {

    static ThreadLocal<FNameFilter> fnTL = ThreadLocal.withInitial(FNameFilter::new);

    public static Map<Short, long[]> fNameAccountsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByFName = new HashMap<Short, Integer>();

    public static long[] nullFNamesBitMap = null;
    public static long[] notNullFNamesBitMap = null;

    public static int nullFNamesCount = 0;
    public static int notNnullFNamesCount = 0;


    //FNameFilter
    //public static final Map<Integer, IntArray> fNamesEqMap = new HashMap<>();
    //public static int[] fNamesNull = null;
    //public static int[] fNamesNotNull = null;

    public FNameFilter() {
    }

    public void init() {
        super.init();

        nullFNamesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullFNamesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            short nameCode = fnamesGlobal[i];
            if (nameCode != Short.MAX_VALUE) {
                notNnullFNamesCount++;
                DirectMemoryAllocator.setBit(notNullFNamesBitMap, i);
                long[] snameAccsMap = fNameAccountsBitMaps.get(nameCode);
                if (snameAccsMap == null) {
                    snameAccsMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    fNameAccountsBitMaps.put(nameCode, snameAccsMap);
                }
                DirectMemoryAllocator.setBit(snameAccsMap, i);
            } else {
                nullFNamesCount++;
                DirectMemoryAllocator.setBit(nullFNamesBitMap, i);
            }
        }
        /*
        Dictionaries.fnames.forEach((key, value) -> {
            List<Integer> ids = new ArrayList<>(10000);
            for (int i = 0; i < fnamesGlobal.length; i++) {
                if (fnamesGlobal[i] == value) {
                    ids.add(i);
                }
            }

            IntArray arr = new IntArray();
            arr.array = ids.stream().mapToInt(Integer::intValue).toArray();
            fNamesEqMap.put(value, arr);
        });

*/
        fNameAccountsBitMaps.forEach((key, value1) -> {
            countByFName.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });
    }

    @Override
    public void processNew(InternalAccount a) {
        short nameCode = a.fname;
        if (nameCode != Short.MAX_VALUE) {
            notNnullFNamesCount++;
            DirectMemoryAllocator.setBit(notNullFNamesBitMap, a.id);
            long[] snameAccsMap = fNameAccountsBitMaps.get(nameCode);
            if (snameAccsMap == null) {
                snameAccsMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                fNameAccountsBitMaps.put(nameCode, snameAccsMap);
            }
            DirectMemoryAllocator.setBit(snameAccsMap, a.id);
        } else {
            nullFNamesCount++;
            DirectMemoryAllocator.setBit(nullFNamesBitMap, a.id);
        }
        countByFName.merge(nameCode, 1, Integer::sum);
    }

    @Override
    public FNameFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        FNameFilter filter = fnTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }

        if (predicat.equalsIgnoreCase("fname_eq") || predicat.equalsIgnoreCase("fname")) {
            return true;
        } else if(predicat.equalsIgnoreCase("fname_null")) {
            if (value[0].equalsIgnoreCase("0") || value[0].equalsIgnoreCase("1")) {
                return true;
            } else {
                return false;
            }
        } else if (predicat.equalsIgnoreCase("fname_any")) {
            return true; //TODO
        }
        return false;

    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("fname_eq") || predicate.equalsIgnoreCase("fname")) {
            Short fNameIndex = Dictionaries.fnames.get(value);
            if (fNameIndex == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = countByFName.get(fNameIndex);
            }
        } else if (predicate.equalsIgnoreCase("fname_null")) {
            if (value.equalsIgnoreCase("0") ) {
                calculatedPriority = notNnullFNamesCount;
            } else if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullFNamesCount;
            } else {
                calculatedPriority = 0;
            }
        } else if (predicate.equalsIgnoreCase("fname_any")) {
            String[] fNameVals = value.split(",");
            if (fNameVals.length == 0) {
                calculatedPriority = 0;
            }
            calculatedPriority = 0;
            for (Map.Entry<String, Short> e: Dictionaries.fnames.entrySet()) {
                if (Dictionaries.fnames.containsKey(e.getKey())) {
                    calculatedPriority += e.getValue();
                }
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
        if (predicate.equalsIgnoreCase("fname_eq") || predicate.equalsIgnoreCase("fname")) {
            Short fNameIndex = Dictionaries.fnames.get(value);
            if (fNameIndex == null) {
                return null;
            }
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(fNameAccountsBitMaps.get(fNameIndex), 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, fNameAccountsBitMaps.get(fNameIndex), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, fNameAccountsBitMaps.get(fNameIndex));
                }
            }
        } else if (predicate.equalsIgnoreCase("fname_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(notNullFNamesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, notNullFNamesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, notNullFNamesBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(nullFNamesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullFNamesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullFNamesBitMap);
                    }
                }
            }
        } else if (predicate.equalsIgnoreCase("fname_any")) {
            String[] fnamesVal = value.split(",");
            short[] actualFNames = new short[fnamesVal.length];
            int length = 0;
            for (int i = 0; i < fnamesVal.length; i++) {
                String s = fnamesVal[i];
                Short idx = Dictionaries.fnames.get(s);
                if (idx != null) {
                    actualFNames[i] = idx;
                    length++;
                } else {
                    actualFNames[i] = Short.MAX_VALUE;
                }
            }
            if (length == 0) {
                return null;
            }
            //todo add limit

            long[] bitMap = bisTL.get();
            DirectMemoryAllocator.clear(bitMap);

            for (int i = 0; i < actualFNames.length; i++) {
                short fnameIdx = actualFNames[i];
                if (fnameIdx != Short.MAX_VALUE) {
                    long[] b = fNameAccountsBitMaps.get(fnameIdx);
                    DirectMemoryAllocator.bisectArrays(bitMap, b);
                }
            }

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
        return currentSet;
    }

    public static void updateForAccount(String fname, int idx) {
        short oldNameIdx = fnamesGlobal[idx]; //
        if (oldNameIdx != Short.MAX_VALUE) {
            final String oldName = Dictionaries.fnamesById.get(oldNameIdx);
            if (!oldName.equalsIgnoreCase(fname)) {
                short newNameIdx = 0;
                if (Dictionaries.fnames.containsKey(fname)) {
                    newNameIdx = Dictionaries.fnames.get(fname);
                    long[] nameBits = fNameAccountsBitMaps.get(newNameIdx);
                    DirectMemoryAllocator.setBit(nameBits, idx);
                    countByFName.put(newNameIdx, countByFName.get(newNameIdx) + 1);
                } else {
                    //newNameIdx = (short)++ArrayDataAllocator.seq; //TODO
                    Dictionaries.fnames.put(fname.trim(), newNameIdx);
                    Dictionaries.fnamesById.put(newNameIdx, fname.trim());
                    long[] newNameBits = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    fNameAccountsBitMaps.put(newNameIdx, newNameBits);
                    DirectMemoryAllocator.setBit(newNameBits, idx);
                    countByFName.put(newNameIdx, 1);
                }
                fnamesGlobal[idx] = newNameIdx;
                //unset old name bit
                long[] oldNameMap = fNameAccountsBitMaps.get(oldNameIdx);
                DirectMemoryAllocator.unsetBit(oldNameMap, idx);
                countByFName.put(oldNameIdx, countByFName.get(newNameIdx) - 1);
                DirectMemoryAllocator.setBit(notNullFNamesBitMap, idx);
            }
        }
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("fname_eq")) {
            if (Dictionaries.fnames.containsKey(value)) {
                int fnameIndex = Dictionaries.fnames.get(value);
                //return returnIntersect(currentSet, fnamesEqMap.get(fnameIndex).array);

                if (currentSet == null) { //first filter
                    return copySet(fNamesEqMap.get(fnameIndex).array);
                } else {
                    invalidateSetNonEquals(currentSet, fnamesGlobal, fnameIndex);
                    return currentSet;
                }
            } else {
                return new int[0];
            }
        } else if (predicate.equalsIgnoreCase("fname_null")) {
            if (currentSet == null) {
                if (value.equalsIgnoreCase("0")) {
                    return copySet(fNamesNotNull);
                } else if (value.equalsIgnoreCase("1")) {
                    return copySet(fNamesNull);
                } else {
                    return new int[0];
                }
            } else {
                if (value.equalsIgnoreCase("0")) {
                    invalidateSetEquals(currentSet, fnamesGlobal, Integer.MAX_VALUE);
                    return currentSet;
                } else if (value.equalsIgnoreCase("1")) {
                    invalidateSetNonEquals(currentSet, fnamesGlobal, Integer.MAX_VALUE);
                    return currentSet;
                } else {
                    return new int[0];
                }
            }
        } else if (predicate.equalsIgnoreCase("fname_any")) {
            String[] fNameVals = value.split(",");
            List<String> fnames = Dictionaries.fnames.keySet().stream()
                    .filter( fnamesGlobal -> Arrays.stream(fNameVals).anyMatch(fnamesGlobal::equals))
                    .collect(Collectors.toList());

            Set<Integer> fnameIndexes = Dictionaries.fnames.keySet().stream()
                    .filter( fnamesGlobal -> Arrays.stream(fNameVals).anyMatch(fnamesGlobal::equals))
                    .map(Dictionaries.fnames::get)
                    .collect(Collectors.toSet());

            if (fnames.isEmpty()) {
                return new int[0];
            } else {
                if (currentSet == null) {
                    int finalArrSize = fnames.stream().mapToInt(fnamesGlobal -> fNamesEqMap.get(Dictionaries.fnames.get(fnamesGlobal)).array.length).sum();
                    int[] finalArr = new int[finalArrSize];
                    int position = 0;
                    for (String s : fnames) {
                        int[] a = fNamesEqMap.get(Dictionaries.fnames.get(s)).array;
                        System.arraycopy(a, 0, finalArr, position, a.length);
                        position += a.length;
                    }
                    return finalArr;
                } else {
                    for (int i = 0; i < currentSet.length; i++) {
                        if (currentSet[i] != Integer.MAX_VALUE && !fnameIndexes.contains(fnamesGlobal[currentSet[i]])) {
                            currentSet[i] = Integer.MAX_VALUE;
                        }
                    }
                    return currentSet;
                }
            }
        } else {
            throw new RuntimeException("getFilteredSet FName oooops!");
        }
    }
    */

}
