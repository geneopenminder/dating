package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static com.highloadcup.ArrayDataAllocator.*;

@Deprecated
public class BirthFilter extends BaseFilter {

    private static final Logger logger = LogManager.getLogger(BirthFilter.class);

    static ThreadLocal<BirthFilter> bfTL = ThreadLocal.withInitial(BirthFilter::new);

    public static Map<Short, long[]> bYearBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByYear = new HashMap<Short, Integer>();


    //public static Map<Integer, IntArray> birthYearsMap = new HashMap<>();
    public static int[] birthDatesSorted = null;
    public static byte[] sortedByBirthDatesAccounts = null;

    private int position = -1;

    public BirthFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(bYearBitMaps.get(Short.parseShort(value)), 0, bitset, 0, bitset.length);
        int bc = DirectMemoryAllocator.getBitsCounts(bitset);
        if (bc > 1000) {
            throw new RuntimeException();
        }
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = bYearBitMaps.get(Short.parseShort(value));
        if (filterBitMap == null) {
            DirectMemoryAllocator.clear(bitSet);
        } else {
            DirectMemoryAllocator.intersectArrays(bitSet, filterBitMap);
        }
    }

    @Override
    public void init() {
        super.init();
        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            final int birth = birthDates[i];
            final short year = getYear(birth);

            long[] acb = bYearBitMaps.get(year);
            if (acb == null) {
                acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                bYearBitMaps.put(year, acb);
            }
            DirectMemoryAllocator.setBit(acb, i);
        }

        List<SortedNode> nodes = new ArrayList<SortedNode>(ArrayDataAllocator.ACCOUNTS_TOTAL.get());
        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            nodes.add(new SortedNode(birthDates[i], i));
        }

        nodes.sort(new Comparator<SortedNode>() {
            @Override
            public int compare(SortedNode o1, SortedNode o2) {
                return Long.compare(o1.value, o2.value);
            }
        });

        sortedByBirthDatesAccounts = DirectMemoryAllocator.allocTriple(nodes.size() + 1);
        birthDatesSorted = new int[nodes.size() + 1];
        for (int i = 1; i < nodes.size() + 1; i++) {
            DirectMemoryAllocator.putToTriple(sortedByBirthDatesAccounts, i, nodes.get(i-1).accountIdx);
            birthDatesSorted[i] = nodes.get(i-1).value;
        }

        bYearBitMaps.forEach((key, value1) -> {
            countByYear.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

        //clear TODO
        //ArrayDataAllocator.birthDates = null;
    }

    @Override
    public void processNew(InternalAccount a) {
        final int birth = a.birth;
        final short year = getYear(birth);

        long[] acb = bYearBitMaps.get(year);
        if (acb == null) {
            acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            bYearBitMaps.put(year, acb);
            countByYear.merge(year, 1, Integer::sum);
        } else {
            countByYear.put(year, 1);
        }
        DirectMemoryAllocator.setBit(acb, a.id);

        //TODO

    }

    @Override
    public BaseFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        BirthFilter filter = bfTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    public static short getYear(int timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date((long)timestamp * 1000L));
        return (short)cal.get(Calendar.YEAR);
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("birth_lt")
                || predicate.equalsIgnoreCase("birth_gt")) {
            int timestamp = Integer.parseInt(value);
            position = findPositionBinarySearch(birthDatesSorted, 1, birthDatesSorted.length - 1, timestamp);
            if (position < 0 && predicate.equalsIgnoreCase("birth_lt")) {
                calculatedPriority = 0;
            } else if (position > (birthDatesSorted.length - 1) && predicate.equalsIgnoreCase("birth_gt")) {
                calculatedPriority = 0;
            } else {
                if (predicate.equalsIgnoreCase("birth_lt")) {
                    calculatedPriority = Integer.MAX_VALUE; //position;
                } else if (predicate.equalsIgnoreCase("birth_gt")) {
                    calculatedPriority = Integer.MAX_VALUE; //birthDatesSorted.length - position;
                } else {
                    calculatedPriority = 0;
                }
            }
        } else if (predicate.equalsIgnoreCase("birth_year") || predicate.equalsIgnoreCase("birth")) {
            short year = Short.parseShort(value);
            Integer cnt = countByYear.get(year);
            if (cnt == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = cnt;
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
        if (predicate.equalsIgnoreCase("birth_year") || predicate.equalsIgnoreCase("birth")) {
            short year = Short.parseShort(value);
            long[] yearSet = bYearBitMaps.get(year);
            if (yearSet == null) {
                return null;
            }
            if (currentSet == null) {
                long[] bs = bisTL.get();
                System.arraycopy(yearSet, 0, bs, 0, bs.length);
                return bs;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, yearSet, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, yearSet);
                }
            }
        } else if (predicate.equalsIgnoreCase("birth_lt")) {
            if (position < 0) {
                return null;
            } else {
                int birth = Integer.parseInt(value);
                int finalPos = position > birthDatesSorted.length - 1 ? birthDatesSorted.length - 1 : position;
                for (int i = finalPos; i >= 0; i--) {
                    //int accIdx = DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, i);
                    int birthDateToCompare = birthDatesSorted[i];
                    if (Integer.compare(birth, birthDateToCompare) >= 0) {
                        finalPos = i;
                        break;
                    }
                }
                if (currentSet == null) {
                    long[] bs = bisTL.get();
                    DirectMemoryAllocator.clear(bs);
                    /*if (isLast) {
                        for (int j = finalPos; j >= finalPos - limit; j--) {
                            DirectMemoryAllocator.setBit(bs, sortedByBirthDatesAccounts[j]);
                        }
                        return bs;
                    } else {*/
                        for (int j = finalPos; j >= 0; j--) {
                            DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                        }
                        return bs;
                    //}
                } else {
                    /*if (isLast) {
                        int count = 0;
                        for (int j = finalPos; j >= 0; j--) {
                            //DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                            boolean isSet = DirectMemoryAllocator.setBitAnd(currentSet, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                            if (isSet) {
                                count++;
                            }
                            if (count > limit) {
                                break;
                            }
                        }
                    } else {*/
                        int opsCount = setBirthBits(currentSet, birth, -1, limit);

                        //for (int j = finalPos; j >= 0; j--) {
                        //    //DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                        //    DirectMemoryAllocator.setBitAnd(currentSet, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                        //}
                        //DirectMemoryAllocator.intersectArrays(currentSet, bs);
                    //}
                }
            }
        } else if (predicate.equalsIgnoreCase("birth_gt")) {
            if (position < 0) {
                return null;
            } else {
                int birth = Integer.parseInt(value);


                int finalPos = position > birthDatesSorted.length - 1 ? birthDatesSorted.length - 1 : position;
                for (int i = finalPos; i < birthDatesSorted.length; i++) {
                    //int accIdx = DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, i);
                    int birthDateToCompare = birthDatesSorted[i];
                    if (Integer.compare(birth, birthDateToCompare) <= 0) {
                        finalPos = i;
                        break;
                    }
                }
                if (currentSet == null) {
                    /*if (isLast) {
                        long[] bs = bisTL.get();
                        DirectMemoryAllocator.clear(bs);
                        for (int j = finalPos; j < finalPos + limit; j++) {
                            DirectMemoryAllocator.setBit(bs, sortedByBirthDatesAccounts[j]);
                        }
                        return bs;
                    } else {*/
                        long[] bs = bisTL.get();
                        DirectMemoryAllocator.clear(bs);
                        for (int j = finalPos; j < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; j++) {
                            DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, j));
                        }
                        return bs;
                    //}
                } else { //TODO add is single
                    /*if (isLast) {
                        int count = 0;
                        for (int x = finalPos; x < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; x++) {
                            boolean isSet = DirectMemoryAllocator.setBitAnd(currentSet, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, x));
                            if (isSet) {
                                count++;
                            }
                            if (count > limit) {
                                break;
                            }
                        }
                    } else {*/
                        //long[] bs = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);


                        int opsCount = setBirthBits(currentSet, birth, 1, limit);
                        //logger.error("BirthFilter gt ops - {}", opsCount);

                        //for (int x = finalPos; x < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; x++) {
                        //    DirectMemoryAllocator.setBitAnd(currentSet, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, x));
                            //DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByBirthDatesAccounts, x));
                        //}
                        //DirectMemoryAllocator.intersectArrays(currentSet, bs);
                    //}
                }
            }
        }
        return currentSet;
    }

    public static int setBirthBits(long[] finalBitSet, int birth, int direction, int limit) {
        int operationsCount = 0;

        int foundAccs = 0;
        for (int i = ACCOUNTS_TOTAL.get(); i > 0; i--) {


            int dataIndex = i >>> 6; // x/5
            if (finalBitSet[dataIndex] == 0) {
                i -= 63;
                continue;
            }

            operationsCount++;
            int accBirth = birthDates[i];

            if (direction < 0) { //lt
                if (accBirth < birth) {
                    if (DirectMemoryAllocator.setBitAnd(finalBitSet, i)) {
                        foundAccs++;
                    }
                } else {
                    DirectMemoryAllocator.unsetBit(finalBitSet, i);
                }
            } else {//gt
                if (accBirth > birth) {
                    if (DirectMemoryAllocator.setBitAnd(finalBitSet, i)) {
                        foundAccs++;
                    }
                } else {
                    DirectMemoryAllocator.unsetBit(finalBitSet, i);
                }
            }

            if (foundAccs > limit) {
                break;
            }
        }
        return operationsCount;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("birth_year")) {
            int year = Integer.parseInt(value);
            IntArray accounts = birthYearsMap.get(year);
            if (accounts == null) {
                return new int[0];
            } else {
                return copySet(accounts.array);
            }
        } else if (predicate.equalsIgnoreCase("birth_lt")) {
            if (position < 0) {
                return new int[0];
            } else {
                long birth = Long.parseLong(value);
                if (currentSet == null) {
                    int finalPos = position > birthDatesSorted.length - 1 ? birthDatesSorted.length - 1 : position;
                    for (int i = finalPos; i >= 0; i--) {
                        int accIdx = sortedByBirthDatesAccounts[i];
                        long birthDateToCompare = birthDates[accIdx];
                        if (Long.compare(birth, birthDateToCompare) >= 0) {
                            finalPos = i;
                            break;
                        }
                    }
                    int[] finalSet = new int[finalPos + 1];
                    System.arraycopy(sortedByBirthDatesAccounts, 0, finalSet, 0, finalSet.length);
                    return finalSet;
                } else {
                    //TODO
                    for (int i = 0; i < currentSet.length; i++) {
                        final int idx = currentSet[i];
                        if (idx != Integer.MAX_VALUE) {
                            long birthDateToCompare = birthDates[idx];
                            if (Long.compare(birth, birthDateToCompare) <= 0) {
                                currentSet[i] = Integer.MAX_VALUE;
                            }
                        }
                    }
                    return currentSet;
                }
            }
        } else if (predicate.equalsIgnoreCase("birth_gt")) {
            if (position > birthDatesSorted.length - 1) {
                return new int[0];
            } else {
                long birth = Long.parseLong(value);
                if (currentSet == null) {
                    int finalPos = position < 0 ? 0 : position;
                    for (int i = finalPos; i < sortedByBirthDatesAccounts.length; i++) {
                        int accIdx = sortedByBirthDatesAccounts[i];
                        long birthDateToCompare = birthDates[accIdx];
                        if (Long.compare(birth, birthDateToCompare) <= 0) {
                            finalPos = i;
                            break;
                        }
                    }
                    int[] finalSet = new int[sortedByBirthDatesAccounts.length - finalPos];
                    System.arraycopy(sortedByBirthDatesAccounts, finalPos, finalSet, 0, finalSet.length);
                    return finalSet;
                } else {
                    for (int i = 0; i < currentSet.length; i++) {
                        final int idx = currentSet[i];
                        if (idx != Integer.MAX_VALUE) {
                            long birthDateToCompare = birthDates[idx];
                            if (Long.compare(birth, birthDateToCompare) >= 0) {
                                currentSet[i] = Integer.MAX_VALUE;
                            }
                        }
                    }
                    return currentSet;
                }
            }
        } else {
            throw new RuntimeException("getFilteredSet Birth oooops!");
        }
    }*/

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("birth_lt")
                || predicat.equalsIgnoreCase("birth_gt")
                || predicat.equalsIgnoreCase("birth_year")
                || predicat.equalsIgnoreCase("birth")) {
            return true;
        } else {
            return false;
        }
    }
}
