package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;

import java.util.HashMap;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.phoneGlobal;

public class PhoneFilter extends BaseFilter {

    static ThreadLocal<PhoneFilter> pTL = ThreadLocal.withInitial(PhoneFilter::new);

    public static Map<Integer, long[]> phoneCodeBitMaps = new HashMap<Integer, long[]>();
    public static Map<Integer, Integer> countByCode = new HashMap<Integer, Integer>();

    public static long[] nullBitMap = null;
    public static long[] notNullBitMap = null;

    public static int nullCount = 0;
    public static int notNullCount = 0;

    public PhoneFilter() {
    }

    @Override
    public void init() {
        super.init();

        nullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            if (phoneGlobal[i] == Integer.MAX_VALUE) {
                nullCount++;
                DirectMemoryAllocator.setBit(nullBitMap, i);
            } else {
                notNullCount++;
                DirectMemoryAllocator.setBit(notNullBitMap, i);

                final long p = phoneGlobal[i];
                final int prefix = getPrefix(p);

                long[] phoneCodeMap = phoneCodeBitMaps.get(prefix);
                if (phoneCodeMap == null) {
                    phoneCodeMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    phoneCodeBitMaps.put(prefix, phoneCodeMap);
                }
                DirectMemoryAllocator.setBit(phoneCodeMap, i);
            }
        }

        phoneCodeBitMaps.forEach((key, value1) -> {
            countByCode.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

    }

    public static int getPrefix(long phone) {
        return (int)(phone / 10_000_000L) % 1000;
    }

    @Override
    public void processNew(InternalAccount account) {
        if (phoneGlobal[account.id] == Integer.MAX_VALUE) {
            nullCount++;
            DirectMemoryAllocator.setBit(nullBitMap, account.id);
        } else {
            notNullCount++;
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);

            final long p = account.phone;
            final int prefix = getPrefix(p);

            long[] phoneCodeMap = phoneCodeBitMaps.get(prefix);
            if (phoneCodeMap == null) {
                phoneCodeMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                phoneCodeBitMaps.put(prefix, phoneCodeMap);
            }
            DirectMemoryAllocator.setBit(phoneCodeMap, account.id);
            countByCode.merge(prefix, 1, Integer::sum);
        }

    }

    @Override
    public void processUpdate(InternalAccount account) {
        long oldPhone = phoneGlobal[account.id];
        long newPhone = account.phone;

        if (oldPhone == Integer.MAX_VALUE) {
            nullCount--;
            notNullCount++;
            DirectMemoryAllocator.unsetBit(nullBitMap, account.id);
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);
        } else {
            //remove old info
            final int oldPrefix = getPrefix(oldPhone);
            long[] phoneCodeMap = phoneCodeBitMaps.get(oldPrefix);
            DirectMemoryAllocator.unsetBit(phoneCodeMap, account.id);
            countByCode.merge(oldPrefix, -1, Integer::sum);
        }


        //add new info
        final int prefix = getPrefix(newPhone);

        long[] phoneCodeMap = phoneCodeBitMaps.get(prefix);
        if (phoneCodeMap == null) {
            phoneCodeMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            phoneCodeBitMaps.put(prefix, phoneCodeMap);
        }
        DirectMemoryAllocator.setBit(phoneCodeMap, account.id);
        countByCode.merge(prefix, 1, Integer::sum);
    }

    @Override
    public void calculatePriority() {
        calculatedPriority = 0;
        if (predicate.equalsIgnoreCase("phone_code")) {
            final int prefix = Integer.parseInt(value);
            Integer accounts = countByCode.get(prefix);
            if (accounts == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = accounts;
            }
        } else if (predicate.equalsIgnoreCase("phone_null")) {
            if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullCount;
            } else {
                calculatedPriority = notNullCount;
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
        if (predicate.equalsIgnoreCase("phone_code")) {
            int code = Integer.parseInt(value);
            if(!phoneCodeBitMaps.containsKey(code)) {
                return null;
            }
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(phoneCodeBitMaps.get(code), 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, phoneCodeBitMaps.get(code), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, phoneCodeBitMaps.get(code));
                }
            }
        } else if (predicate.equalsIgnoreCase("phone_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(notNullBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, notNullBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, notNullBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] set = bisTL.get();
                    System.arraycopy(nullBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullBitMap);
                    }
                }
            }
        }
        return currentSet;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("phone_code")) {
            int code = Integer.parseInt(value);
            if (currentSet == null) {
                return copySet(accs.array);
            } else {
                invalidateSetNonEquals(currentSet, phoneCodesByAccount, code);
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("phone_null")) {
            if (currentSet == null) {
                if (value.equalsIgnoreCase("1")) {
                    return copySet(phonesNull);
                } else {
                    return copySet(phonesNotNull);
                }
            } else {
                if (value.equalsIgnoreCase("1")) {
                    invalidateSetNonEquals(currentSet, phoneGlobal, 0);
                } else {
                    invalidateSetEquals(currentSet, phoneGlobal, 0);
                }
                return currentSet;
            }
        } else {
            throw new RuntimeException("getFilteredSet PhoneFilter oooops!");
        }
    }
*/

    @Override
    public PhoneFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        PhoneFilter filter = pTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("phone_code")) {
            if (!value[0].chars().allMatch(Character::isDigit)) {
                return false;
            } else {
                return true;
            }
        } else if (predicat.equalsIgnoreCase("phone_null")) {
            return true;
        } else {
            return false;
        }
    }

}
