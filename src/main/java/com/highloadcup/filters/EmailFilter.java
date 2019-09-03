package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;

import java.util.HashMap;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;
import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.emailDomains;
import static com.highloadcup.ArrayDataAllocator.emailsPlain;

public class EmailFilter extends BaseFilter {

    static ThreadLocal<EmailFilter> efTL = ThreadLocal.withInitial(EmailFilter::new);

    public static Map<Short, long[]> domainAccountsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByDomain = new HashMap<Short, Integer>();

    //EmailFilter
    //public static final Map<Integer, IntArray> emailDomainsEqMap = new HashMap<>();

    public EmailFilter() {
    }

    @Override
    public void init() {
        super.init();
        //new

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            Short domainIdx = emailDomains[i];
            long[] domainMap = domainAccountsBitMaps.get(domainIdx);
            if (domainMap == null) {
                domainMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                domainAccountsBitMaps.put(domainIdx, domainMap);
            }
            DirectMemoryAllocator.setBit(domainMap, i);
        }

        domainAccountsBitMaps.forEach((key, value1) -> {
            countByDomain.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

        //old
        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            final String email = emailsPlain[i];
            long emailIdx = getEmailIndex(email);
            DirectMemoryAllocator.putEmailIdxToFS(i, emailIdx);
        }
    }

    public static long getEmailIndex(String email) {
        long first = (long)email.charAt(0);
        long second = (long)email.length() > 1 ? (long)email.charAt(1) : 0;
        long third = (long)email.length() > 2 ? (long)email.charAt(2) : 0;
        long fourth = (long)email.length() > 3 ? (long)email.charAt(3) : 0;
        long fshift = (long)first<<48L;
        long sshift = (long)second<<32L;
        long tshift = (long)third<<16L;
        long fourthshift = fourth<<0L;
        long sum = fshift + sshift + tshift + fourthshift;
        //final long emailIndex = (long)fourth + (long)third<<16L + (long)second<<32L + (long)first<<48L;
        return sum;
    }

    @Override
    public void processNew(InternalAccount account) {
        Short domainIdx = emailDomains[account.id];
        long[] domainMap = domainAccountsBitMaps.get(domainIdx);
        if (domainMap == null) {
            domainMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            domainAccountsBitMaps.put(domainIdx, domainMap);
        }
        DirectMemoryAllocator.setBit(domainMap, account.id);
        countByDomain.merge(domainIdx, 1, Integer::sum);

        final String email = emailsPlain[account.id];
        long emailIdx = getEmailIndex(email);
        DirectMemoryAllocator.putEmailIdxToFS(account.id, emailIdx);

    }

    @Override
    public void processUpdate(InternalAccount account) {
        short oldDomainIdx = emailDomains[account.id];

        final String domain = account.email.toLowerCase().trim().split("@")[1];
        Short newIdx = Dictionaries.emailDomainsMap.get(domain);

        if (oldDomainIdx != newIdx) {
            long[] oldIdxMap = domainAccountsBitMaps.get(oldDomainIdx);
            DirectMemoryAllocator.unsetBit(oldIdxMap, account.id);
            countByDomain.merge(oldDomainIdx, -1, Integer::sum);

            long[] newIdxMap = domainAccountsBitMaps.get(newIdx);
            if (newIdxMap == null) {
                newIdxMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                domainAccountsBitMaps.put(newIdx, newIdxMap);
            }
            DirectMemoryAllocator.setBit(newIdxMap, account.id);
            countByDomain.merge(newIdx, 1, Integer::sum);

        }

        long emailIdx = getEmailIndex(account.email);
        DirectMemoryAllocator.putEmailIdxToFS(account.id, emailIdx);

    }

    @Override
    public EmailFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        EmailFilter filter = efTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("email_domain")
                || predicat.equalsIgnoreCase("email_lt")
                || predicat.equalsIgnoreCase("email_gt")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("email_domain")) {
            Short domainIdx = Dictionaries.emailDomainsMap.get(value);
            if (domainIdx == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = countByDomain.get(domainIdx);
            }
        } else if (predicate.equalsIgnoreCase("email_lt")
                || predicate.equalsIgnoreCase("email_gt")) {
            //long emailIdx = getEmailIndex(value);
            calculatedPriority = Integer.MAX_VALUE; //TODO
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
        if (predicate.equalsIgnoreCase("email_domain")) {
            Short domainIdx = Dictionaries.emailDomainsMap.get(value);
            if (domainIdx == null) {
                return null;
            }
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(domainAccountsBitMaps.get(domainIdx), 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, domainAccountsBitMaps.get(domainIdx), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, domainAccountsBitMaps.get(domainIdx));
                }
            }
        } else if (predicate.equalsIgnoreCase("email_lt") || predicate.equalsIgnoreCase("email_gt")) {
            if (currentSet == null) {
                long[] set = bisTL.get();
                DirectMemoryAllocator.clearFF(set);
                return set;
            }
        }
        return currentSet;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("email_domain")) {
            if (Dictionaries.emailDomainsMap.containsKey(value)) {
                int domainIdx = Dictionaries.emailDomainsMap.get(value);

                if (currentSet == null) { //first filter
                    return copySet(emailDomainsEqMap.get(domainIdx).array);
                } else {
                    invalidateSetNonEquals(currentSet, emailDomainsMap, domainIdx);
                    return currentSet;
                }
            } else {
                return new int[0];
            }
        } else if (predicate.equalsIgnoreCase("email_lt")) {
            if (position < 0) {
                return new int[0];
            } else {
                String email = value;
                if (currentSet == null) {
                    int finalPos = position > emailsIndexes.length - 1 ? emailsIndexes.length - 1 : position;
                    for (int i = finalPos; i >= 0; i--) {
                        int accIdx = sortedByEmailAccounts[i];
                        String emailToCompare = emailsPlain[accIdx];
                        if (email.compareTo(emailToCompare) >= 0) {
                            finalPos = i;
                            break;
                        }
                    }
                    int[] finalSet = new int[finalPos + 1];
                    System.arraycopy(sortedByEmailAccounts, 0, finalSet, 0, finalSet.length);
                    return finalSet;
                } else {
                    //TODO
                    for (int i = 0; i < currentSet.length; i++) {
                        final int idx = currentSet[i];
                        if (idx != Integer.MAX_VALUE) {
                            String emailToCompare = emailsPlain[idx];
                            if (email.compareTo(emailToCompare) <= 0) {
                                currentSet[i] = Integer.MAX_VALUE;
                            }
                        }
                    }
                    return currentSet;
                }
            }
        } else if (predicate.equalsIgnoreCase("email_gt")) {
            if (position > emailsIndexes.length - 1) {
                return new int[0];
            } else {
                String email = value;
                if (currentSet == null) {
                    int finalPos = position < 0 ? 0 : position;
                    for (int i = finalPos; i < sortedByEmailAccounts.length; i++) {
                        int accIdx = sortedByEmailAccounts[i];
                        String emailToCompare = emailsPlain[accIdx];
                        if (email.compareTo(emailToCompare) <= 0) {
                            finalPos = i;
                            break;
                        }
                    }
                    int[] finalSet = new int[sortedByEmailAccounts.length - finalPos];
                    System.arraycopy(sortedByEmailAccounts, finalPos, finalSet, 0, finalSet.length);
                    return finalSet;
                } else {
                    for (int i = 0; i < currentSet.length; i++) {
                        final int idx = currentSet[i];
                        if (idx != Integer.MAX_VALUE) {
                            String emailToCompare = emailsPlain[idx];
                            if (email.compareTo(emailToCompare) >= 0) {
                                currentSet[i] = Integer.MAX_VALUE;
                            }
                        }
                    }
                    return currentSet;
                }
            }
        } else {
            throw new RuntimeException("getFilteredSet oooops!");
        }
    }
    */
}
