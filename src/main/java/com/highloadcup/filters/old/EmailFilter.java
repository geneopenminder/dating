package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.model.InternalAccount;

import java.util.*;

import static com.highloadcup.ArrayDataAllocator.*;

@Deprecated
public class EmailFilter extends BaseFilter {

    static ThreadLocal<EmailFilter> efTL = ThreadLocal.withInitial(EmailFilter::new);

    public static Map<Short, long[]> domainAccountsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByDomain = new HashMap<Short, Integer>();

    //EmailFilter
    //public static final Map<Integer, IntArray> emailDomainsEqMap = new HashMap<>();
    public static long[] emailsIndexes = null;
    public static byte[] sortedByEmailAccounts = null;

    private int position = -1;

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
        emailsIndexes = new long[ACCOUNTS_TOTAL.get() + 1];
        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            final String email = emailsPlain[i];
            emailsIndexes[i] = getEmailIndex(email);
        }

        List<SortedLongNode> nodes = new ArrayList<SortedLongNode>(ACCOUNTS_TOTAL.get() + 1);
        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            nodes.add(new SortedLongNode(emailsIndexes[i], i));
        }

        nodes.sort(new Comparator<SortedLongNode>() {
            @Override
            public int compare(SortedLongNode o1, SortedLongNode o2) {
                return Long.compare(o1.value, o2.value);
            }
        });

        sortedByEmailAccounts = DirectMemoryAllocator.allocTriple(nodes.size());
        for (int i = 1; i < nodes.size(); i++) {
            DirectMemoryAllocator.putToTriple(sortedByEmailAccounts, i, nodes.get(i).accountIdx);
            emailsIndexes[i] = nodes.get(i).value;
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

        //emailsIndexes[account.id] = getEmailIndex(account.email);

        //TODO indexes

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
            long emailIdx = getEmailIndex(value);

            position = findPositionBinarySearch(emailsIndexes, 1, emailsIndexes.length - 1, emailIdx);
            if (position < 0 && predicate.equalsIgnoreCase("email_lt")) {
                calculatedPriority = 0;
            } else if (position > (emailsIndexes.length - 1) && predicate.equalsIgnoreCase("email_gt")) {
                calculatedPriority = 0;
            } else {
                if (predicate.equalsIgnoreCase("email_lt")) {
                    calculatedPriority = position;
                } else if (predicate.equalsIgnoreCase("email_gt")) {
                    calculatedPriority = emailsIndexes.length - position;
                } else {
                    calculatedPriority = 0; //emailsIndexes.length / Dictionaries.emailDomainsMap.size(); //TODO
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
        } else if (predicate.equalsIgnoreCase("email_lt")) {
            if (position < 0) {
                return null;
            } else {
                String email = value;
                int finalPos = position > ACCOUNTS_TOTAL.get() ? ACCOUNTS_TOTAL.get() : position;
                for (int i = finalPos; i >= 0; i--) {
                    int accIdx = DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, i);
                    String emailToCompare = emailsPlain[accIdx];
                    if (email.compareTo(emailToCompare) >= 0) {
                        finalPos = i;
                        break;
                    }
                }
                if (currentSet == null) {
                    //int[] finalSet = new int[finalPos + 1];
                    //System.arraycopy(sortedByEmailAccounts, 0, finalSet, 0, finalSet.length);
                    /*if (isLast) {
                        long[] bs = DirectMemoryAllocator.alloc(emailDomainsMap.length);
                        for (int j = 0; j < limit; j++) {
                            DirectMemoryAllocator.setBit(bs, sortedByEmailAccounts[j]);
                        }
                        return bs;
                    } else {*/
                        long[] bs = bisTL.get();
                        DirectMemoryAllocator.clear(bs);
                        for (int j = 0; j < finalPos + 1; j++) {
                            DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, j));
                        }
                        return bs;
                    //}
                } else {
                    //if (isLast) {
                        long[] bs = bisTL.get();
                        DirectMemoryAllocator.clear(bs);
                        for (int j = 0; j < finalPos + 1; j++) {
                            DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, j));
                        }
                        DirectMemoryAllocator.intersectArrays(currentSet, bs);
                    //} else {
                    //    for (int j = 0; j < finalPos + 1; j++) {
                    //        DirectMemoryAllocator.setBitAnd(currentSet, sortedByEmailAccounts[j]);
                    //    }
                    //}
                }
            }
        } else if (predicate.equalsIgnoreCase("email_gt")) {
            if (position > emailsIndexes.length - 1) {
                return null;
            } else {
                String email = value;
                int finalPos = position < 0 ? 0 : position;
                for (int i = finalPos; i < emailsIndexes.length; i++) {
                    int accIdx = DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, i);
                    String emailToCompare = emailsPlain[accIdx];
                    if (email.compareTo(emailToCompare) <= 0) {
                        finalPos = i;
                        break;
                    }
                }
                if (currentSet == null) {
                    //if (isLast) {
                        long[] bs = bisTL.get();
                        DirectMemoryAllocator.clear(bs);
                        for (int j = finalPos; j < emailsIndexes.length; j++) {
                            DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, j));
                        }
                        return bs;
                    //} else {
                    //    long[] bs = DirectMemoryAllocator.alloc(emailDomainsMap.length);
                    //    for (int j = finalPos; j < sortedByEmailAccounts.length; j++) {
                    //        DirectMemoryAllocator.setBit(bs, sortedByEmailAccounts[j]);
                    //    }
                    //    return bs;
                    //}
                } else {
                    long[] bs = bisTL.get();
                    DirectMemoryAllocator.clear(bs);
                    for (int j = finalPos; j < emailsIndexes.length; j++) {
                        DirectMemoryAllocator.setBit(bs, DirectMemoryAllocator.getFromTriple(sortedByEmailAccounts, j));
                    }
                    DirectMemoryAllocator.intersectArrays(currentSet, bs);
                }
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
