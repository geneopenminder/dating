package com.highloadcup;

import com.highloadcup.filters.EmailFilter;
import com.highloadcup.model.Account;
import com.highloadcup.model.InternalAccount;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayDataAllocator {

    public static long NOW_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC")).toEpochSecond(ZoneOffset.UTC);

    public static int MAX_ACCOUNTS = 1_390_000 + 1;

    public static int[] allAccounts = new int[MAX_ACCOUNTS];


    //temporary for init phase
    public static int[] joined = new int[MAX_ACCOUNTS];

    @Deprecated
    public static int[] birthDates = null;// new int[MAX_ACCOUNTS];

    //public static IntArray[] externalIdLikes = new IntArray[MAX_ACCOUNTS];

    public static long[] phoneGlobal = new long[MAX_ACCOUNTS];

    public static short[] fnamesGlobal = new short[MAX_ACCOUNTS];
    public static short[] snameGlobal = new short[MAX_ACCOUNTS];

    public static int[] premiumStart = new int[MAX_ACCOUNTS];
    public static int[] premiumFinish = new int[MAX_ACCOUNTS];

    public static short[] countryGlobal = new short[MAX_ACCOUNTS];
    public static short[] cityGlobal = new short[MAX_ACCOUNTS];

    public static byte[] sexGlobal = new byte[MAX_ACCOUNTS];
    public static byte[] statusGlobal = new byte[MAX_ACCOUNTS];

    public static String[] emailsPlain = new String[MAX_ACCOUNTS];
    public static short[] emailDomains = new short[MAX_ACCOUNTS];

    public static final HashMap<String, Integer> emailsGlobal = new HashMap<>();

    public static ShortArray[] accountsInterests = new ShortArray[MAX_ACCOUNTS];

    public static volatile AtomicInteger ACCOUNTS_TOTAL = new AtomicInteger(0);

    public static volatile AtomicInteger ACCOUNTS_LOADED = new AtomicInteger(0);

    public static volatile AtomicInteger MAX_ACC_ID_ADDED = new AtomicInteger(0);

    public static int TOTAL_ACC_LOADED = 0;

    public static int[][] likesGb = new int[MAX_ACCOUNTS][];
    public static int[][] likesTSGb = new int[MAX_ACCOUNTS][];

    public static class SortedIdNode {
        public int externalId;
        public int internalId;

        public SortedIdNode(int externalId, int internalId) {
            this.externalId = externalId;
            this.internalId = internalId;
        }
    }

    public static class ShortArray {
        public ShortArray() {
        }
        public ShortArray(short[] array) {
            this.array = array;
        }
        public short[] array;
        public short position;
    }

    public static class IntArray {
        public int[] array;
        public int position;
    }

    /*
    public static void convertLikes() {
        for (IntArray arr: externalIdLikes) {
            if (arr == null) {
                continue;
            }
            int[] newArr = new int[arr.array.length];
            for (int i = 0; i < arr.array.length; i++) {
                newArr[i] = arr.array[i];
            }
            arr.array = newArr;
        }
    }
*/
    /*
    public static void sortAllArraysByExternalAccID() {
        final List<SortedIdNode> nodes = new ArrayList<SortedIdNode>(idMap.size());
        idMap.forEach((ext, internal) -> {
            nodes.add(new SortedIdNode(ext, internal));
        });

        nodes.sort(new Comparator<SortedIdNode>() {
            @Override
            public int compare(SortedIdNode o1, SortedIdNode o2) {
                return Long.compare(o2.externalId, o1.externalId); //reverse
            }
        });

        {
            //new sorted data arrays
            int[] allAccountsNew = new int[nodes.size()];
            long[] birthDatesNew = new long[nodes.size()];
            int[] joinedNew = new int[nodes.size()];;
            long[] phoneNew = new long[nodes.size()];;
            short[] fnameNew = new short[nodes.size()];;
            short[] snameNew = new short[nodes.size()];;
            long[] premiumStartNew = new long[nodes.size()];;
            long[] premiumFinishNew = new long[nodes.size()];;
            short[] countryNew = new short[nodes.size()];;
            short[] cityNew = new short[nodes.size()];;
            byte[] sexNew = new byte[nodes.size()];;
            byte[] statusNew = new byte[nodes.size()];;
            String[] emailsPlainNew = new String[nodes.size()];
            short[] emailDomainsNew = new short[nodes.size()];
            ShortArray[] accountsInterestsNew = new ShortArray[nodes.size()];
            IntArray[] externalIdLikesNew = new IntArray[externalIdLikes.length];

            idMap.clear();
            idMapReverse.clear();

            for (int i = 0; i < nodes.size(); i++) {
                SortedIdNode n = nodes.get(i);
                int externalId = n.externalId;
                int oldInternalId = n.internalId;
                int newInternalId = i;

                idMap.put(externalId, newInternalId);
                idMapReverse.put(newInternalId, externalId);

                String email = emailsPlain[oldInternalId];
                emailsGlobal.put(email, newInternalId);

                allAccountsNew[newInternalId] = allAccounts[oldInternalId];
                birthDatesNew[newInternalId] = birthDates[oldInternalId];
                joinedNew[newInternalId] = joined[oldInternalId];
                phoneNew[newInternalId] = phoneGlobal[oldInternalId];
                fnameNew[newInternalId] = fnamesGlobal[oldInternalId];
                snameNew[newInternalId] = snameGlobal[oldInternalId];
                premiumStartNew[newInternalId] = premiumStart[oldInternalId];
                premiumFinishNew[newInternalId] = premiumFinish[oldInternalId];
                countryNew[newInternalId] = countryGlobal[oldInternalId];
                cityNew[newInternalId] = cityGlobal[oldInternalId];
                sexNew[newInternalId] = sexGlobal[oldInternalId];
                statusNew[newInternalId] = statusGlobal[oldInternalId];
                emailsPlainNew[newInternalId] = emailsPlain[oldInternalId];
                emailDomainsNew[newInternalId] = emailDomainsMap[oldInternalId];
                accountsInterestsNew[newInternalId] = accountsInterests[oldInternalId];
                externalIdLikesNew[newInternalId] = externalIdLikes[oldInternalId];
            }

            allAccounts = allAccountsNew;
            birthDates = birthDatesNew;
            joined = joinedNew;
            phoneGlobal = phoneNew;
            fnamesGlobal = fnameNew;
            snameGlobal = snameNew;
            premiumStart = premiumStartNew;
            premiumFinish = premiumFinishNew;
            countryGlobal = countryNew;
            cityGlobal = cityNew;
            sexGlobal = sexNew;
            statusGlobal = statusNew;
            emailsPlain = emailsPlainNew;
            emailDomainsMap = emailDomainsNew;
            accountsInterests = accountsInterestsNew;
            externalIdLikes = externalIdLikesNew;
        }

    }

    @Deprecated
    public static void increaseArrays(int increaseSize) {
        int[] newIntArr = new int[MAX_ACCOUNTS + increaseSize];
        System.arraycopy(allAccounts, 0, newIntArr, 0, MAX_ACCOUNTS);
        allAccounts = newIntArr;

        long[] newLArr = new long[birthDates.length + increaseSize];
        System.arraycopy(birthDates, 0, newLArr, 0, birthDates.length);
        birthDates = newLArr;

        newIntArr = new int[joined.length + increaseSize];
        System.arraycopy(joined, 0, newIntArr, 0, joined.length);
        joined = newIntArr;

        newLArr = new long[phoneGlobal.length + increaseSize];
        System.arraycopy(phoneGlobal, 0, newLArr, 0, phoneGlobal.length);
        phoneGlobal = newLArr;

        newLArr = new long[premiumStart.length + increaseSize];
        System.arraycopy(premiumStart, 0, newLArr, 0, premiumStart.length);
        premiumStart = newLArr;

        newLArr = new long[premiumFinish.length + increaseSize];
        System.arraycopy(premiumFinish, 0, newLArr, 0, premiumFinish.length);
        premiumFinish = newLArr;

        short[] newShortArr = new short[fnamesGlobal.length + increaseSize];
        System.arraycopy(fnamesGlobal, 0, newShortArr, 0, fnamesGlobal.length);
        fnamesGlobal = newShortArr;

        newShortArr = new short[snameGlobal.length + increaseSize];
        System.arraycopy(snameGlobal, 0, newShortArr, 0, snameGlobal.length);
        snameGlobal = newShortArr;


        newShortArr = new short[countryGlobal.length + increaseSize];
        System.arraycopy(countryGlobal, 0, newShortArr, 0, countryGlobal.length);
        countryGlobal = newShortArr;

        newShortArr = new short[cityGlobal.length + increaseSize];
        System.arraycopy(cityGlobal, 0, newShortArr, 0, cityGlobal.length);
        cityGlobal = newShortArr;

        byte[] newByteArr = new byte[sexGlobal.length + increaseSize];
        System.arraycopy(sexGlobal, 0, newByteArr, 0, sexGlobal.length);
        sexGlobal = newByteArr;

        newByteArr = new byte[statusGlobal.length + increaseSize];
        System.arraycopy(statusGlobal, 0, newByteArr, 0, statusGlobal.length);
        statusGlobal = newByteArr;

        newShortArr = new short[emailDomainsMap.length + increaseSize];
        System.arraycopy(emailDomainsMap, 0, newShortArr, 0, emailDomainsMap.length);
        emailDomainsMap = newShortArr;

        ShortArray[] ai = new ShortArray[accountsInterests.length + increaseSize];
        System.arraycopy(accountsInterests, 0, ai, 0, accountsInterests.length);
        accountsInterests = ai;

        String[] newStrArr = new String[emailsPlain.length + increaseSize];
        System.arraycopy(emailsPlain, 0, newStrArr, 0, emailsPlain.length);
        emailsPlain = newStrArr;

        IntArray[] a = new IntArray[externalIdLikes.length + increaseSize];
        System.arraycopy(externalIdLikes, 0, a, 0, externalIdLikes.length);
        externalIdLikes = a;
    }
    */


    public static void setMaxAccId(int newId) {
        int current = MAX_ACC_ID_ADDED.get();
        if (current < newId) {
            boolean isSet = MAX_ACC_ID_ADDED.compareAndSet(current, newId);
            while (!isSet) {
                current = MAX_ACC_ID_ADDED.get();
                isSet = MAX_ACC_ID_ADDED.compareAndSet(current, newId);
            }
        }
    }


    public static void fillData(List<InternalAccount> accounts) {
        accounts.forEach( account -> {
            ACCOUNTS_TOTAL.incrementAndGet();
            ACCOUNTS_LOADED.incrementAndGet();
            setMaxAccId(account.id);

            final int currentIdx = account.id;

            allAccounts[currentIdx] = currentIdx;

            DirectMemoryAllocator.putBirthDateToFS(currentIdx, account.birth);

            joined[currentIdx] = account.joined;
            phoneGlobal[currentIdx] = account.phone;

            fnamesGlobal[currentIdx] = account.fname;
            snameGlobal[currentIdx] = account.sname;

            premiumStart[currentIdx] = account.premiumStart;
            premiumFinish[currentIdx] = account.premiumFinish;

            countryGlobal[currentIdx] = account.country;
            cityGlobal[currentIdx] = account.city;

            sexGlobal[currentIdx] = account.sex;
            statusGlobal[currentIdx] = account.status;

            emailDomains[currentIdx] = Dictionaries.emailDomainsMap.get(account.email.split("@")[1]);
            emailsPlain[currentIdx] = account.email;

            DirectMemoryAllocator.putEmailIdxToFS(currentIdx, EmailFilter.getEmailIndex(account.email));

            emailsGlobal.put(emailsPlain[currentIdx], currentIdx);

            if (account.interests != null && !account.interests.isEmpty()) {
                ShortArray sa = new ShortArray();

                sa.array = new short[account.interests.size()];

                for (int i = 0; i < account.interests.size(); i++ ) {
                    sa.array[i] = Dictionaries.interests.get(account.interests.get(i).trim());
                }
                Arrays.sort(sa.array);
                accountsInterests[currentIdx] = sa;
            } else {
                accountsInterests[currentIdx] = null;
            }

            if (account.likeIds != null && account.likeIds.length > 0) {
                IntArray a = new IntArray();
                a.array = account.likeIds;
                //externalIdLikes[currentIdx] = a;


                Map<Integer, Integer> offsets = new HashMap<>();

                LikeTS[] pairs = new LikeTS[account.likes.size()];
                int[] accLikes = new int[account.likes.size()];
                int[] accTs = new int[account.likes.size()];

                int offset = 0;
                for (int o = 0; o < account.likes.size(); o++) {
                    Account.Like l = account.likes.get(o);
                    Integer offs = offsets.get(l.id);

                    if (offs == null) {
                        offsets.put(l.id, offset);
                        accLikes[offset] = l.id | 0x01000000;
                        accTs[offset] = l.ts;

                        pairs[offset] = new LikeTS(accLikes[offset], l.ts);
                        offset++;
                    } else {
                        int count = (int)((int)(accLikes[offs] & 0xFF000000) >>> 24);
                        int newTS = (int)(((long)accTs[offs] * (long)count + (long)l.ts) / (long) (count + 1));
                        accTs[offs] = newTS;
                        count++;
                        accLikes[offs] = (int)(l.id & 0x00FFFFFF) | (int)(count << 24);

                        LikeTS old = pairs[offs];
                        old.accId = accLikes[offs];
                        old.ts = newTS;
                    }
                }

                LikeTS[] pairsNew = new LikeTS[offset];

                System.arraycopy(pairs, 0, pairsNew, 0, offset);

                pairs = pairsNew;

                Arrays.sort(pairs, new Comparator<LikeTS>() {
                    @Override
                    public int compare(LikeTS o1, LikeTS o2) {
                        return Integer.compare(o1.ts, o2.ts);
                    }
                });

                likesGb[account.id] = DirectMemoryAllocator.realloc(accLikes, offset);
                likesTSGb[account.id] = DirectMemoryAllocator.realloc(accTs, offset);

                for (int j = 0; j < offset; j++) {
                    likesGb[account.id][j] = pairs[j].accId;
                    likesTSGb[account.id][j] = pairs[j].ts;
                }

            } else {
                //externalIdLikes[currentIdx] = null;
            }
        });
    }

    public static void fillSingleData(InternalAccount account) {

        final int currentIdx = account.id;

        allAccounts[currentIdx] = currentIdx; //TODO remove

        DirectMemoryAllocator.putBirthDateToFS(currentIdx, account.birth);

        //joined[currentIdx] = account.joined;
        phoneGlobal[currentIdx] = account.phone;

        fnamesGlobal[currentIdx] = account.fname;
        snameGlobal[currentIdx] = account.sname;

        premiumStart[currentIdx] = account.premiumStart;
        premiumFinish[currentIdx] = account.premiumFinish;

        countryGlobal[currentIdx] = account.country;
        cityGlobal[currentIdx] = account.city;

        sexGlobal[currentIdx] = account.sex;
        statusGlobal[currentIdx] = account.status;

        emailDomains[currentIdx] = Dictionaries.emailDomainsMap.get(account.email.toLowerCase().split("@")[1]);
        emailsPlain[currentIdx] = account.email.toLowerCase().trim();

        synchronized (emailsGlobal) {
            emailsGlobal.put(emailsPlain[currentIdx], currentIdx);
        }

        DirectMemoryAllocator.putEmailIdxToFS(currentIdx, EmailFilter.getEmailIndex(account.email));

        if (account.interestsNumber > 0) {
                ShortArray sa = new ShortArray();

                sa.array = new short[account.interestsNumber];

                for (int i = 0; i < account.interestsNumber; i++ ) {
                    short itrId = Dictionaries.interests.get(Dictionaries.escToUnescapedHashes.get(account.interestsHashes[i]));
                    sa.array[i] = itrId;
                }
                Arrays.sort(sa.array);
                accountsInterests[currentIdx] = sa;
        } else {
            accountsInterests[currentIdx] = null;
        }
    }



    public static class LikeTS {
        public int accId;
        public int ts;

        public LikeTS() {
        }

        public LikeTS(int accId, int ts) {
            this.accId = accId;
            this.ts = ts;
        }
    }

}
