package com.highloadcup;

import com.highloadcup.filters.BirthFilter;
import com.highloadcup.filters.InterestsBitMapFilter;
import com.highloadcup.filters.PremiumFilter;
import com.highloadcup.filters.SexFilter;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.PoolEntry;
import com.highloadcup.pool.RelictumPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.MultithreadConcurrentQueueCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.*;

public class Recomendator {

    private static final Logger logger = LogManager.getLogger(Recomendator.class);

    /*
   Если в GET-запросе передана страна или город с ключами country и city соответственно,
    то нужно искать только среди живущих в указанном месте.

   По ключу "accounts" должны быть N пользователей,
   сортированных по убыванию их совместимости с обозначенным id.
   Число N задаётся в запросе GET-параметром limit и не бывает больше 20.

   В итоговом списке необходимо выводить только следующие поля: id, email, status, fname, sname, birth, premium, interests
       * */
    //{"accounts": [ ... ]}
    //GET: /accounts/89528/recommend/?country=Индция&limit=8&query_id=151

    /*
    {
    "accounts":  [
        {
            "email": "heernetletem@me.com",
            "premium": {"finish": 1546029018.0, "start": 1530304218},
            "status": "свободны",
            "sname": "Данашевен",
            "fname": "Анатолий",
            "id": 35473,
            "birth": 926357446
        },{
            "email": "teicfiwidadsuna@inbox.com",
            "premium": {"finish": 1565741391.0, "start": 1534205391},
            "status": "свободны",
            "id": 23067,
            "birth": 801100962
        },{
            "email": "nonihiwwahigtegodyn@inbox.com",
            "premium": {"finish": 1557069862.0, "start": 1525533862},
            "status": "свободны",
            "sname": "Стаметаный",
            "fname": "Виталий",
            "id": 90883,
            "birth": 773847481
        }
    ]
}
    * */


    static final TreeSet<Lover> EMPTY_lOVERS = new TreeSet<>();

    static ThreadLocal<long[]> bitmapRecomendThreadLocal = new ThreadLocal<long[]>() {

        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }

    };

    static ThreadLocal<List<Lover>> loversListThreadLocal = new ThreadLocal<List<Lover>>() {

        @Override
        protected List<Lover> initialValue() {
            return new ArrayList<Lover>(10000);
        }

    };

    static ThreadLocal<TreeSet<Lover>> lTL = ThreadLocal.withInitial(TreeSet::new);

    static ThreadLocal<Lover> lookingForTL = ThreadLocal.withInitial(Lover::new);

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };


    static ThreadLocal<Map<Integer, Long>> CALCULATED_PRIORITY_TL = ThreadLocal.withInitial(HashMap::new);

    static ThreadLocal<Map<Integer, Integer>> INTERESTS_INTERSECT_TL = ThreadLocal.withInitial(HashMap::new);

    public static Map<Integer, Lover> allLovers = new HashMap<>();

    //male
    public static TreeSet<Lover> allMLoversFunnySet = new TreeSet<>(); //+ interests
    public final static Map<Short, TreeSet<Lover>> countryMLoverSets = new HashMap<>();
    public final static Map<Short, TreeSet<Lover>> cityMLoverSets = new HashMap<>();

    //female
    public static TreeSet<Lover> allFLoversFunnySet = new TreeSet<>(); //+ interests
    public final static Map<Short, TreeSet<Lover>> countryFLoverSets = new HashMap<>();
    public final static Map<Short, TreeSet<Lover>> cityFLoverSets = new HashMap<>();

    public final static Map<Short, Map<Short, TreeSet<Lover>>> countryInterestsFLoverSets = new HashMap<>();
    public final static Map<Short, Map<Short, TreeSet<Lover>>> countryInterestsMLoverSets = new HashMap<>();

    public final static Map<Short, TreeSet<Lover>> interestsPremiumFLoverSets = new HashMap<>();
    public final static Map<Short, TreeSet<Lover>> interestsPremiumMLoverSets = new HashMap<>();

    public static Comparator<Lover> ageComparator = new Comparator<Lover>() {
        @Override
        public int compare(Lover o1, Lover o2) {
            if (o1.id == o2.id) {
                return 0;
            }

            int comp =  Long.compare(o1.age, o2.age);

            if (comp == 0) {
                return -1;
            }
            return comp;
        }
    };

    public static Comparator<Lover> id_Comparator = new Comparator<Lover>() {
        @Override
        public int compare(Lover o1, Lover o2) {
            return Long.compare(o1.id, o2.id);
        }
    };


    public static TreeSet<Lover> allFLoversBoringSet = new TreeSet<Lover>(ageComparator); //no interests
    public static TreeSet<Lover> allMLoversBoringSet = new TreeSet<Lover>(ageComparator); //no interests

    public static final HashMap<Short, InterestsLeaf> premiumInterestsMTree = new HashMap<>();
    public static final HashMap<Short, InterestsLeaf> premiumInterestsFTree = new HashMap<>();

    public static final HashMap<Short, InterestsLeaf> basicInterestsMTree = new HashMap<>();
    public static final HashMap<Short, InterestsLeaf> basicInterestsFTree = new HashMap<>();

    public static class LoverItr {
        public int idx;
        public int count;

        public LoverItr() {
        }

        public LoverItr(int idx) {
            this.idx = idx;
        }
    }

    public static void initRecommends() throws Exception {

        fillBaseLovers();

        /*
        int itrPerAccAvg = 0;
        List<Integer> itrPerAcc = new ArrayList<>();

        int avgLine = 0;
        List<Integer> loversForItrList = new ArrayList<>();


        int totalWithItr = 0;

        for (int jjj = 0; jjj < accountsInterests.length; jjj++){

            ShortArray lInterests = accountsInterests[jjj];

            if (lInterests == null) {
                continue;
            }
            totalWithItr++;

            itrPerAcc.add(lInterests.array.length);

            int bitCount = 0;
            for (short itr: lInterests.array) {
                bitCount += DirectMemoryAllocator.getBitsCounts(InterestsBitMapFilter.interestsBitMaps.get(itr));
            }

            loversForItrList.add(bitCount);
        }

        avgLine = new Double(loversForItrList.stream().mapToInt(Integer::intValue).average().getAsDouble()).intValue();
        itrPerAccAvg = new Double(itrPerAcc.stream().mapToInt(Integer::intValue).average().getAsDouble()).intValue();

        logger.error("all itr acc - {}; itr per acc - {}; avg lovers line - {}",
                totalWithItr, itrPerAccAvg, avgLine);

*/
        //System.exit(3);

        /*
        int[] i1 = interestsPremiumFLoverSets.get((short)45).stream().mapToInt(l -> l.id).toArray();
        int[] i2 = interestsPremiumFLoverSets.get((short)18).stream().mapToInt(l -> l.id).toArray();
        int[] i3 = interestsPremiumFLoverSets.get((short)12).stream().mapToInt(l -> l.id).toArray();

        Arrays.sort(i1);
        Arrays.sort(i2);
        Arrays.sort(i3);

        int number = 5000;

        //int[] full = null;

        int cycles = 10_000;

        long start = System.nanoTime();


        int[] full = new int[30_000];

        //Map<Integer, Integer> counts = new HashMap<>();

        Map<Integer, LoverItr> counts = new HashMap<>();

        int[][] users = new int[][]{i1, i2, i3};

        for (int j = 0; j < cycles; j++) {
            //int[] itr = linearArrayIntersect(i1, i2);

            //DirectMemoryAllocator.clear(full);
            //full = new int [i1.length + i2.length];

            //System.arraycopy(i1, 0, full, 0, i1.length);
            //System.arraycopy(i2, 0, full, i1.length, i2.length);

            counts.clear();

            for (int[] user: users) {
                for (int idx: user) {
                    LoverItr itr = counts.putIfAbsent(idx, null);
                    if (itr == null) {
                        itr = new LoverItr(idx);
                        itr.count = 1;
                        counts.put(idx, itr);
                    }
                    itr.count++;
                    //counts.computeIfPresent(idx, (key, value) -> value += 1);
                }
            }

            //counts.values().forEach(pool::free);
        }
        long end = System.nanoTime();


        logger.error("count - {}; total time - {}; req per sec - {}; avg - {}",
                cycles, (end - start), 1_000_000_000 / ((end - start)/cycles), ((end - start)/cycles));

        System.exit(3);

*/
        logger.error("start prepare lovers");
        //prepareAllLovers();
        logger.error("end prepare lovers");


        /*
        prepareBitmaps();

        long start = System.nanoTime();
        int count = 1000000;

        int[] indexes = I_BMAPS.keySet().stream().map( x -> new Integer((int)x)).mapToInt(x -> x.intValue()).toArray();


        int ppp = 0;

        int ops = 10000;
        for (int jjj = 0; jjj < accountsInterests.length; jjj++) {
            long startIntersect = System.nanoTime();

            ShortArray lInterests = accountsInterests[jjj];

            if (lInterests == null) {
                continue;
            }

            if (ppp++ > ops) {
                break;
            }
            long startInersect = System.nanoTime();
            Lover l = allLovers.get(jjj);

            long[] tmp = new long[2];

            int total = 0;
            TreeSet<Lover> tmpLovers = new TreeSet<>(new Comparator<Lover>() {
                @Override
                public int compare(Lover o1, Lover o2) {
                    return Integer.compare(o1.id, o2.id);
                }
            });
            InterestsBitmap loverBMap = I_BMAPS.get(l.id);

            for (short idx: lInterests.array) {
                TreeSet<Lover> lovers = interestsPremiumFLoverSets.get(idx);
                System.arraycopy(loverBMap.bmap, 0, tmp, 0, tmp.length);
                for(Lover wanted: lovers) {
                    if (tmpLovers.contains(wanted)) {
                        continue;
                    }
                    tmpLovers.add(wanted);
                    InterestsBitmap wantedMap = I_BMAPS.get(l.id);
                    DirectMemoryAllocator.intersectArrays(tmp, wantedMap.bmap);
                    int weight = DirectMemoryAllocator.getBitsCounts(tmp);
                    total++;
                    //logger.error("bit count - {}; total - {}", weight, total++);
                }
            }
            long endInersect = System.nanoTime();
            logger.error("intersect total - {}", total++);

            logger.error("ops - {}; total time : {}; avg - {}; rps - {}",total, endInersect - startIntersect, (endInersect - startIntersect) / total,
                    1000000000 / ((endInersect - startIntersect) / total));

        }

System.exit(3);

        long startInersect = System.nanoTime();
        Lover l = allLovers.get((14640));

        InterestsBitmap loverBMap = I_BMAPS.get(l.id);


        ShortArray lInterests = accountsInterests[l.id];

        long[] tmp = new long[2];

        int total = 0;
        TreeSet<Lover> tmpLovers = new TreeSet<>(new Comparator<Lover>() {
            @Override
            public int compare(Lover o1, Lover o2) {
                return Integer.compare(o1.id, o2.id);
            }
        });

        for (short idx: lInterests.array) {
            TreeSet<Lover> lovers = interestsPremiumFLoverSets.get(idx);
            System.arraycopy(loverBMap.bmap, 0, tmp, 0, tmp.length);
            for(Lover wanted: lovers) {
                if (tmpLovers.contains(wanted)) {
                    continue;
                }
                tmpLovers.add(wanted);
                InterestsBitmap wantedMap = I_BMAPS.get(l.id);
                DirectMemoryAllocator.intersectArrays(tmp, wantedMap.bmap);
                int weight = DirectMemoryAllocator.getBitsCounts(tmp);
                logger.error("bit count - {}; total - {}", weight, total++);
            }
        }
        long endInersect = System.nanoTime();

        logger.error("total - {}", endInersect - startInersect);

        System.exit(3);

        int totalOps = 0;
        for (int i = 0; i < indexes.length-1; i++) {
            if (totalOps > count) {
                break;
            }
            totalOps++;
            InterestsBitmap b1 = I_BMAPS.get(indexes[i]);
            InterestsBitmap b2 = I_BMAPS.get(indexes[i+1]);

            DirectMemoryAllocator.intersectArrays(b1.bmap, b2.bmap);
        }
        long finish = System.nanoTime();

        logger.error("ops - {}; total time : {}; avg - {}; rps - {}",totalOps, finish - start, (finish - start) / totalOps, 1000000000 / ((finish - start) / totalOps));

        System.exit(3);
        */
        //fillBaseInterestsTree(premiumInterestsMTree, (byte)1, true);
        //fillBaseInterestsTree(premiumInterestsFTree, (byte)0, true);
        //fillBaseInterestsTree(basicInterestsMTree, (byte)1, false);
        //fillBaseInterestsTree(basicInterestsFTree, (byte)0, false);

    }

    static ThreadLocal<Lover> loverForThreadLocal = new ThreadLocal<Lover>() {

        @Override
        protected Lover initialValue() {
            return new Lover();
        }

    };

    final static Set<String> VALID_KEY_SET = new TreeSet<String>() {{
        add("birth"); add("city"); add("email"); add("status"); add("country");
        add("sex"); add("sname"); add("fname"); add("likes"); add("phone");
        add("joined"); add("interests"); add("premium");
    }};


    static ThreadLocal<Boolean> isBasicSortTL = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return true;
        }

    };

    public static class InterestsLeaf {
        public short[] interestIds; //sorted
        public TreeSet<Lover> affectedLovers;

        public HashMap<Short, InterestsLeaf> nexLevelLeafs;

    }


    public static void fillBaseLovers() {
        isBasicSortTL.set(true);

        Dictionaries.countries.values().forEach( idx -> countryMLoverSets.put(idx, new TreeSet<Lover>()));
        Dictionaries.countries.values().forEach( idx -> countryInterestsMLoverSets.put(idx, new HashMap<Short, TreeSet<Lover>>()));
        Dictionaries.cities.values().forEach( idx -> cityMLoverSets.put(idx, new TreeSet<Lover>()));

        Dictionaries.countries.values().forEach( idx -> countryFLoverSets.put(idx, new TreeSet<Lover>()));
        Dictionaries.countries.values().forEach( idx -> countryInterestsFLoverSets.put(idx, new HashMap<Short, TreeSet<Lover>>()));
        Dictionaries.cities.values().forEach( idx -> cityFLoverSets.put(idx, new TreeSet<Lover>()));

        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            int loverId = i;

            byte sex = ArrayDataAllocator.sexGlobal[i];

            byte status = ArrayDataAllocator.statusGlobal[loverId];


            Lover l = new Lover();
            l.id = loverId;
            l.city = ArrayDataAllocator.cityGlobal[loverId];
            l.country = ArrayDataAllocator.countryGlobal[loverId];
            l.sex = ArrayDataAllocator.sexGlobal[loverId];
            l.age = DirectMemoryAllocator.getBirthDateFromFS(loverId);
            l.status = status;
            l.statusWeight = getStatusWeight(status) * 100_000_000_000L;
            l.premiumWeight = DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, loverId) ? 1000_000_000_000L : 0;
            l.isPremium = DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, loverId);

            allLovers.put(loverId, l);

            //if (status == 2) {
            //    continue; //заняты
            //}

            ShortArray interests = accountsInterests[i];
            boolean isFunny = false;
            if (interests != null) {
                isFunny = true;
                fillInterestsMap(interests.array, l);
            }

            l.basePriority = getBasePriority(l);

            if (l.isPremium) {
                if (sex == 1) {
                    if (isFunny) {
                        allMLoversFunnySet.add(l);
                    } else {
                        allMLoversBoringSet.add(l);
                    }
                } else {
                    if (isFunny) {
                        allFLoversFunnySet.add(l);
                    } else {
                        allFLoversBoringSet.add(l);
                    }
                }
            }

            short cityIdx = ArrayDataAllocator.cityGlobal[i];
            if (cityIdx != Short.MAX_VALUE) {
                if (sex == 1) {
                    cityMLoverSets.get(cityIdx).add(l);
                } else {
                    cityFLoverSets.get(cityIdx).add(l);
                }
            }

            short countryIdx = ArrayDataAllocator.countryGlobal[i];
            if (countryIdx != Short.MAX_VALUE) {
                if (sex == 1) {
                    countryMLoverSets.get(countryIdx).add(l);
                } else {
                    countryFLoverSets.get(countryIdx).add(l);
                }
            }
        }

        fillLoversForInterests(allMLoversFunnySet, interestsPremiumMLoverSets, true);
        fillLoversForInterests(allFLoversFunnySet, interestsPremiumFLoverSets, true);


        Dictionaries.countries.values().forEach(country -> {
            fillLoversForInterests(countryMLoverSets.get(country), countryInterestsMLoverSets.get(country), false);
            fillLoversForInterests(countryFLoverSets.get(country), countryInterestsFLoverSets.get(country), false);
        });

        countryMLoverSets.clear();
        countryFLoverSets.clear();
    }


    public static void fillInterestsMap(short[] array, Lover l) {
        l.iMap = new long[2];
        for (short iIdx : array) {
            DirectMemoryAllocator.setBit(l.iMap, iIdx);
        }
    }

    final public static Map<Integer, TreeSet<Lover>> ALL_FREE_LOVERS = new HashMap<Integer, TreeSet<Lover>>();

    public static void prepareAllLovers() {
        isBasicSortTL.set(false);

        for (int i = 0; i < accountsInterests.length; i++) {

            if (i % 1000 == 0) {
                logger.error("process i acc {}", i);
            }
            Map<Integer, Long> prMap = CALCULATED_PRIORITY_TL.get();
            prMap.clear();

            HashMap<Integer, Integer> counts = new HashMap<>();
            TreeSet<Lover> tmpLovers = new TreeSet<Lover>();

            ShortArray lfInterests = ArrayDataAllocator.accountsInterests[i];

            Lover lookingFor = allLovers.get(i);
            lookingForTL.set(lookingFor);

            if (lfInterests == null || lfInterests.array.length == 0) {
                continue;
            }

            INTERESTS_INTERSECT_TL.get().clear();

            for (short intr: lfInterests.array) {
                //set by id
                TreeSet<Lover> intrLovers = lookingFor.sex == 1 ? interestsPremiumFLoverSets.get(intr) : interestsPremiumMLoverSets.get(intr);
                tmpLovers.addAll(intrLovers);
            }

            int count = 0;

            Iterator<Lover> loverIterator = tmpLovers.iterator();

            TreeSet<Lover> finalSet = new TreeSet<>();
            while (loverIterator.hasNext() && count < 20) {
                Lover lover = loverIterator.next();
                finalSet.add(lover);
                count++;
            }

            ALL_FREE_LOVERS.put(i, tmpLovers.isEmpty() ? EMPTY_lOVERS : finalSet);
        }

    }


    public TreeSet<Lover> getLoversByInterests(Lover lookingFor) {
        ShortArray a = accountsInterests[lookingFor.id];

        int[][] lists = new int[a.array.length][];

        for (int i = 0; i < a.array.length; i++) {
            lists[i] = interestsPremiumFLoverSets.get(a.array[i]).stream().mapToInt( l -> l.id).toArray();
            Arrays.sort(lists[i]);
        }

        return null;
    }


    public static int[] linearArrayIntersect(int[] a1, int[] a2) {
        int[] first = a1[0] > a2[0] ? a1 : a2;
        int[] second = a1[0] <= a2[0] ? a1 : a2;

        int[] newArr = new int[a1.length > a2.length ? a1.length : a2.length];

        int pointer = 0;

        int foundCount = 0;
        int secondLength = second.length;
        for (int i = 0; i < first.length; i++) {
            if (pointer >= secondLength) {
                break;
            }
            if (secondLength > i) {
                int one = first[i];
                int two = second[pointer];

                if (one == two) {
                    newArr[foundCount++] = one;
                    pointer++;
                } else if (one > two){
                    for (++pointer; pointer < secondLength; pointer++) {
                        if (one ==  second[pointer]) {
                            newArr[foundCount++] = one;
                            break;
                        } else if (one < second[pointer]) {
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        logger.error("found count - {}", foundCount);
        return newArr;
    }

    public static TreeSet<Lover> findRecommendsForAllOld(int lookingForId, int limit) {
        CALCULATED_PRIORITY_TL.get().clear();
        INTERESTS_INTERSECT_TL.get().clear();
        isBasicSortTL.set(false);

        try {
            int id = (lookingForId);

            //TODO temporary
            if (id + 1 > ArrayDataAllocator.MAX_ACCOUNTS) {
                return EMPTY_lOVERS;
            }

            Lover lookingFor = allLovers.get(id);
            lookingForTL.set(lookingFor);

            byte sex = ArrayDataAllocator.sexGlobal[id];
            boolean isLookingForBoring = (accountsInterests[id] == null);

            TreeSet<Lover> tmp = lTL.get();
            tmp.clear();

            if (!isLookingForBoring) {

                if (ALL_FREE_LOVERS.containsKey(id)) {
                    return ALL_FREE_LOVERS.get(id);
                } else {
                    return EMPTY_lOVERS;
                }
                //if (wasPost) {
                //    return EMPTY_lOVERS;
                //}
                /*
                long[] tmpSet = bisTL.get();
                System.arraycopy(sex == 1 ? SexFilter.femaleBitMap : SexFilter.maleBitMap, 0, tmpSet, 0, tmpSet.length);

                ShortArray lfInterests = ArrayDataAllocator.accountsInterests[id];

                Arrays.sort(lfInterests.array);

                HashMap<Short, InterestsLeaf> tmap = lookingFor.sex == 1 ? premiumInterestsFTree : premiumInterestsMTree;

                lookForLoversByITree(tmap, tmp, lookingFor, limit);

                //DirectMemoryAllocator.intersectArrays(tmpSet, PremiumFilter.existPremiumBitMap);


                TreeSet<Lover> tmpLovers = new TreeSet<>(id_Comparator);


                HashMap<Integer, Integer> counts = new HashMap<>();


                for (short intr: lfInterests.array) {
                    //set by id
                    TreeSet<Lover> intrLovers = sex == 1 ? interestsPremiumFLoverSets.get(intr) : interestsPremiumMLoverSets.get(intr);
                    intrLovers.forEach( iLover -> {
                        Integer old = counts.putIfAbsent(iLover.id, 1);
                        if (old != null) {
                            counts.put(iLover.id, old + 1);
                        }
                    });
                }

                TreeSet<Map.Entry<Integer, Integer>> finalSet = new TreeSet<>(new Comparator<Map.Entry<Integer, Integer>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                        int compare =  Integer.compare(o1.getValue(), o2.getValue());
                        if (compare == 0) {
                            long ageLF = lookingFor.age;
                            long o1Age = allLovers.get(o1.getKey()).age;
                            long o2Age = allLovers.get(o2.getKey()).age;

                            int compareAge = Long.compare(Math.abs(ageLF - o2Age), Math.abs(ageLF - o1Age));
                            if (compareAge != 0) {
                                return compareAge;
                            }
                            return Integer.compare(o1.getKey(), o2.getKey());
                        }
                        return compare;
                    }
                });

                finalSet.addAll(counts.entrySet());

                Iterator<Map.Entry<Integer, Integer>> i = finalSet.descendingIterator();
                int count = 0;
                while (i.hasNext() && count < limit) {
                    Map.Entry<Integer, Integer> entry = i.next();
                    tmp.add(allLovers.get(entry.getKey()));
                    count++;
                }

                return tmp;*/
            } else {
                return EMPTY_lOVERS;
/*
                TreeSet<Lover> boringLovers = null;
                if (sex == 1) {
                    boringLovers = allFLoversBoringSet;
                } else {
                    boringLovers = allMLoversBoringSet;
                }

                int totalCount = 0;

                Lover higher = boringLovers.higher(lookingFor);
                Lover lower = boringLovers.lower(lookingFor);

                while(totalCount <= limit) {
                    if (higher != null) {
                        tmp.add(higher);
                        totalCount++;
                        higher = boringLovers.higher(higher);
                    }

                    if (lower != null) {
                        tmp.add(lower);
                        totalCount++;
                        lower = boringLovers.lower(lower);
                    }

                    if (higher == null && lower == null) {
                        break;
                    }
                }

                return tmp;*/
            }
        } catch (Exception e) {
            logger.error("error: ", e);
            return EMPTY_lOVERS;
        } finally {
            //logger.error("time findRecommendsForLocation: {}" , System.nanoTime() - start);
        }
    }



    static ThreadLocal<HashMap<Integer, Integer>> countLT = ThreadLocal.withInitial(HashMap::new);


    public static TreeSet<Lover> findRecommendsForAll(int lookingForId, int limit) {
        CALCULATED_PRIORITY_TL.get().clear();
        INTERESTS_INTERSECT_TL.get().clear();
        isBasicSortTL.set(false);

        Map<Integer, Integer> counts = countLT.get();
        counts.clear();

        try {
            int id = (lookingForId);

            //TODO temporary
            if (id + 1 > ArrayDataAllocator.MAX_ACCOUNTS) {
                return EMPTY_lOVERS;
            }

            Lover lookingFor = allLovers.get(id);
            lookingForTL.set(lookingFor);

            byte sex = ArrayDataAllocator.sexGlobal[id];
            boolean isLookingForBoring = (accountsInterests[id] == null);

            TreeSet<Lover> tmp = lTL.get();
            tmp.clear();

            if (!isLookingForBoring) {

                ShortArray lfInterests = ArrayDataAllocator.accountsInterests[id];

                int pairLength = 0;

                for (short intr: lfInterests.array) {
                    TreeSet<Lover> intrLovers = sex == 1 ? interestsPremiumFLoverSets.get(intr) : interestsPremiumMLoverSets.get(intr);
                    pairLength += intrLovers.size();
                }

                long[] lookingForMap = lookingFor.iMap;
                long[] intrAccA1 = new long[pairLength * 2];
                int[] accIds = new int[pairLength];

                int offset = 0;
                for (short intr: lfInterests.array) {

                    TreeSet<Lover> intrLovers = sex == 1 ? interestsPremiumFLoverSets.get(intr) : interestsPremiumMLoverSets.get(intr);
                    Iterator<Lover> itr = intrLovers.iterator();
                    while (itr.hasNext()) {
                        Lover next = itr.next();
                        intrAccA1[offset] = next.iMap[0];
                        intrAccA1[offset + 1] = next.iMap[1];
                        accIds[offset/2] = next.id;
                        offset += 2;
                    }
                }

                for (int i = 0; i < intrAccA1.length; i += 2) {
                    intrAccA1[i] = intrAccA1[i] & lookingForMap[0];
                    intrAccA1[i+1] = intrAccA1[i+1] & lookingForMap[1];
                    int count = Long.bitCount(intrAccA1[i]) + Long.bitCount(intrAccA1[i+1]);
                    //count = count << 24;
                    accIds[i/2] = accIds[i/2] | (int)(count << 24);
                }

                Arrays.sort(accIds);

                int toLimit = 0;
                for (int i = accIds.length - 1; i >= 0; i--) {
                    if (toLimit++ > limit * 100) {
                        break;
                    }
                    tmp.add(allLovers.get(accIds[i] & 0x00FFFFFF));
                }

                return tmp;
            } else {
                return EMPTY_lOVERS;
           }
        } catch (Exception e) {
            //logger.error("error: ", e);
            return EMPTY_lOVERS;
        } finally {
            //logger.error("time findRecommendsForLocation: {}" , System.nanoTime() - start);
        }
    }


    public static TreeSet<Lover> findRecommendsForLocation(int lookingForId, int limit, short city, short country) {
        CALCULATED_PRIORITY_TL.get().clear();
        INTERESTS_INTERSECT_TL.get().clear();

        isBasicSortTL.set(false);
        long start = System.nanoTime();

        try {
            TreeSet<Lover> lovers = lTL.get();
            lovers.clear();

            int id = (lookingForId);

            //TODO temporary
            if (id + 1 > ArrayDataAllocator.MAX_ACCOUNTS) {
                return EMPTY_lOVERS;
            }

            Lover lookingFor = allLovers.get(id);

            lookingForTL.set(lookingFor);

            byte sex = ArrayDataAllocator.sexGlobal[id];

            //no without interests
            if (ArrayDataAllocator.accountsInterests[id] == null || ArrayDataAllocator.accountsInterests[id].array.length == 0) {
                return EMPTY_lOVERS;
            }


            if (country != 0) {
                ShortArray lfInterests = ArrayDataAllocator.accountsInterests[id];
                for (short intr: lfInterests.array) {
                    TreeSet<Lover> intrLovers = sex == 1 ?
                            countryInterestsFLoverSets.get(country).get(intr) : countryInterestsMLoverSets.get(country).get(intr);
                    if (intrLovers == null) {
                        continue;
                    }
                    fillTmpLovers(intrLovers, id, lovers);
                }
            } else if (city != 0) {

                if (sex == 1) {
                    fillTmpLovers(cityFLoverSets.get(city), id, lovers);
                } else {
                    fillTmpLovers(cityMLoverSets.get(city), id, lovers);
                }
            } else {
                return EMPTY_lOVERS;
            }

            if (lovers.isEmpty()) {
                return EMPTY_lOVERS;
            }


            return lovers;
        } catch (Exception e) {
            //logger.error("error: ", e);
            return EMPTY_lOVERS;
        } finally {
            //logger.error("time findRecommendsForLocation: {}" , System.nanoTime() - start);
        }
    }

    public static TreeSet<Lover> processRecommends(int lookingForId, String city, String country, int limit) {
        isBasicSortTL.set(false);
        try {
            if (city == null && country == null) {
                return findRecommendsForAll(lookingForId, limit);
            } else {
                return findRecommendsForLocation(lookingForId, limit,
                        Dictionaries.cities.containsKey(city) ? Dictionaries.cities.get(city) : (short)0,
                        Dictionaries.countries.containsKey(country) ? Dictionaries.countries.get(country) : (short)0);
            }
        } catch (Exception e) {
            //logger.error("error: {}", e);
            return EMPTY_lOVERS;
        }
    }


    static ThreadLocal<long[]> iisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return new long[2];
        }
    };

    public static long getIntersectInterests(Lover lf, Lover wanted) {

        if (lf.iMap == null) {
            return -1;
        } else if (wanted.iMap == null) {
            return 0;
        }

        //countLT.get();
        long[] tmp = iisTL.get();
        System.arraycopy(lf.iMap, 0, tmp, 0, tmp.length);
        DirectMemoryAllocator.intersectArrays(tmp, wanted.iMap);
        return DirectMemoryAllocator.getBitsCounts(tmp);
    }

    public static int getIntersectInterests(int lf, int lover) {

        Map<Integer, Integer> isc = INTERESTS_INTERSECT_TL.get();

        if (isc.containsKey(lover)) {
            return isc.get(lover);
        } else {
            ShortArray a = ArrayDataAllocator.accountsInterests[lf];
            ShortArray b = ArrayDataAllocator.accountsInterests[lover];

            if (a == null && b == null) {
                isc.put(lover, 1);
                return 1;
            } else if (b == null) {
                isc.put(lover, 0);
                return 0;
            } else if (a == null) {
                isc.put(lover, 1);
                return 1;
            }
            int count = 0;
            //TODO optimize
            for (short interestA : a.array) {
                for (short interestB : b.array) {
                    if (interestA == interestB) {
                        count++;
                    }
                }
            }
            isc.put(lover, count);
            return count;
        }
    }

    public static long getStatusWeight(byte status) {
        if (status == 1) {
            return 3L;
        } else if (status == 2) {
            return 1L;
        } else if (status == 3){
            return 2L;
        } else {
            return 0L;
        }
    }

    public static class Lover implements Comparable<Lover> {

        public int id;
        public byte status;
        public long statusWeight;
        public int age;
        public int interests;
        public int interestsCount = 0;
        public long premiumWeight;
        public boolean isPremium;
        public short city;
        public short country;
        public byte sex;
        public long[] iMap = null;

        public boolean isItrCalculated = true;

        public long basePriority;

        public void clean() {
            id = 0;
            status = 0;
            statusWeight = 0;
            age = 0;
            interests = 0;
            interestsCount = 0;
            premiumWeight = 0;
            isPremium = false;
            basePriority = 0;
        }

        @Override
        public int compareTo(Lover o) {
            if (isBasicSortTL.get()) {
                if (id == o.id) {
                    return 0;
                } else {
                    int compare = Long.compare(o.basePriority, this.basePriority);
                    if (compare == 0) {
                        return Integer.compare(o.id, this.id);
                    } else {
                        return compare;
                    }
                }
            } else {
                if (id == o.id) {
                    return 0;
                } else {
                    int compare =  Long.compare(getAdvancedPriority(o), getAdvancedPriority(this));
                    if (compare == 0) {
                        return Integer.compare(o.id, this.id);
                    } else {
                        return compare;
                    }
                }
            }
        }
    }

     public static long getAdvancedPriority(Lover lover) {
        Map<Integer, Long> prMap =  CALCULATED_PRIORITY_TL.get();
        Lover lookingFor = lookingForTL.get();

        if (!prMap.containsKey(lover.id)) {
            long iIntersect = (long)getIntersectInterests(lookingFor, lover);
            long interestsWeight = iIntersect != -1 ? iIntersect * 10_000_000_000L : 0;

            int ageLF = lookingFor.age;
            int loverAge = lover.age;

            int diff1 = Math.abs(loverAge - ageLF);

            long ageWeight = 500_000_000 - (long)diff1;
            long priority = lover.statusWeight + lover.premiumWeight + interestsWeight + ageWeight;
            prMap.put(lover.id, priority);
            return priority;
        } else {
            return prMap.get(lover.id);
        }
    }

    public static long getBasePriority(Lover lover) {
        return lover.statusWeight + lover.premiumWeight;
    }

    public static boolean processNewLover(InternalAccount o) {
        isBasicSortTL.set(true);

        Lover l = new Lover();
        allLovers.put(o.id, l);

        l.id = o.id;
        l.age = o.birth;
        l.sex = o.sex;
        l.status = o.status;
        l.statusWeight = getStatusWeight(l.status) * 100_000_000_000L;
        l.premiumWeight = o.isPremiumNow ? 1000_000_000_000L : 0;
        l.isPremium = o.isPremiumNow;
        l.basePriority = getBasePriority(l);

        boolean isFunny = o.interestsNumber > 0;
        if (l.sex == 1) {
            if (isFunny) {
                allMLoversFunnySet.add(l);
            } else {
                allMLoversBoringSet.add(l);
            }
        } else {
            if (isFunny) {
                allFLoversFunnySet.add(l);
            } else {
                allFLoversBoringSet.add(l);
            }
        }


        if (o.city != Short.MAX_VALUE) {
            if (l.sex == 1) {
                if (!cityMLoverSets.containsKey(o.city)) {
                    cityMLoverSets.put(o.city, new TreeSet<Lover>());
                }
                //TODO merge
                cityMLoverSets.get(o.city).add(l);
            } else {
                if (!cityFLoverSets.containsKey(o.city)) {
                    cityFLoverSets.put(o.city, new TreeSet<Lover>());
                }
                //TODO merge
                cityFLoverSets.get(o.city).add(l);
            }
        }

        if (isFunny) {
            ShortArray interests = accountsInterests[o.id];
            if (interests != null) {
                fillInterestsMap(interests.array, l);
            }

            if (DirectMemoryAllocator.isBitSet(PremiumFilter.existPremiumBitMap, o.id)) {
                if (interests != null) {
                    if (l.sex == 1) {
                        fillLoversForInterests(l, interests, interestsPremiumMLoverSets);
                    } else {
                        fillLoversForInterests(l, interests, interestsPremiumFLoverSets);
                    }
                }
            }

            //TODO
            if (o.country != Short.MAX_VALUE) {
                if (l.sex == 1) {
                    fillLoversForInterests(l, interests, countryInterestsMLoverSets.get(o.country));
                } else {
                    fillLoversForInterests(l, interests, countryInterestsFLoverSets.get(o.country));
                }
            }

        }

        return true;
    }

    public static boolean processUpdateLover(InternalAccount o, short oldCity, short oldCountry, ShortArray oldItr, byte oldStatus, boolean wasPremium) {
        isBasicSortTL.set(true);

        Lover l = new Lover();

        Lover old = allLovers.put(o.id, l);

        short oldCC = Dictionaries.cities.get("Амстеролесск");

        boolean isPremiumNow = DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, o.id);
        l.id = o.id;
        l.age = o.birth;
        l.sex = o.sex;
        l.status = o.status;
        l.statusWeight = getStatusWeight(l.status) * 100_000_000_000L;
        l.premiumWeight = isPremiumNow ? 1000_000_000_000L : 0;
        l.isPremium = isPremiumNow;
        l.basePriority = getBasePriority(l);

        //TODO optimize
        allMLoversFunnySet.remove(old);
        allMLoversBoringSet.remove(old);
        allFLoversFunnySet.remove(old);
        allFLoversBoringSet.remove(old);

        boolean isFunny = accountsInterests[l.id] != null;
        if (l.sex == 1) {
            if (isFunny) {
                allMLoversFunnySet.add(l);
            } else {
                allMLoversBoringSet.add(l);
            }
        } else {
            if (isFunny) {
                allFLoversFunnySet.add(l);
            } else {
                allFLoversBoringSet.add(l);
            }
        }

        boolean removed = false;
        if (oldCity != Short.MAX_VALUE) {
            if (l.sex == 1) {
                removed = cityMLoverSets.get(oldCity).remove(old);
            } else {
                removed = cityFLoverSets.get(oldCity).remove(old);
            }
        }

        if (o.city != Short.MAX_VALUE) {
            if (l.sex == 1) {
                if (!cityMLoverSets.containsKey(o.city)) {
                    cityMLoverSets.put(o.city, new TreeSet<Lover>());
                }
                //TODO merge
                removed = cityMLoverSets.get(o.city).add(l);
            } else {
                if (!cityFLoverSets.containsKey(o.city)) {
                    cityFLoverSets.put(o.city, new TreeSet<Lover>());
                }
                //TODO merge
                removed = cityFLoverSets.get(o.city).add(l);
            }
        }

        if (isFunny) {
            ShortArray interests = accountsInterests[o.id];
            fillInterestsMap(interests.array, l);

            if (DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, o.id)) {
                if (l.sex == 1) {
                    updateLoversForInterests(l, old, oldItr, interests, interestsPremiumMLoverSets);
                } else {
                    updateLoversForInterests(l, old, oldItr, interests, interestsPremiumFLoverSets);
                }
            }

            if (o.country != oldCountry && oldItr != null && oldCountry != Short.MAX_VALUE) {
                Map<Short, TreeSet<Lover>> cntrItrLovers;
                if (l.sex == 1) {
                    cntrItrLovers = countryInterestsMLoverSets.get(oldCountry);
                } else {
                    cntrItrLovers = countryInterestsFLoverSets.get(oldCountry);
                }

                for (short itr: oldItr.array) {
                    cntrItrLovers.get(itr).remove(old);
                }
            }

            if (o.country != Short.MAX_VALUE) {
                if (l.sex == 1) {
                    updateLoversForInterests(l, old, oldItr, interests, countryInterestsMLoverSets.get(o.country));
                } else {
                    updateLoversForInterests(l, old, oldItr, interests, countryInterestsFLoverSets.get(o.country));
                }
            }

        }

        return true;
    }


    //three levels of deep
    public static void fillBaseInterestsTree(HashMap<Short, InterestsLeaf> tree, byte sex, boolean premium) {
        //first level
        Dictionaries.interests.values().forEach( itr -> {
            InterestsLeaf leaf = new InterestsLeaf();
            leaf.interestIds = new short[]{itr};
            leaf.affectedLovers = null;
            leaf.nexLevelLeafs = new HashMap<>();
            tree.put(itr, leaf);
        });

        //second level
        tree.values().forEach( floorLeaf -> {
            Dictionaries.interests.values().forEach( itr -> {
                if (floorLeaf.interestIds[0] != itr) {
                    InterestsLeaf leaf = new InterestsLeaf();
                    leaf.interestIds = new short[]{floorLeaf.interestIds[0], itr};
                    //Arrays.sort(leaf.interestIds);
                    leaf.affectedLovers = null;
                    leaf.nexLevelLeafs = new HashMap<>();
                    floorLeaf.nexLevelLeafs.put(itr, leaf);
                }
            });
        });

        //third level
        tree.values().forEach( floorLeaf -> {
            floorLeaf.nexLevelLeafs.values().forEach( secondLevelLeaf -> {
                Dictionaries.interests.values().forEach( itr -> {
                    if (floorLeaf.interestIds[0] != itr && secondLevelLeaf.interestIds[1] != itr) {
                        InterestsLeaf leaf = new InterestsLeaf();
                        leaf.interestIds = new short[]{secondLevelLeaf.interestIds[0], secondLevelLeaf.interestIds[1], itr};
                        //Arrays.sort(leaf.interestIds);
                        leaf.affectedLovers = null;
                        secondLevelLeaf.nexLevelLeafs.put(itr, leaf);
                    }
                });
            });
        });

        //create sets
        Map<String, TreeSet<Lover>> tmpMap = new HashMap<>();

        tree.values().forEach( floorLeaf -> {
            String key1 = itrToStr(floorLeaf.interestIds);
            tmpMap.putIfAbsent(key1, new TreeSet<Lover>(ageComparator));
            floorLeaf.affectedLovers = tmpMap.get(key1);

            floorLeaf.nexLevelLeafs.values().forEach( secondLevelLeaf -> {
                String key2 = itrToStr(secondLevelLeaf.interestIds);
                tmpMap.putIfAbsent(key2, new TreeSet<Lover>(ageComparator));
                secondLevelLeaf.affectedLovers = tmpMap.get(key2);

                secondLevelLeaf.nexLevelLeafs.values().forEach( thirdLevelLeaf -> {
                    String key3 = itrToStr(thirdLevelLeaf.interestIds);
                    tmpMap.putIfAbsent(key3, new TreeSet<Lover>(ageComparator));
                    thirdLevelLeaf.affectedLovers = tmpMap.get(key3);

                });
            });
        });

        //fill lovers lists
        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) { //TODO MAX_ACC_ID_ADDED
            Lover lover = allLovers.get(i);

            if (lover.sex != sex || !lover.isPremium ||
                    ArrayDataAllocator.accountsInterests[i] == null || lover.status != 1) {
                continue;
            }
            short[] iList = ArrayDataAllocator.accountsInterests[i].array;
            Arrays.sort(iList);

            short[][] iCombo = getAllConbinationsHard(iList); //getAllConbinations(iList);

            for (short[] sublist: iCombo) {
                for (short basic1 : sublist) {
                    tree.get(basic1).affectedLovers.add(lover);

                    for (short basic2 : sublist) {
                        if (basic2 == basic1) {
                            continue;
                        }
                        tree.get(basic1).nexLevelLeafs.get(basic2).affectedLovers.add(lover);

                        for (short basic3 : sublist) {
                            if (basic3 == basic1 || basic3 == basic2) {
                                continue;
                            }
                            tree.get(basic1).nexLevelLeafs.get(basic2).nexLevelLeafs.get(basic3).affectedLovers.add(lover);
                        }
                    }
                }
            }
        }
    }

    public static String itrToStr(short[] interests) {
        StringBuilder sb = new StringBuilder(30);
        Arrays.sort(interests);

        for (short s:interests) {
            sb.append(s).append(":");
        }
        return sb.toString();
    }

    public static void fillLoversForInterests(Lover l, ShortArray interests, Map<Short, TreeSet<Lover>> loversByInterests) {
        for (short intIdx: interests.array) {
            TreeSet<Lover> loversForI = loversByInterests.get(intIdx);
            if (loversForI == null) {
                loversForI = new TreeSet<Lover>(id_Comparator);
                loversByInterests.put(intIdx, loversForI);
            }
            loversForI.add(l);
        }
    }

    public static void updateLoversForInterests(Lover l, Lover old, ShortArray oldItr, ShortArray newItr, Map<Short, TreeSet<Lover>> loversByInterests) {

        if (oldItr != null) {
            for (short intIdx: oldItr.array) {
                TreeSet<Lover> loversForI = loversByInterests.get(intIdx);
                if (loversForI != null) {
                    loversForI.remove(old);
                }
            }
        }

        for (short intIdx: newItr.array) {
            TreeSet<Lover> loversForI = loversByInterests.computeIfAbsent(intIdx, k -> new TreeSet<Lover>(id_Comparator));
            loversForI.add(l);
        }
    }

    public static void fillLoversForInterests(TreeSet<Lover> lovers, Map<Short, TreeSet<Lover>> loversByInterests, boolean premiumOnly) {
        for (Lover l: lovers) {
            if (premiumOnly && !DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, l.id)) {
                continue;
            }
            ShortArray interests = ArrayDataAllocator.accountsInterests[l.id];
            if (interests != null) {
                fillLoversForInterests(l, interests, loversByInterests);
            }
        }
    }

    public static void removeLoverForInterests(Lover l, List<String> interests, Map<Short, TreeSet<Lover>> loversByInterests) {
        for (String interest: interests) {
            TreeSet<Lover> loversForI = loversByInterests.get(Dictionaries.interests.get(interest.trim()));
            if (loversForI == null) {
                loversForI = new TreeSet<Lover>();
                loversByInterests.put(Dictionaries.interests.get(interest.trim()), loversForI);
            }
            loversForI.remove(l);
        }
    }


    public static void fillTmpLovers(TreeSet<Lover> src, int lookingForId, TreeSet<Lover> dest) {
        Iterator<Lover> iterator = src.descendingIterator();
        while (iterator.hasNext()) {
            Lover next = iterator.next();
            if (next.id == lookingForId) {
                continue;
            }

            int interestsIntersect = (int)getIntersectInterests(allLovers.get(lookingForId), next);
            if (interestsIntersect != 0) {
                dest.add(next);
            }
        }
    }

    public static int factorial(int start, int deepLevel) {
        if (start <= deepLevel) {
            return start;
        } else {
            return start * factorial(start - 1, deepLevel);
        }
    }

    public static short[][] getAllConbinationsHard(short[] iArr) {
        int factorial = factorial(iArr.length, iArr.length - 3);

        //int variantsCount = iArr.length/3 + iArr.length%3; //iArr.length%3 == 0 ? iArr.length/3 : iArr.length/3 + 1;
        short[][] combinations = new short[factorial][];


        int index = 0;
        if (iArr.length > 3) {
            for (int i = 0; i < iArr.length; i++) {
                short first = iArr[i];
                for (int j = 0; j < iArr.length; j++) {
                    if (i == j) {
                        continue;
                    }
                    short second = iArr[j];
                    for (int k = 0; k < iArr.length; k++) {
                        if (k != i && k != j) {
                            short third = iArr[k];
                            combinations[index++] = new short[]{first, second, third};
                        }
                    }
                }
            }
        } else if (iArr.length == 2) {
            for (int i = 0; i < iArr.length; i++) {
                for (int j = 0; j < iArr.length; j++) {
                    if (i == j) {
                        continue;
                    }
                    combinations[index++] = new short[]{iArr[i], iArr[j]};
                }
            }
        } else {
            combinations[0] = new short[]{iArr[0], iArr[1]};
            combinations[1] = new short[]{iArr[1], iArr[0]};
        }
        return combinations;
    }

    public static short[][] getAllConbinations(short[] iArr) {

        if (iArr.length == 1) {
            short[][] combinations = new short[1][];
            combinations[0] = new short[]{iArr[0]};
            return combinations;
        } else if (iArr.length == 2) {
            short[][] combinations = new short[2][];
            combinations[0] = new short[]{iArr[0], iArr[1]};
            combinations[1] = new short[]{iArr[1], iArr[0]};
            return combinations;
        } else if (iArr.length == 3) {
            short[][] combinations = new short[3][];
            combinations[0] = new short[]{iArr[0], iArr[1], iArr[2]};
            combinations[1] = new short[]{iArr[2], iArr[0], iArr[1]};
            combinations[2] = new short[]{iArr[1], iArr[2], iArr[0]};
            return combinations;
        }

        int variantsCount = iArr.length/3 + iArr.length%3; //iArr.length%3 == 0 ? iArr.length/3 : iArr.length/3 + 1;
        short[][] combinations = new short[variantsCount][];


        int index = 0;
        for (int i = 0; i < variantsCount; i++) {
            combinations[index++] = new short[]{iArr[i], iArr[i+1], iArr[i+2]};
        }
        return combinations;
    }

    public static void lookForLoversByITree(HashMap<Short, InterestsLeaf> iTree, TreeSet<Lover> intrLovers, Lover lookingFor, int limit) {

        ShortArray lfInterests = ArrayDataAllocator.accountsInterests[lookingFor.id];
        Arrays.sort(lfInterests.array);

        short[] iArr = lfInterests.array;

        short[][] iCombo = getAllConbinations(iArr);

        int deepLevel = iArr.length > 3 ? 3 : iArr.length;
        InterestsLeaf[][] leafs = new InterestsLeaf[iCombo.length][];

        InterestsLeaf next = null;

        for (short j = 0; j <  iCombo.length; j++) {
            short[] comboPart = iCombo[j];

            next = iTree.get(comboPart[0]);
            leafs[j] = new InterestsLeaf[]{next, null, null};
            for (short i = 1; i < comboPart.length; i++) {
                short index = comboPart[i];
                if (index != 0 && next != null && next.nexLevelLeafs != null) {
                    next = next.nexLevelLeafs.get(index);
                    if (next != null) {
                        leafs[j][i] = next;
                    } else {
                        leafs[j][i] = null;
                    }
                } else {
                    leafs[j][i] = null;
                }
            }
        }

        for (int j = deepLevel - 1; j >= 0; j--) {

            for (int k = leafs.length - 1; k >=0; k--) {
                InterestsLeaf l = leafs[k][j];
                if (l != null && l.affectedLovers != null) {
                    intrLovers.addAll(l.affectedLovers);
                }
            }

            if (intrLovers.size() >= limit) {
                break;
            }
        }

    }

}
