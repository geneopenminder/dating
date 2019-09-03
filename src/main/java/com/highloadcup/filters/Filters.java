package com.highloadcup.filters;

import com.highloadcup.DirectMemoryAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;
import static com.highloadcup.ArrayDataAllocator.MAX_ACC_ID_ADDED;

public class Filters {

    private static final Logger logger = LogManager.getLogger(Filters.class);

    //countries: 70
    //cities: 606
    //fnames: 108
    //snames: 1638
    //interests: 90

    public static void initFilters() throws Exception {
        System.gc();
        ExecutorService e = Executors.newFixedThreadPool(4);
        logger.error("start init filters");
        BaseFilter[] filters = new BaseFilter[]{
                new EmailFilter(),
            new CityFilter(),
            new SNameFilter(),
            new FNameFilter(),
            new SexFilter(),
            new CountryBitMapFilter(),
            new StatusFilter(),
            new PhoneFilter(),
            new BirthFilter(),
            new InterestsBitMapFilter(),
            new LikesFilter(),
            new PremiumFilter(),
            new JoinedFilter()
        };

        for (BaseFilter f: filters) {
            e.execute(new Runnable() {
                @Override
                public void run() {
                    f.init();
                }
            });
        }
        /*
        new EmailFilter().init();
        new CityBitMapFilter().init();
        new SNameFilter().init();
        new FNameFilter().init();
        new SexFilter().init();
        new CountryBitMapFilter().init();
        new StatusFilter().init();
        new PhoneFilter().init();
        new BirthFilter().init();
        new InterestsBitMapFilter().init();
        new LikesFilter().init();
        new PremiumFilter().init();
        new JoinedFilter().init();
        //new InterestsFilter().init();
        */

        //logger.error("wait for init filters");
        e.shutdown();
        e.awaitTermination(10, TimeUnit.MINUTES);
        System.gc();
        logger.error("init filters finished");
    }

    final public static Set<String> COMPOUND_FILTER_PREDICATES = new HashSet<String>() {{
        add("birth_lt");
        add("birth_gt");
        add("email_lt");
        add("email_gt");
    }};

    final public static Map<String, BaseFilter> FILTERS_BY_PREDICATE = new HashMap<String, BaseFilter>() {{
        put("fna", new FNameFilter());
        put("sna", new SNameFilter());
        put("sex", new SexFilter());
        put("cou", new CountryBitMapFilter());
        put("cit", new CityFilter());
        put("ema", new EmailFilter());
        put("sta", new StatusFilter());
        put("pho", new PhoneFilter());
        put("bir", new BirthFilter());

        //put("int", new InterestsFilter());
        put("int", new InterestsBitMapFilter());

        put("joi", new JoinedFilter());
        put("lik", new LikesFilter());
        put("pre", new PremiumFilter());
        put("que", new EmptyFilter());
        put("lim", new EmptyFilter());
    }};

    final public static Map<Class, BaseFilter> FILTERS_BY_CLASS = new HashMap<Class, BaseFilter>() {{
        put(FNameFilter.class, new FNameFilter());
        put(SNameFilter.class, new SNameFilter());
        put(SexFilter.class, new SexFilter());
        put(CountryBitMapFilter.class, new CountryBitMapFilter());
        put(CityFilter.class, new CityFilter());
        put(EmailFilter.class, new EmailFilter());
        put(StatusFilter.class, new StatusFilter());
        put(PhoneFilter.class, new PhoneFilter());
        put(BirthFilter.class, new BirthFilter());

        put(InterestsBitMapFilter.class, new InterestsBitMapFilter());

        put(JoinedFilter.class, new JoinedFilter());
        put(LikesFilter.class, new LikesFilter());
        put(PremiumFilter.class, new PremiumFilter());
    }};

    public static long[] applyFilters(List<BaseFilter> filters) {
        long[] finalSet = null;

        if (filters == null || filters.isEmpty()) {
            return finalSet;
        }
        for (BaseFilter f: filters) {
            finalSet = f.getBitmapFilteredSet(finalSet);

            //boolean isSet = DirectMemoryAllocator.isBitSet(finalSet, 24373);

            if (finalSet == null) {
                return null;
            }
        }

        return finalSet;
    }

    public static boolean validateFilterParams(Iterable<Map.Entry<String, String>> params) {
        if (params == null || !params.iterator().hasNext()) {
            return false;
        }
        Iterator<Map.Entry<String, String>> i = params.iterator();
        while(i.hasNext()) {
            Map.Entry<String, String> e = i.next();
            if (e.getKey().length() < 3) {
                return false;
            }
            if (!FILTERS_BY_PREDICATE.containsKey(e.getKey().substring(0, 3))) {
                return false;
            }
        }
        return true;
    }

    public static boolean validateFilterParams(Map<String, String[]> params) {
        if (params == null || params.isEmpty()) {
            return false;
        }
        for (String key: params.keySet()) {
            if (key.length() < 3) {
                return false;
            }
            if (!FILTERS_BY_PREDICATE.containsKey(key.trim().toLowerCase().substring(0, 3))) {
                return false;
            }
        }

        return true;
    }

    public static final List<BaseFilter> noneFilters = new ArrayList<BaseFilter>();


    public static final List<BaseFilter> emptyResults = new ArrayList<BaseFilter>() {{
        add(new EmptyFilter());
    }};

    public static final List<BaseFilter> badFilter = new ArrayList<BaseFilter>() {{
        add(new BadFilter());
    }};

    public static List<BaseFilter> getFiltersByPriority(Iterable<Map.Entry<String, String>> params, int limit) {
        return getFiltersByPriority(params, limit, null);
    }

    static ThreadLocal<ArrayList<BaseFilter>> preFiltersTL = new ThreadLocal<ArrayList<BaseFilter>>() {
        @Override
        protected ArrayList<BaseFilter> initialValue() {
            return new ArrayList<BaseFilter>(12);
        }
    };

    static ThreadLocal<ArrayList<BaseFilter>> postFiltersTL = new ThreadLocal<ArrayList<BaseFilter>>() {
        @Override
        protected ArrayList<BaseFilter> initialValue() {
            return new ArrayList<BaseFilter>(12);
        }
    };

    static ThreadLocal<FullScanCompoundFilter> compFilterTL = new ThreadLocal<FullScanCompoundFilter>() {
        @Override
        protected FullScanCompoundFilter initialValue() {
            return new FullScanCompoundFilter();
        }
    };

    public static FullScanCompoundFilter getCompoundFilterTL() {
        return compFilterTL.get();
    }

    public static List<BaseFilter> getFiltersByPriority(Iterable<Map.Entry<String, String>> params, int limit, Set<String> excludeKeys) {
        final List<BaseFilter> filters = preFiltersTL.get();
        filters.clear();

        FullScanCompoundFilter compoundFilter = compFilterTL.get();
        compoundFilter.disableAndClear();

        Iterator<Map.Entry<String, String>> it  = params.iterator();
        while(it.hasNext()) {
            Map.Entry<String, String> next = it.next();
            final String predicate = next.getKey();
            if (excludeKeys != null && excludeKeys.contains(predicate)) {
                continue;
            }
            final String value = next.getValue();
            if (value == null || value.isEmpty()) {
                return badFilter;
            }
            String[] valHolder = new String[]{value};

            final BaseFilter filter = FILTERS_BY_PREDICATE.get(predicate.substring(0, 3));
            if (!filter.validatePredicateAndVal(predicate, valHolder)) {
                return badFilter;
            } else {
                if (!filter.getClass().equals(EmptyFilter.class)) {
                    BaseFilter cloned = filter.clone(predicate, valHolder, limit);
                    cloned.reset();
                    filters.add(cloned);

                    if (COMPOUND_FILTER_PREDICATES.contains(predicate)) {
                        compoundFilter.enabled = true;
                        compoundFilter.addPredicate(predicate, valHolder[0]);
                    }
                }
            }
        }

        if (filters.size() == 0) {
            return noneFilters;
        }

        for (BaseFilter f: filters) {
            f.calculatePriority();
            if (f.calculatedPriority == 0) {
                return emptyResults;
            }
        }

        final List<BaseFilter> postFilters = postFiltersTL.get();
        postFilters.clear();


        if (filters.size() > 1) {

            filters.forEach( f -> {
                if (f.getClass() != EmptyFilter.class) {
                    postFilters.add(f);
                }
            });

            postFilters.sort(new Comparator<BaseFilter>() {
                @Override
                public int compare(BaseFilter o1, BaseFilter o2) {
                    return Integer.compare(o1.calculatedPriority, o2.calculatedPriority);
                }
            });
            if (!compoundFilter.enabled) {
                postFilters.get(postFilters.size() - 1).isLast = true;
            }
            return postFilters;
        } else if (filters.size() == 1) {
            if (!compoundFilter.enabled) {
                filters.get(0).isSingle = true;
                filters.get(0).isLast = true;
            }
            return filters;
        }

        return emptyResults;
    }

    @Deprecated
    public static List<BaseFilter> getFiltersByPriority(Map<String, String[]> params, int limit) {
        final List<BaseFilter> filters = preFiltersTL.get();
        filters.clear();

        for (Map.Entry<String, String[]> entry: params.entrySet()) {
            final String predicate = entry.getKey();
            final String[] value = entry.getValue();
            if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
                return null;
            }
            final BaseFilter filter = FILTERS_BY_PREDICATE.get(predicate.trim().substring(0, 3));
            if (!filter.validatePredicateAndVal(predicate, value)) {
                return null;
            } else {
                BaseFilter cloned = filter.clone(predicate, value, limit);
                cloned.reset();
                filters.add(cloned);
            }
        }

        for (BaseFilter f: filters) {
            f.calculatePriority();
            if (f.calculatedPriority == 0) {
                return emptyResults;
            }
        }

        final List<BaseFilter> postFilters = postFiltersTL.get();
        postFilters.clear();


        if (filters.size() > 2) {

            filters.forEach( f -> {
                if (f.getClass() != EmptyFilter.class) {
                    postFilters.add(f);
                }
            });

            postFilters.sort(new Comparator<BaseFilter>() {
                @Override
                public int compare(BaseFilter o1, BaseFilter o2) {
                    return Integer.compare(o1.calculatedPriority, o2.calculatedPriority);
                }
            });
            postFilters.get(filters.size() - 1).isLast = true;
            return postFilters;
        } else if (filters.size() == 1) {
            filters.get(0).isSingle = true;
            filters.get(0).isLast = true;
            return filters;
        }
        return emptyResults;
    }

    static ThreadLocal<int[]> bpToSetTL = new ThreadLocal<int[]>() {
        @Override
        protected int[]initialValue() {
            return new int[64];
        }
    };


    public static int PRECALC_SKIP_ACC = 1000;

    public static int[] transformBitMapToSet(long[] finalBitSet, int limit) {


        if (finalBitSet == null) {
            return null;
        } else {
            int totalCount = DirectMemoryAllocator.getBitsCounts(finalBitSet);
            if (totalCount == 0) {
                return null;
            }
            int[] finalSet = bpToSetTL.get();
            DirectMemoryAllocator.clear(finalSet);

            int num = 0;

            //int precalBitCount = 0;
            //int nextSkip = ACCOUNTS_TOTAL.get();

            //int skipCount = 0;

            //boolean trySkip = true;

            for (int i = MAX_ACC_ID_ADDED.get(); i > 0; i--) {

                /*
                if (trySkip) {
                    if (i > PRECALC_SKIP_ACC) {
                        precalBitCount = DirectMemoryAllocator.getBitsCounts(finalBitSet, (i - PRECALC_SKIP_ACC) / 64, i/64);
                        if (precalBitCount == 0) {
                            i -= PRECALC_SKIP_ACC;
                            continue;
                        } else {
                            trySkip = false;
                        }
                    }
                }*/


                int dataIndex = i >>> 6; // x/5
                if (finalBitSet[dataIndex] == 0) {
                    int offset = i % 64;
                    i -= offset == 0 ? 63 : offset;
                    continue;
                }

                if (DirectMemoryAllocator.isBitSet(finalBitSet, i)) {
                    finalSet[num++] = i;
                    //precalBitCount--;
                    //if (precalBitCount == 0) {
                    //    trySkip = true;
                    //}
                }
                if (num >= limit || num >= totalCount) {
                    //logger.error("skip count - {}", skipCount);
                    return finalSet;
                }
            }
            //logger.error("skip count - {}", skipCount);
            return finalSet;
        }
    }

/*
    public static int[] transformBitMapToSet(long[] finalBitSet, int limit) {
        if (finalBitSet == null) {
            return null;
        } else {
            int[] finalSet = new int[limit];
            int num = 0;
            for (int i = 0; i < finalBitSet.length * 64; i++) {
                if (DirectMemoryAllocator.isBitSet(finalBitSet, i)) {
                    //String email = ArrayDataAllocator.emailsPlain[i];
                    finalSet[num++] = i;
                }
                if (num >= limit) {
                    return finalSet;
                }
            }
            return BaseFilter.realloc(finalSet, num);
        }
    }
    */

}
