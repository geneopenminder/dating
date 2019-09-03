package com.highloadcup.filters;

import com.highloadcup.*;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.MAX_ACC_ID_ADDED;

public class GroupBitmapsAggregator {

    private static final Logger logger = LogManager.getLogger(GroupBitmapsAggregator.class);

    //GET: /accounts/group/?birth=1998&limit=4&order=-1&keys=country
    /*
{"groups": [
    {"country": "Малатрис", "count": 8745},
    {"country": "Алания", "count": 4390},
    {"country": "Финляндия", "count": 2100},
    {"country": "Гератрис", "count": 547}
]}
    * */

    //birth - 8
    //joined - 8
    //status - 2
    //interests - 8



    final static AtomicInteger GN_ID = new AtomicInteger(1);

    public static class CountValue {
        int count;
        int from;
        int to;
    }

    public static class NodeFilters {
        Map<Short, Integer> cityMap = new HashMap<>();
        Map<Short, Integer> countryMap = new HashMap<>();
        Map<Short, Integer> joinedMap = new HashMap<>();
        Map<Short, Integer> birthMap = new HashMap<>();
        Map<Short, Integer> interestMap = new HashMap<>();
        Map<Byte, Integer> statusMap = new HashMap<>();
        int sexM;
        int sexF;
    }

    public static class GroupNode implements Comparable<GroupNode> {

        public int uniqId = GN_ID.incrementAndGet();

        public String[] keys;
        public short[] keysIds;
        public int count;

        //public NodeFilters filters = new NodeFilters();
        //public int[] accounts;

        public GroupNode(String[] keys, short[] keysIds, int count) {
            this.keys = keys;
            this.keysIds = keysIds;
            this.count = count;
        }

        @Override
        public int compareTo(GroupNode o2) {
            int countCompare = Integer.compare(this.count, o2.count);
            if (countCompare != 0) {
                return countCompare;
            } else {
                if (this.keys[0] != null && o2.keys[0] != null) {
                    int keyFirstCompare =  this.keys[0].compareTo(o2.keys[0]);
                    if (keyFirstCompare == 0) {
                        if (this.keys.length > 1 && this.keys[1] != null && o2.keys.length > 1 && o2.keys[1] != null) {
                            return this.keys[1].compareTo(o2.keys[1]);
                        }
                    }
                    return keyFirstCompare;
                } else if (this.keys[0] == null) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }

    }

    public static class GroupRoot {

        Map<String, Map<String, Map<Integer, Integer>>> groupRoot;

    }


    final static  TreeSet<GroupNode> emptyG = new TreeSet<>();

    //common without filters ****************************************************
    public static HashMap<String, TreeSet<GroupNode>> groupsByKeys = new HashMap<>();

    //1 male
    public static TreeSet<GroupNode> sexGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> statusGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> countryGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> cityGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> interestsGroups = new TreeSet<GroupNode>();

    public static TreeSet<GroupNode> countryStatusGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> countrySexGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> cityStatusGroups = new TreeSet<GroupNode>();
    public static TreeSet<GroupNode> citySexGroups = new TreeSet<GroupNode>();
    //************************************************************************************

    public static Map<String, Map<Integer, GroupNode>> groupNodesByKeys = new HashMap<>();

    public static Map<Integer, GroupNode> GROUPS_BY_UNIQ = new HashMap<>();

    public static GroupNode[] GROUPS_BY_UNIQ_ARRAY;

    //query_id=1048&likes=22783&keys=interests&limit=25&order=1
    public final static Set<String> GROUP_EXCLUDE_KEYS = new HashSet<String>() {{add("query_id"); add("order"); add("keys"); add("limit");}};

    final static TreeSet<GroupNode> EMPTY_SET = new TreeSet<>();

    public static void init() throws Exception {

        fillGroupIndex();
        initBaseGroups();

        GROUPS_BY_UNIQ_ARRAY = new GroupNode[G_KEY_BATCH*10];

        createIndexKeys();
        warmUp();
    }

    /*
    [ERROR] 2019-03-02 11:51:30.286 [main] App - countries: 70
    [ERROR] 2019-03-02 11:51:30.286 [main] App - cities: 611
    [ERROR] 2019-03-02 11:51:30.286 [main] App - email domains: 13
    [ERROR] 2019-03-02 11:51:30.286 [main] App - fnames: 108
    [ERROR] 2019-03-02 11:51:30.286 [main] App - snames: 1638
    [ERROR] 2019-03-02 11:51:30.286 [main] App - interests: 90
    * */

    public static int[] GROUP_IDX = new int[MAX_ACCOUNTS];

    public static short YEAR_OFFSET = 1960;

    public static void fillGroupIndex() {

        long start = System.nanoTime();
        //birth - 6 bit
        //joined - 6 bit
        //sex - 1 bit
        //status - 2 bit
        //city - 10 bit
        //country - 7 bit

        for (int i = 1; i <= ArrayDataAllocator.MAX_ACC_ID_ADDED.get(); i++) {
            int idx = 0;

            short city = ArrayDataAllocator.cityGlobal[i];
            if (city == Short.MAX_VALUE) {
                city = 0;
            }
            short country = ArrayDataAllocator.countryGlobal[i];
            if (country == Short.MAX_VALUE) {
                country = 0;
            }
            byte sex = ArrayDataAllocator.sexGlobal[i];
            short joined = (short)(JoinedFilter.getJoinYear(ArrayDataAllocator.joined[i]) - YEAR_OFFSET);
            short birth = (short)(BirthFilter.getYear(DirectMemoryAllocator.getBirthDateFromFS(i)) - YEAR_OFFSET);

            byte status = ArrayDataAllocator.statusGlobal[i];

            idx = idx | (int) city;
            idx = idx | (((int)country & 0x7F) << 10 );
            idx = idx | (((int)joined & 0x3F) << 17 );
            idx = idx | (((int)birth & 0x3F) << 23 );
            idx = idx | (((int)sex & 0x1) << 29 );
            idx = idx | (((int)status & 0x3) << 30 );


            GROUP_IDX[i] = idx;

            short _city = (short)(idx & 0x3FF);
            short _country = (short)((idx >>> 10) & 0x7F);
            short _joined = (short)((idx >>> 17) & 0x3F);
            short _birth = (short)((idx >>> 23) & 0x3F);
            byte _sex = (byte)((idx >>> 29) & 0x1);
            byte _status = (byte)((idx >>> 30) & 0x3);
            //_joined += YEAR_OFFSET;
            //_birth += YEAR_OFFSET;


            if (city != _city
                    || country != _country
            || joined != _joined
            || birth != _birth
            || sex != _sex
            || status != _status) {
                logger.error("wrong bitmaps");
            }

        }

        logger.error("fillGroupIndex end; time - {}" , System.nanoTime() - start);

    }


    //            Iterator<GroupBitmapsAggregator.GroupNode> iterator = direction > 0 ? groups.iterator() : groups.descendingIterator();

    public static TreeSet<GroupNode> getLimitedList(TreeSet<GroupNode> all) {
        TreeSet<GroupNode> newSet = new TreeSet<>();

        Iterator<GroupNode> ascIterator = all.iterator();
        Iterator<GroupNode> descIterator = all.descendingIterator();

        int limit = all.size() < 50 ? all.size() : 50;

        for (int i = 0; i < limit; i++) {
            newSet.add(ascIterator.next());
            newSet.add(descIterator.next());
        }

        return newSet;
    }

    public static void initBaseGroups() {

        final long[] tmpMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        {
            //sex
            final GroupNode mGroupNode = new GroupNode(new String[]{"m"}, new short[]{1}, SexFilter.totalM);
            sexGroups.add(mGroupNode);

            final GroupNode fGroupNode = new GroupNode(new String[]{"f"}, new short[]{0}, SexFilter.totalF);
            sexGroups.add(fGroupNode);

            groupsByKeys.put("sex", sexGroups);
        }

        {
            //status
            StatusFilter.countsByStatuses.forEach((key, value) -> {
                if (value != 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{InternalAccount.getStatus(key)}, new short[]{key}, value);
                    statusGroups.add(groupNode);
                }
            });
            groupsByKeys.put("status", statusGroups);
        }

        {
            CountryBitMapFilter.countByCountry.forEach((key, value) -> {
                if (value != 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.countriesById.get(key)},
                            new short[]{key}, value);
                    countryGroups.add(groupNode);
                }
            });

            int bitCountNullCountry = DirectMemoryAllocator.getBitsCounts(CountryBitMapFilter.nullBitMap);
            if (bitCountNullCountry != 0) {
                final GroupNode groupNode = new GroupNode(new String[]{null}, new short[]{0}, bitCountNullCountry);
                countryGroups.add(groupNode);
            }

            groupsByKeys.put("country", countryGroups);
        }

        {
            CityFilter.countByCity.forEach((key, value) -> {
                if (value != 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(key)}, new short[]{key}, value);
                    long[] set = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    DirectMemoryAllocator.fillSet(set, CityFilter.cityAccounts.get(key).array);
                    cityGroups.add(groupNode);
                }
            });

            int bitCountNullCity = DirectMemoryAllocator.getBitsCounts(CityFilter.nullCitiesBitMap);
            if (bitCountNullCity != 0) {
                final GroupNode groupNode = new GroupNode(new String[]{null}, new short[]{0}, bitCountNullCity);
                cityGroups.add(groupNode);
            }

            groupsByKeys.put("city", cityGroups);
        }

        {
            InterestsBitMapFilter.countByInterests.forEach((key, value) -> {
                if (value != 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.interestsById.get(key)}, new short[]{key}, value);
                    interestsGroups.add(groupNode);
                }
            });
            groupsByKeys.put("interests", interestsGroups);
        }

        long[] tmp = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        {
            //country,status
            StatusFilter.countsByStatuses.forEach((status, sCount) -> {
                CountryBitMapFilter.countByCountry.forEach((country, cCount) -> {
                    System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
                    DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));
                    int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    //if (bitCount > 0) {
                        final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.countriesById.get(country), InternalAccount.getStatus(status)},
                                new short[]{country, status}, bitCount);
                        countryStatusGroups.add(groupNode);
                    //}
                });

                long[] nullCountry = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                System.arraycopy(CountryBitMapFilter.nullBitMap, 0, nullCountry, 0, nullCountry.length);
                DirectMemoryAllocator.intersectArrays(nullCountry, StatusFilter.bitmapsByStatuses.get(status));

                int bitCount = DirectMemoryAllocator.getBitsCounts(nullCountry);
                //if (bitCount > 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{null, InternalAccount.getStatus(status)},
                            new short[]{0, status}, bitCount);
                    countryStatusGroups.add(groupNode);
                //}
            });
            groupsByKeys.put("country,status", countryStatusGroups);
        }

        {
            //city,status
            StatusFilter.countsByStatuses.forEach((status, sCount) -> {
                CityFilter.countByCity.forEach((city, cCount) -> {
                    DirectMemoryAllocator.clear(tmp);
                    DirectMemoryAllocator.fillSet(tmp, CityFilter.cityAccounts.get(city).array);
                    DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));
                    int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    //if (bitCount > 0) {
                        final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(city), InternalAccount.getStatus(status)},
                                new short[]{city, status}, bitCount);
                        cityStatusGroups.add(groupNode);
                    //}
                });

                long[] nullCity = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                System.arraycopy(CityFilter.nullCitiesBitMap, 0, nullCity, 0, nullCity.length);
                DirectMemoryAllocator.intersectArrays(nullCity, StatusFilter.bitmapsByStatuses.get(status));

                int bitCount = DirectMemoryAllocator.getBitsCounts(nullCity);
                //if (bitCount > 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{null, InternalAccount.getStatus(status)},
                            new short[]{0, status}, bitCount);
                    cityStatusGroups.add(groupNode);
                //}
            });
            groupsByKeys.put("city,status", cityStatusGroups);
        }

        {
            //country,sex
            CountryBitMapFilter.countByCountry.forEach((country, cCount) -> {
                System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
                DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
                int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                //if (bitCount > 0) {
                    final GroupNode groupNodeM = new GroupNode(new String[]{Dictionaries.countriesById.get(country), "m"},
                            new short[]{country, 1}, bitCount);
                    countrySexGroups.add(groupNodeM);
                //}

                System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
                DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
                bitCount = DirectMemoryAllocator.getBitsCounts(tmp);

             //   if (bitCount > 0) {
                    final GroupNode groupNodeF = new GroupNode(new String[]{Dictionaries.countriesById.get(country), "f"},
                            new short[]{country, 0}, bitCount);
                    countrySexGroups.add(groupNodeF);
               // }
            });


            long[] nullCountry = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            System.arraycopy(CountryBitMapFilter.nullBitMap, 0, nullCountry, 0, nullCountry.length);
            DirectMemoryAllocator.intersectArrays(nullCountry, SexFilter.maleBitMap);
            final GroupNode mGroupNode = new GroupNode(new String[]{null, "m"},
                    new short[]{0, 1}, DirectMemoryAllocator.getBitsCounts(nullCountry));
            countrySexGroups.add(mGroupNode);

            nullCountry = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            System.arraycopy(CountryBitMapFilter.nullBitMap, 0, nullCountry, 0, nullCountry.length);
            DirectMemoryAllocator.intersectArrays(nullCountry, SexFilter.femaleBitMap);
            final GroupNode fGroupNode = new GroupNode(new String[]{null, "f"},
                    new short[]{0, 0}, DirectMemoryAllocator.getBitsCounts(nullCountry));
            countrySexGroups.add(fGroupNode);

            groupsByKeys.put("country,sex", countrySexGroups);
        }

        {
            //city,sex
            CityFilter.countByCity.forEach((city, cCount) -> {
                DirectMemoryAllocator.clear(tmp);
                DirectMemoryAllocator.fillSet(tmp, CityFilter.cityAccounts.get(city).array);
                DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
                int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
          //      if (bitCount > 0) {
                    final GroupNode mGroupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(city), "m"},
                            new short[]{city, 1}, bitCount);
                    citySexGroups.add(mGroupNode);
             //   }

                DirectMemoryAllocator.clear(tmp);
                DirectMemoryAllocator.fillSet(tmp, CityFilter.cityAccounts.get(city).array);
                DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
                bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
             //   if (bitCount > 0) {
                    final GroupNode fGroupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(city), "f"},
                            new short[]{city, 0}, bitCount);
                    citySexGroups.add(fGroupNode);
                //}
            });

            long[] nullCity = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            System.arraycopy(CityFilter.nullCitiesBitMap, 0, nullCity, 0, nullCity.length);
            DirectMemoryAllocator.intersectArrays(nullCity, SexFilter.femaleBitMap);
            final GroupNode fGroupNode = new GroupNode(new String[]{null, "f"},
                    new short[]{0, 0}, DirectMemoryAllocator.getBitsCounts(nullCity));
            citySexGroups.add(fGroupNode);

            nullCity = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            System.arraycopy(CityFilter.nullCitiesBitMap, 0, nullCity, 0, nullCity.length);
            DirectMemoryAllocator.intersectArrays(nullCity, SexFilter.maleBitMap);
            final GroupNode mGroupNode = new GroupNode(new String[]{null, "m"},
                    new short[]{0, 1}, DirectMemoryAllocator.getBitsCounts(nullCity));
            citySexGroups.add(mGroupNode);

            groupsByKeys.put("city,sex", citySexGroups);
        }
    }

    public static void warmUp() {
        final TreeSet<GroupNode> result = resultTL.get();
        result.clear();

        final Map<Integer, Integer> groupCountMaps = nodeCountsfTL.get();
        groupCountMaps.clear();

        final Map<Integer, GroupNode> resultGroups = gTL.get();
        resultGroups.clear();

        long[] bsss = JoinedFilter.yearBitMaps.get((short)2013);

        long[] bs = new long[bsss.length];

        System.arraycopy(bsss, 0, bs, 0, bs.length);

        DirectMemoryAllocator.intersectArrays(bs, SexFilter.femaleBitMap);


        int count = 1000;

        for (int i = 0; i < count * 30; i++) {
            getGroupsForBitSet("city", bs, result, groupCountMaps);
        }

        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            getGroupsForBitSet("city", bs, result, groupCountMaps);
        }

        long end = System.nanoTime();

        logger.error("time - {}; req time - {}", end - start, (end - start) / count);


    }

    public static final int G_KEY_BATCH = 4000;

    public static GroupNode[] G_KEY_GROUPS = new GroupNode[G_KEY_BATCH*10];

    public static void createIndexKeys() {
        groupsByKeys.forEach( (key, value) -> {
            Map<Integer, GroupNode> nodesMap = new HashMap<>();
            groupNodesByKeys.put(key, nodesMap);

            final int keyType = getGKeyType(key);
            final int offset = getGkeyOffset(keyType);

            value.forEach( g -> {
                short[] keys = g.keysIds;

                if (keys.length == 1) {
                    int id = keys[0];
                    nodesMap.put(id, g);
                    g.uniqId = offset + keys[0];
                    G_KEY_GROUPS[g.uniqId] = g;
                } else {
                    int id = (int)keys[0] << 16;
                    id = id | (int)keys[1];
                    nodesMap.put(id, g);
                    int idx = keys[1]*1000 + keys[0];
                    g.uniqId = offset + idx;
                    G_KEY_GROUPS[g.uniqId] = g;
                }

                GROUPS_BY_UNIQ.put(g.uniqId, g);
                GROUPS_BY_UNIQ_ARRAY[g.uniqId] = g;
            });

        });
    }

    public static void processNewCity(short cityIdx) {

        {
            final GroupNode g = new GroupNode(new String[]{Dictionaries.citiesById.get(cityIdx)}, new short[]{cityIdx}, CityFilter.countByCity.get(cityIdx));
            cityGroups.add(g);
        }

        {
            StatusFilter.countsByStatuses.forEach((status, sCount) -> {
                //DirectMemoryAllocator.clear(tmp);
                //DirectMemoryAllocator.fillSet(tmp, CityFilter.cityAccounts.get(city).array);
                //DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));
                //int bitCount = 1; //DirectMemoryAllocator.getBitsCounts(tmp);
                //if (bitCount > 0) {
                    final GroupNode groupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(cityIdx), InternalAccount.getStatus(status)},
                            new short[]{cityIdx, status}, 1);

                        int id = (int)groupNode.keysIds[0] << 16;
                        id = id | (int)groupNode.keysIds[1];
                        //logger.error("city - {}, city gr id - {}", cityIdx, id);

                //   fillFiltersCounts(tmpMap, tmp, groupNode, false);
                    //  groupNode.accounts = DirectMemoryAllocator.transformBitMapToSetFull(tmp);
                    cityStatusGroups.add(groupNode);
                //}
            });
        }

        {
            final GroupNode mGroupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(cityIdx), "m"},
                    new short[]{cityIdx, 1}, 1);
            citySexGroups.add(mGroupNode);

            final GroupNode fGroupNode = new GroupNode(new String[]{Dictionaries.citiesById.get(cityIdx), "f"},
                    new short[]{cityIdx, 0}, 1);
            citySexGroups.add(fGroupNode);
        }

    }

    public static void newCountry(short countryIdx) {
        final String country = Dictionaries.countriesById.get(countryIdx);
        {
            final GroupNode groupNode = new GroupNode(new String[]{country},
                    new short[]{countryIdx}, 1);
            countryGroups.add(groupNode);
        }
        {
            StatusFilter.countsByStatuses.forEach((status, sCount) -> {
                final GroupNode groupNode = new GroupNode(new String[]{country, InternalAccount.getStatus(status)},
                        new short[]{countryIdx, status}, 1);
                countryStatusGroups.add(groupNode);
            });
        }
        {
            final GroupNode mGroupNode = new GroupNode(new String[]{country, "m"},
                    new short[]{countryIdx, 1}, 1);
            countrySexGroups.add(mGroupNode);

            final GroupNode groupNode = new GroupNode(new String[]{country, "f"},
                    new short[]{countryIdx, 0}, 1);
            countrySexGroups.add(groupNode);
        }
    }

    static ThreadLocal<long[]> bitmapThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    public static ThreadLocal<Map<Integer, Integer>> nodeCountsfTL = ThreadLocal.withInitial(TreeMap::new);

    static ThreadLocal<TreeSet<GroupNode>> resultTL = ThreadLocal.withInitial(new Supplier<TreeSet<GroupNode>>() {
        @Override
        public TreeSet<GroupNode> get() {
            return new TreeSet<GroupNode>(new Comparator<GroupNode>() {
                @Override
                public int compare(GroupNode o1, GroupNode o2) {
                    Map<Integer, Integer> numbers = nodeCountsfTL.get();

                    if (o1.uniqId == o2.uniqId) {
                        return 0;
                    }

                    int o1Count = numbers.get(o1.uniqId);
                    int o2Count = numbers.get(o2.uniqId);

                    int countCompare = Integer.compare(o1Count, o2Count);
                    if (countCompare != 0) {
                        return countCompare;
                    } else {

                        int o1KeysCount = 0;
                        int o2KeysCount = 0;
                        o1KeysCount += o1.keys[0] == null ? 0 : 1;
                        o2KeysCount += o2.keys[0] == null ? 0 : 1;

                        if (o1.keys.length > 1) {
                            o1KeysCount += o1.keys[1] == null ? 0 : 1;
                            o2KeysCount += o2.keys[1] == null ? 0 : 1;
                        }


                        if (o1.keys[0] != null && o2.keys[0] != null) {
                            int keyFirstCompare = o1.keys[0].compareTo(o2.keys[0]);
                            if (keyFirstCompare == 0) {
                                if (o1.keys.length > 1 && o1.keys[1] != null && o2.keys[1] != null) {
                                    return o1.keys[1].compareTo(o2.keys[1]);
                                }
                            }
                            return keyFirstCompare;
                        } else if (o1.keys.length > 1 && o1.keys[0] == null && o2.keys[0] == null
                                && o1.keys[1] != null && o2.keys[1] != null) {
                                return o1.keys[1].compareTo(o2.keys[1]);
                        } else if (o1.keys[0] == null) {
                            return -1; //direction == 1 ? 1 : -1;
                        } else {
                            return 1; //direction == 1 ? -1 : 1;
                        }
                    }
                }
            });
        }
    });

    static ThreadLocal<List<BaseFilter>> filtersTL = ThreadLocal.withInitial(new Supplier<List<BaseFilter>>() {
        @Override
        public List<BaseFilter> get() {
            return new ArrayList<BaseFilter>();
        }
    });

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<long[]> fTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static final List<BaseFilter> EMPTY_FILTERS = new ArrayList<>(0);

    static ThreadLocal<int[]> accountsTL = ThreadLocal.withInitial(new Supplier<int[]>() {
        @Override
        public int[] get() {
            return new int[MAX_ACCOUNTS];
        }
    });


    static ThreadLocal<Map<Integer, GroupNode>> gTL = new ThreadLocal<Map<Integer, GroupNode>>() {
        @Override
        protected Map<Integer, GroupNode> initialValue() {
            return new HashMap<Integer, GroupNode>();
        }
    };

    public static void getGroupsForAccounts(String keys, int[] accounts, int length, TreeSet<GroupNode> result, Map<Integer, Integer> groupCountsMap) {

        final Map<Integer, GroupNode> groups = groupNodesByKeys.get(keys);
        final Map<Integer, GroupNode> resultGroups = gTL.get();

        final int gKeyType = getGKeyType(keys);
        final int keysOffset = getGkeyOffset(gKeyType);

        for (int i = 0; i < length; i++) {
            int id = accounts[i];

            if (id == Integer.MAX_VALUE) {
                continue;
            }

            if (keys.equalsIgnoreCase("interests")) {
                ArrayDataAllocator.ShortArray a = ArrayDataAllocator.accountsInterests[id];

                if (a != null) {
                    for (short itrIdL : a.array) {
                        int gKey = (int) itrIdL;
                        GroupNode g = groups.get(gKey);
                        groupCountsMap.merge(g.uniqId, 1, Integer::sum);
                        resultGroups.putIfAbsent(g.uniqId, g);
                    }
                }
            } else {
                byte sex = ArrayDataAllocator.sexGlobal[id];
                int acc = GROUP_IDX[id];
                int gKey = getGkey(gKeyType, acc);
                if (gKeyType == 4 & gKey == 0) {
                    gKey = Short.MAX_VALUE;
                }

                GroupNode g = G_KEY_GROUPS[keysOffset + gKey];
                groupCountsMap.merge(g.uniqId, 1, Integer::sum);
                resultGroups.putIfAbsent(g.uniqId, g);
            }
        }

        //groupCountsMap.keySet().forEach( key -> {
        //    result.add(resultGroups.get(key));
        //});

    }

    public static abstract class GKeyBaseProvider {
        public abstract int getGKey(int acc);

    }

    public static class GKeySexProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            byte sex = (byte)((acc >>> 29) & 0x1);
            return (int) sex;
        }
    }

    public static class GKeyStatusProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            byte status = (byte)((acc >>> 30) & 0x3);
            return (int) status;
        }
    }

    public static class GKeyCountryProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short country = (short)((acc >>> 10) & 0x7F);
            return (int)country;
        }
    }

    public static class GKeyCityProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short city = (short)(acc & 0x3FF);
            return (int)city;
        }
    }

    public static class GKeyCountryStatusProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short country = (short)((acc >>> 10) & 0x7F);
            byte status = (byte)((acc >>> 30) & 0x3);
            int gKey = (int)country << 16;
            return status*1000 + country;
            //return gKey | (int)status;
        }
    }

    public static class GKeyCityStatusProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short city = (short)(acc & 0x3FF);
            byte status = (byte)((acc >>> 30) & 0x3);
            int gKey = (int)city << 16;
            return status*1000 + city;
            //return gKey | (int)status;
        }
    }

    public static class GKeyCountrySexProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short country = (short)((acc >>> 10) & 0x7F);
            byte sex = (byte)((acc >>> 29) & 0x1);
            int gKey = (int)country << 16;
            return sex*1000 + country;
            //return gKey | (int)sex;
        }
    }

    public static class GKeyCitySexProvider extends GKeyBaseProvider {
        @Override
        public int getGKey(int acc) {
            short city = (short)(acc & 0x3FF);
            byte sex = (byte)((acc >>> 29) & 0x1);
            int gKey = (int)city << 16;
            return sex*1000 + city;
            //return gKey | (int)sex;
        }
    }

    public static Map<Integer, GKeyBaseProvider> G_KEY_PROVIDERS = new HashMap<>() {
        {
            put(1, new GKeySexProvider());
            put(2, new GKeyStatusProvider());
            put(3, new GKeyCountryProvider());
            put(4, new GKeyCityProvider());
            put(5, new GKeyCountryStatusProvider());
            put(6, new GKeyCityStatusProvider());
            put(7, new GKeyCountrySexProvider());
            put(8, new GKeyCitySexProvider());
        }
    };

    public static int getGKeyType(String keys) {
        if (keys.equalsIgnoreCase("sex")) {
            return 1;
        } else if (keys.equalsIgnoreCase("status")) {
            return 2;
        } else if (keys.equalsIgnoreCase("country")) {
            return 3;
        } else if (keys.equalsIgnoreCase("city")) {
            return 4;
        } else if (keys.equalsIgnoreCase("country,status")) {
            return 5;
        } else if (keys.equalsIgnoreCase("city,status")) {
            return 6;
        } else if (keys.equalsIgnoreCase("country,sex")) {
            return 7;
        } else if (keys.equalsIgnoreCase("city,sex")) {
            return 8;
        }
        return 0;
    }


    public static int getGkeyOffset(int keyType) {
        return keyType * G_KEY_BATCH;
    }

    public static int getGkey(int keyType, int acc) {
        int gKey = -1;

        if (keyType == 1) { //sex
            byte sex = (byte)((acc >>> 29) & 0x1);
            gKey = (int) sex;
        } else if (keyType == 2) { //keys.equalsIgnoreCase("status")) {
            byte status = (byte)((acc >>> 30) & 0x3);
            gKey = (int) status;
        } else if (keyType == 3) { //keys.equalsIgnoreCase("country")) {
            short country = (short)((acc >>> 10) & 0x7F);
            gKey = (int)country;
        } else if (keyType == 4) { //keys.equalsIgnoreCase("city")) {
            short city = (short)(acc & 0x3FF);
            gKey = (int)city;
        } else if (keyType == 5) { //keys.equalsIgnoreCase("country,status")) {
            short country = (short)((acc >>> 10) & 0x7F);
            byte status = (byte)((acc >>> 30) & 0x3);
            gKey = status * 1000 + country;
        } else if (keyType == 6) { //keys.equalsIgnoreCase("city,status")) {
            short city = (short)(acc & 0x3FF);
            byte status = (byte)((acc >>> 30) & 0x3);
            gKey = status * 1000 + city;
        } else if (keyType == 7) { //keys.equalsIgnoreCase("country,sex")) {
            short country = (short)((acc >>> 10) & 0x7F);
            byte sex = (byte)((acc >>> 29) & 0x1);
            gKey = sex * 1000 + country;
        } else if (keyType == 8) { //keys.equalsIgnoreCase("city,sex")) {
            short city = (short)(acc & 0x3FF);
            byte sex = (byte)((acc >>> 29) & 0x1);
            gKey = sex * 1000 + city;
        }
        return gKey;
    }

    static ThreadLocal<int[]> countsTL = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[G_KEY_BATCH*10];
        }
    };

    public static void getGroupsForBitSet(String keys, long[] bitSet,
                                           TreeSet<GroupNode> result, Map<Integer, Integer> groupCountMaps) {
        int[] counts = countsTL.get();
        DirectMemoryAllocator.clear(counts);

        final Map<Integer, GroupNode> groups = groupNodesByKeys.get(keys);

        //final Map<Integer, GroupNode> resultGroups = gTL.get();
        //resultGroups.clear();

        final int limit = MAX_ACC_ID_ADDED.get() / 64 + 1;

        final int keyType = getGKeyType(keys);
        final int keysOffset = getGkeyOffset(keyType);
        final GKeyBaseProvider gKeyProvider = G_KEY_PROVIDERS.get(keyType);

        //final int totalBits = DirectMemoryAllocator.getBitsCounts(bitSet);

        int foundBits = 0;

        //int[] foundKeys = new int[1000];

        boolean isInterests = false;
        if (keys.equalsIgnoreCase("interests")) {
            isInterests = true;
        }

        for (int i = 0; i < limit ; i++) {
            long data = bitSet[i];
            if (data == 0) {
                continue;
            }

            int bitCount = Long.bitCount(data);

            //for (int b = 0; b < 64 ; b++) {
            for (int b = Long.numberOfTrailingZeros(data); b < 64; b++) {
                long id = i*64 + b;
                if (id == 0) {
                    continue; //TODO
                }
                //id = ((long)id&0x0000003FL);
                long mask = (long)(0x01L << b);
                if((data & mask) != 0) {
                    bitCount--;
                    foundBits++;

                    int acc = GROUP_IDX[(int)id];

                    if (isInterests) {
                        ArrayDataAllocator.ShortArray a = ArrayDataAllocator.accountsInterests[(int)id];

                        if (a != null) {
                            for (short itrIdL : a.array) {
                                int gKey = (int) itrIdL; //TODO
                                GroupNode g = groups.get(gKey);
                                counts[g.uniqId]++;
                                //resultGroups.putIfAbsent(g.uniqId, g);
                            }
                        }
                    } else {

                        int gKey = gKeyProvider.getGKey(acc);
                        //GroupNode g = groups.get(gKey);
                        GroupNode g = G_KEY_GROUPS[keysOffset + gKey];
                        counts[g.uniqId]++;

                        //GroupNode g = groups.get(gKey);
                        /*for (int j = 0; j < 200; j++) {
                            if (gkeys[j] == gKey) {
                                counts[j]++;
                                break;
                            }
                        }*/
                        //gkeys[5]++;
                        //groupCountsMap.merge(g.uniqId, 1, Integer::sum);
                        //resultGroups.putIfAbsent(g.uniqId, g);
                        //}
                    }

                }
                if (bitCount == 0) {
                    break;
                }
            }
        }

        if (foundBits == 0) {
            return;
        }

        for (int countIdx = 1; countIdx < counts.length; countIdx++) {
            int count = counts[countIdx];
            if (count > 0) {
                //gkeys[countIdx] = (count & 0xFFFF) | (countIdx << 16);
                groupCountMaps.put(countIdx, count);
                result.add(GROUPS_BY_UNIQ.get(countIdx));
            }
        }
        //groupCountsMap.keySet().forEach( key -> {
        //    result.add(GROUPS_BY_UNIQ.get(key));
        //});

        //logger.error("total - {}; found - {}", totalBits, foundBits);
        //logger.error("groupCountsMap size - {}", groupCountsMap.size());
    }

    static ThreadLocal<int[]> groupCountsTL = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[GN_ID.get() + 3];
        }
    };

    public static TreeSet<GroupNode> getGroups(List<BaseFilter> filters, String keys, Iterable<Map.Entry<String, String>> params, int direction, int limit) {


        TreeSet<GroupNode> groups;

        final TreeSet<GroupNode> result = resultTL.get();
        result.clear();

        final Map<Integer, Integer> groupCountMaps = nodeCountsfTL.get();
        groupCountMaps.clear();

        //long[] bitSet = Filters.applyFilters(filters);

        final String[] key = keys.split(",");

        if (key.length == 1) {
            groups = groupsByKeys.get(key[0]);
        } else if (key.length == 2) {
            groups = groupsByKeys.get(keys);
        } else {
            return EMPTY_SET;
        }

        if (filters.isEmpty()) {
            //filtersTL.set(EMPTY_FILTERS);
            groups.forEach( g -> {
                if (g.count > 0) {
                    groupCountMaps.put(g.uniqId, g.count);
                    result.add(g);
                }

            });
            return result;
        } else {

            final int[] groupCounts = groupCountsTL.get();


            final Map<Integer, GroupNode> resultGroups = gTL.get();
            resultGroups.clear();

            for (BaseFilter f: filters) {
                if (f.getClass().equals(LikesFilter.class)) {

                    int[] likers = Suggester.likesForUser[Integer.valueOf(f.value)].ids;

                    int[] accounts = accountsTL.get();

                    for (int i = 0; i < likers.length; i++) {
                        int likeAccId = likers[i] & 0x00FFFFFF;
                        accounts[i] = likeAccId;
                    }

                    for (int i = 0; i < likers.length; i++) {
                        for (BaseFilter filter: filters) {
                            if (!filter.checkAccount(accounts[i])) {
                                accounts[i] = Integer.MAX_VALUE;
                            }
                        }
                    }

                    getGroupsForAccounts(keys, accounts, likers.length, result, groupCountMaps);
                    groupCountMaps.keySet().forEach( gCountKey -> {
                        result.add(resultGroups.get(gCountKey));
                    });
                    return result;
                }
            }



            if (filters.size() == 1) {
                BaseFilter f = filters.get(0);

                long[] set = f.getBitmapFilteredSet(null);
                if (set != null) {
                    getGroupsForBitSet(keys, set, result, groupCountMaps);
                    return result;
                } else {
                    return EMPTY_SET;
                }
            } else {
                long[] finalSet = null;
                for (BaseFilter f: filters) {
                    f.isLast = false;
                    f.isSingle = false;
                    finalSet = f.getBitmapFilteredSet(finalSet);

                    if (finalSet == null) {
                        return EMPTY_SET;
                    }
                }
                getGroupsForBitSet(keys, finalSet, result, groupCountMaps);
                return result;
            }


        }
    }


}
