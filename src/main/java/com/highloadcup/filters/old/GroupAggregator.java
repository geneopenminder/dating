package com.highloadcup.filters.old;

import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.filters.CountryBitMapFilter;
import com.highloadcup.filters.EmptyFilter;
import com.highloadcup.filters.Filters;
import com.highloadcup.filters.GroupFilter;
import com.highloadcup.filters.GroupItem;
import com.highloadcup.filters.InterestsBitMapFilter;
import com.highloadcup.filters.SexFilter;
import com.highloadcup.filters.StatusFilter;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;

@Deprecated
public class GroupAggregator {

    private static final Logger logger = LogManager.getLogger(GroupAggregator.class);

    //GET: /accounts/group/?birth=1998&limit=4&order=-1&keys=country
    /*
{"groups": [
    {"country": "Малатрис", "count": 8745},
    {"country": "Алания", "count": 4390},
    {"country": "Финляндия", "count": 2100},
    {"country": "Гератрис", "count": 547}
]}
    * */

    public static class GroupNode implements Comparable<GroupNode> {
        public BaseFilter filter;
        public String[] keys;
        public short[] keysIds;
        public int count;

        public GroupNode(BaseFilter filter, String[] keys, short[] keysIds, int count) {
            this.filter = filter;
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

    final public static Map<String, BaseFilter> FILTERS_BY_KEY = new HashMap<String, BaseFilter>() {
        {
            put("sex", new SexFilter());
            put("status", new StatusFilter());
            put("interests", new InterestsBitMapFilter());
            put("country", new CountryBitMapFilter());
            put("city", new CityBitMapFilter());
        }
    };

    final static List<GroupItem> emptyG = new ArrayList<>();

    //common withou filters ****************************************************
    public static HashMap<String, TreeSet<GroupNode>> goupsByKeys = new HashMap<>();

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

    //query_id=1048&likes=22783&keys=interests&limit=25&order=1
    public final static Set<String> GROUP_EXCLUDE_KEYS = new HashSet<String>() {{add("query_id"); add("order"); add("keys"); add("limit");}};

    final static TreeSet<GroupNode> EMPTY_SET = new TreeSet<>();

    public static void init() throws Exception {
        initBaseGroups();
    }

    public static void initBaseGroups() {

        //sex
        SexFilter sf = new SexFilter();
        sexGroups.add(new GroupNode(sf, new String[]{"m"}, new short[]{1}, SexFilter.totalM));
        sexGroups.add(new GroupNode(sf, new String[]{"f"}, new short[]{1}, SexFilter.totalF));
        goupsByKeys.put("sex", sexGroups);

        //status
        StatusFilter stF = new StatusFilter();
        StatusFilter.countsByStatuses.forEach((key, value) -> {
            if (value != 0) {
                statusGroups.add(new GroupNode(stF, new String[]{InternalAccount.getStatus(key)}, new short[]{key}, value));
            }
        });
        goupsByKeys.put("status", statusGroups);

        CountryBitMapFilter cf = new CountryBitMapFilter();
        CountryBitMapFilter.countByCountry.forEach((key, value) -> {
                    if (value != 0) {
                        countryGroups.add(new GroupNode(cf, new String[]{Dictionaries.countriesById.get(key)}, new short[]{key}, value));
                    }
        } );
        goupsByKeys.put("country", countryGroups);

        CityBitMapFilter ciF = new CityBitMapFilter();
        CityBitMapFilter.countByCity.forEach((key, value) -> {
                    if (value != 0) {
                        cityGroups.add(new GroupNode(ciF, new String[]{Dictionaries.citiesById.get(key)}, new short[]{key}, value));
                    }
        });
        goupsByKeys.put("city", cityGroups);

        InterestsBitMapFilter inf = new InterestsBitMapFilter();
        InterestsBitMapFilter.countByInterests.forEach((key, value) -> {
                    if (value != 0) {
                        interestsGroups.add(new GroupNode(inf, new String[]{Dictionaries.interestsById.get(key)}, new short[]{key}, value));
                    }
        });
        goupsByKeys.put("interests", interestsGroups);


        long[] tmp = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        //country,status
        StatusFilter.countsByStatuses.forEach((status, sCount) -> {
            CountryBitMapFilter.countByCountry.forEach((country, cCount) -> {
                    System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
                    DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));
                    int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    if (bitCount > 0) {
                        countryStatusGroups.add(new GroupNode(null,
                                new String[]{Dictionaries.countriesById.get(country), InternalAccount.getStatus(status)},
                                new short[]{country, status}, bitCount));
                    }
            });

            System.arraycopy(CountryBitMapFilter.nullBitMap, 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));

            int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
            if (bitCount > 0) {
                countryStatusGroups.add(new GroupNode(null,
                        new String[]{null, InternalAccount.getStatus(status)},
                        new short[]{0, status}, bitCount));
            }
        });
        goupsByKeys.put("country,status", countryStatusGroups);

        //city,status
        StatusFilter.countsByStatuses.forEach((status, sCount) -> {
            CityBitMapFilter.countByCity.forEach((city, cCount) -> {
                System.arraycopy(CityBitMapFilter.cityAccountsBitMaps.get(city), 0, tmp, 0, tmp.length);
                DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));
                int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                if (bitCount > 0) {
                    cityStatusGroups.add(new GroupNode(null,
                            new String[]{Dictionaries.citiesById.get(city), InternalAccount.getStatus(status)},
                            new short[]{city, status}, bitCount));
                }
            });

            System.arraycopy(CityBitMapFilter.nullCitiesBitMap, 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, StatusFilter.bitmapsByStatuses.get(status));

            int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
            if (bitCount > 0) {
                cityStatusGroups.add(new GroupNode(null,
                        new String[]{null, InternalAccount.getStatus(status)},
                        new short[]{0, status}, bitCount));
            }
        });
        goupsByKeys.put("city,status", cityStatusGroups);

        //country,sex
        CountryBitMapFilter.countByCountry.forEach((country, cCount) -> {
            System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
                    int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    if (bitCount > 0) {
                        countrySexGroups.add(new GroupNode(null,
                                new String[]{Dictionaries.countriesById.get(country), "m"},
                                new short[]{country, 1}, bitCount));
                    }

            System.arraycopy(CountryBitMapFilter.countryAccountsBitMaps.get(country), 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
                    bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    if (bitCount > 0) {
                        countrySexGroups.add(new GroupNode(null,
                                new String[]{Dictionaries.countriesById.get(country), "f"},
                                new short[]{country, 2}, bitCount));
                    }
        });

        System.arraycopy(CountryBitMapFilter.nullBitMap, 0, tmp, 0, tmp.length);
        DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
        countrySexGroups.add(new GroupNode(null,
                new String[]{null, "m"},
                new short[]{0, 1}, DirectMemoryAllocator.getBitsCounts(tmp)));

        System.arraycopy(CountryBitMapFilter.nullBitMap, 0, tmp, 0, tmp.length);
        DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
        countrySexGroups.add(new GroupNode(null,
                new String[]{null, "f"},
                new short[]{0, 2}, DirectMemoryAllocator.getBitsCounts(tmp)));
        goupsByKeys.put("country,sex", countrySexGroups);

        //city,sex
        CityBitMapFilter.countByCity.forEach((city, cCount) -> {
            System.arraycopy(CityBitMapFilter.cityAccountsBitMaps.get(city), 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
                    int bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    if (bitCount > 0) {
                        citySexGroups.add(new GroupNode(null,
                                new String[]{Dictionaries.citiesById.get(city), "m"},
                                new short[]{city, 1}, bitCount));
                    }

            System.arraycopy(CityBitMapFilter.cityAccountsBitMaps.get(city), 0, tmp, 0, tmp.length);
            DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
                    bitCount = DirectMemoryAllocator.getBitsCounts(tmp);
                    if (bitCount > 0) {
                        citySexGroups.add(new GroupNode(null,
                                new String[]{Dictionaries.citiesById.get(city), "f"},
                                new short[]{city, 2}, bitCount));
                    }
        });

        System.arraycopy(CityBitMapFilter.nullCitiesBitMap, 0, tmp, 0, tmp.length);
        DirectMemoryAllocator.intersectArrays(tmp, SexFilter.femaleBitMap);
        citySexGroups.add(new GroupNode(null,
                new String[]{null, "f"},
                new short[]{0, 2}, DirectMemoryAllocator.getBitsCounts(tmp)));

        System.arraycopy(CityBitMapFilter.nullCitiesBitMap, 0, tmp, 0, tmp.length);
        DirectMemoryAllocator.intersectArrays(tmp, SexFilter.maleBitMap);
        citySexGroups.add(new GroupNode(null,
                new String[]{null, "m"},
                new short[]{0, 2}, DirectMemoryAllocator.getBitsCounts(tmp)));

        goupsByKeys.put("city,sex", citySexGroups);
    }



    public static TreeSet<GroupNode> getGroupNodes(String keys, Iterable<Map.Entry<String, String>> params, int direction, int limit) {
        final String[] key = keys.split(",");

        if (key.length == 1) {
            TreeSet<GroupNode> groups = goupsByKeys.get(key[0]);
            return groups;
        } else if (key.length == 2) {
            TreeSet<GroupNode> groups = goupsByKeys.get(keys);
            return groups;
        } else {
            return EMPTY_SET;
        }
    }

    static ThreadLocal<long[]> bitmapThreadLocal = new ThreadLocal<long[]>() {

        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }

    };

    @Deprecated
    public static List<GroupItem> getGroups(String keys, Iterable<Map.Entry<String, String>> params, int direction, int limit) {
        List<BaseFilter> filters = Filters.getFiltersByPriority(params, Integer.MAX_VALUE, GROUP_EXCLUDE_KEYS);

        if (filters.size() == 1 && filters.get(0).getClass().equals(EmptyFilter.class)) {
            return emptyG;
        }

        long[] bitSet = Filters.applyFilters(filters);

        ArrayList<BaseFilter> groupFilters = new ArrayList<BaseFilter>(5);

        for (String key: keys.split(",")) {
            groupFilters.add(FILTERS_BY_KEY.get(key.trim()));
        }

        if (groupFilters.isEmpty()) {
            return emptyG;
        }

        final boolean singleKey = groupFilters.size() == 1;
        List<GroupItem> groups = null;
        for (BaseFilter f: groupFilters) {
            groups = ((GroupFilter)f).group(bitSet, groups, singleKey, direction, limit);
        }

        return groups;
    }

}
