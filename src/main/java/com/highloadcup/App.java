package com.highloadcup;

import com.google.gson.Gson;
import com.highloadcup.filters.Filters;
import com.highloadcup.filters.GroupBitmapsAggregator;
import com.highloadcup.model.Likes;
import one.nio.os.NativeLibrary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Unsafe;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    private static Gson gson = new Gson();

    public static StringBuilder sb = new StringBuilder(10000 * 50);

    private static void printUsage() {
        //logger.error("Heap free: {}", Runtime.getRuntime().freeMemory());
        List<GarbageCollectorMXBean> garbageCollectorMXBeans =  ManagementFactory.getGarbageCollectorMXBeans();

        garbageCollectorMXBeans.forEach( gc -> {
            logger.error("gc {}, count - {}, time - {}", gc.getClass(), gc.getCollectionCount(), gc.getCollectionTime());
        });

        /*
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (method.getName().startsWith("get")
                    && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (Exception e) {
                    value = e;
                } // try
                logger.error(method.getName() + " = " + value);
            } // if
        } // for
        */
    }

    /*
    * -server
-XX:MaxGCPauseMillis=500
-XX:+UseCompressedOops
-XX:+UseG1GC
-XX:+AggressiveOpts
-Xmx1700m
-Xverify:none
*/

    public static long START_TIME = System.currentTimeMillis();



    public static void main( String[] args ) throws Exception {

        logger.error("java version - {}", Runtime.version());

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();

        arguments.forEach( a -> {
            logger.error("jvm arg: {}", a);
        });

        //ExecutorService e = Executors.newFixedThreadPool(4);
        DataLoader.loadFromJson();
        ArrayDataAllocator.TOTAL_ACC_LOADED = ArrayDataAllocator.MAX_ACC_ID_ADDED.get();

        logger.error("finish load: accounts - {}", ArrayDataAllocator.TOTAL_ACC_LOADED);

        //logger.error("loadAccounts finished");

        System.gc();
        //logger.error("sort data");
        //ArrayDataAllocator.sortAllArraysByExternalAccID();
        //logger.error("sortAllArraysByExternalAccID finished");
        System.gc();
        Filters.initFilters();
        logger.error("filters finished");
        //logger.error("Filters init groups");
        //GroupAggregator.init();
        GroupBitmapsAggregator.init();
        logger.error("GroupBitmapsAggregator finished");
        //logger.error("init groups finished");
        //logger.error("start initRecomends");
        Recomendator.initRecommends();
        logger.error("Recomendator finished");

        logger.error("Suggester start");
        Suggester.initTS();
        logger.error("Suggester finish");
        //warmUpJVM();

        System.gc();

        logger.error("all init finished, now likeTS - {}", ArrayDataAllocator.NOW_TIMESTAMP);

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                //logger.error("Uncaught error; ", t, e);
            }
        });


        //getSuggests();

        //System.exit(3);
/*


        int count = 10000;
        logger.error("start fun!");
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            Recomendator.findRecommendsForAll(120001, 20);
        }

        logger.error("funny: count - {}; total time - {}; req per sec - {}; avg - {}",
                count, (System.nanoTime() - start), 1_000_000_000 / ((System.nanoTime() - start)/count), ((System.nanoTime() - start)/count));

        start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            Recomendator.findRecommendsForAll(120026, 20);
        }

        logger.error("boring: count - {}; total time - {}; req per sec - {}; avg - {}",
                count, (System.nanoTime() - start), 1_000_000_000 / ((System.nanoTime() - start)/count), ((System.nanoTime() - start)/count));


        System.exit(3);

        ///accounts/11151/recommend/?limit=16&city=Кронанск

        /*Map<String, String> params = new HashMap<String, String>() {{
            put("city","Кронанск");
        }};

        Recomendator.processRecommends(11151, params);
        System.exit(3);*/

        //System.out.println("accounts: " + internalAccountList.size());

        /*
        logger.error("countries: " + Dictionaries.countries.size());
        logger.error("cities: " + Dictionaries.cities.size());
        logger.error("email domains: " + Dictionaries.emailDomainsMap.size());
        logger.error("fnames: " + Dictionaries.fnames.size());
        logger.error("snames: " + Dictionaries.snames.size());
        logger.error("interests: " + Dictionaries.interests.size());
*/
        System.gc();
        logger.error("Final Heap total: {}", Runtime.getRuntime().totalMemory());
        logger.error("Final Heap max: {}", Runtime.getRuntime().maxMemory());
        logger.error("Final Heap free: {}", Runtime.getRuntime().freeMemory());


        //printUsage();



        TimerTask repeatedTask = new TimerTask() {
            public void run() {
                printUsage();
            }
        };
        Timer timer = new Timer("Timer");

        long delay  = 1000L * 60 * 15;
        long period = 1000L * 60;
        //timer.scheduleAtFixedRate(repeatedTask, delay, period);


        logger.error("native supported - {}", NativeLibrary.IS_SUPPORTED);
        try {
            OneNioServer.createServer();
            logger.error("OneNioServer started");
            synchronized (App.class) {
                App.class.wait();
            }
        } finally {
        }

        /*
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("sex_eq", new String[]{"m"});
        }};

        int limit = 2;
        String keys = "interests";
        int direction = -1;
        List<GroupItem> groups = GroupAggregator.getGroups(keys, limit, direction, params);

        Collections.sort(groups, new Comparator<GroupItem>() {
            @Override
            public int compare(GroupItem o1, GroupItem o2) {
                if (direction == 1) {
                    return Integer.compare(o1.count, o2.count);
                } else {
                    return Integer.compare(o2.count, o1.count);
                }
            }
        });

        if (groups.size() >= limit*2) {
            List g = new ArrayList<GroupItem>();
            g.addAll(groups.subList(0, limit));
            g.addAll(groups.subList(groups.size() - limit, groups.size()));
            groups = g;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{\"groups\": [");
        groups.forEach(g -> {
            sb.append("{\"").append(keys).append("\": \"").append(g.key).append("\", \"count\": ").append(g.count).append("},");
        });
        sb.setLength(sb.length() - 1); //remove last "," symbol
        sb.append("]}");

        logger.error(sb.toString());
        System.exit(3);
        /*
        //gabecyegmeah@mail.ru
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            //put("sname_eq", new String[]{(String)Dictionaries.snames.keySet().toArray()[1]});
            //put("fname_eq", new String[]{(String)Dictionaries.fnames.keySet().toArray()[1]});
            //put("sname_eq", new String[]{"Стаматотин"});
            //put("email_gt", new String[]{"zzzz"});
            //put("birth_lt", new String[]{"881950237"});
            put("interests_contains", new String[]{"Курица,Компьютеры,Вечер с друзьями"});
            //
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
            //put("sex_eq", new String[]{"m"});
            //put("email_domain", new String[]{"mail.ru"});
            //put("email_lt", new String[]{"gh"});
            //put("status_eq", new String[]{"свободны"});
            //put("sname_eq", new String[]{"Стаматотин"});
        }};

        //List<String> allDomains = new ArrayList<>(Dictionaries.emailDomainsMap.keySet());
        Thread.sleep(10000);

        long allTime = System.nanoTime();
        int limit = 32;
        //int position = ThreadLocalRandom.current().nextInt(0, allDomains.size());
        for (int i = 0; i < 30000; i++) {
            //params.put("email_domain", new String[]{allDomains.get(position)});
            final List<BaseFilter> filters = Filters.getFilters(params, limit);
            long startApply = System.nanoTime();
            long[] filteredSet = Filters.applyFilters(filters);
            final int[] finalSet = DirectMemoryAllocator.transformBitMapToSet(filteredSet, limit);
            logger.error("applyFilters time - {} ns; finalSet size - {}", System.nanoTime() - startApply, finalSet.length);
            printResponse(finalSet, limit, filters);
            //position = ThreadLocalRandom.current().nextInt(0, allDomains.size());
        }

        long time = System.nanoTime() - allTime;

        logger.error("time: {} ns ; reqTime: {}", time, time / 100000);

        //System.exit(2);
        /*
        int[] dataSet = new int[] {2,5,4,7,8,9,21,99,134,5002,10432};

        long allTime = System.nanoTime();
        for (int i = 0; i < 10000; i++) {
            int[] filteredSet = Filters.applyFilters(params);
            logger.info("size - {}", filteredSet.length);
            createResponse(filteredSet);
        }

        long time = System.nanoTime() - allTime;
        long rqTIme = time / 10000;

        logger.info("time: {}; reqTime - -{}", time, rqTIme);
        */


        /*

        final HttpServer httpServer = new HttpServer();
        final NetworkListener networkListener = new NetworkListener(
                "http-listener", "0.0.0.0", 80);
        networkListener.setChunkingEnabled(false);

        final TCPNIOTransportBuilder builder = TCPNIOTransportBuilder.newInstance();
        final int coresCount = 2; //Runtime.getRuntime().availableProcessors() * 2;


        CompressionConfig cc = networkListener.getCompressionConfig();
        cc.setCompressionMode(CompressionConfig.CompressionMode.ON);
        cc.setCompressionMinSize(5000000); // the min number of bytes to compress
        cc.setCompressableMimeTypes("application/json", "text/json", "text/html");

        final TCPNIOTransport transport = builder
                .setIOStrategy(WorkerThreadIOStrategy.getInstance())
                .setWorkerThreadPoolConfig(ThreadPoolConfig.defaultConfig()
                        .setPoolName("Grizzly-worker")
                        .setCorePoolSize(coresCount)
                        .setMaxPoolSize(coresCount)
                        .setMemoryManager(builder.getMemoryManager())
                        .setQueueLimit(-1))
                .build();

        networkListener.setTransport(transport);
        networkListener.setChunkingEnabled(false);

        transport.setSelectorRunnersCount(coresCount);
        transport.configureBlocking(false);
        transport.setTcpNoDelay(true);


        // force to not initialize worker thread pool
        transport.setWriteBufferSize(30000000);



        // set PooledMemoryManager
        //transport.setMemoryManager(new PooledMemoryManager());

        // always keep-alive
        networkListener.getKeepAlive().setIdleTimeoutInSeconds(-1);
        networkListener.getKeepAlive().setMaxRequestsCount(-1);

        // disableAndClear transaction timeout
        networkListener.setTransactionTimeout(-1);

        // remove the features we don't need
        //networkListener.registerAddOn(new SimplifyAddOn());
        // add HTTP pipeline optimization
        //networkListener.registerAddOn(new HttpPipelineOptAddOn());

        // disableAndClear file-cache
        //networkListener.getFileCache().setEnabled(false);

        httpServer.addListener(networkListener);

        httpServer.getServerConfiguration().addHttpHandler(
                new RequestHandler(), "/");

        try {
            httpServer.start();
            synchronized (App.class) {
                App.class.wait();
            }
        } finally {
            httpServer.shutdown();
        }
*/
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
    }*/

}
