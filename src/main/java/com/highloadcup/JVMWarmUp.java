package com.highloadcup;

import com.highloadcup.filters.BaseFilter;
import com.highloadcup.filters.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.highloadcup.App.START_TIME;

public class JVMWarmUp {

    private static final Logger logger = LogManager.getLogger(App.class);


    public static List<String> loadRequests() throws Exception {

        List<String> r = new ArrayList<>();
        JVMWarmUp jvmWarmUp = new JVMWarmUp();

        ClassLoader classLoader = jvmWarmUp.getClass().getClassLoader();
        File file = new File(classLoader.getResource("phase_1_get.ammo").getFile());

        try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {
            stream.forEach(line -> {
                if(line.startsWith("GET")) {
                    String req = line.substring(4, line.length() - 9);
                    r.add(req);
                }
            });
        }

        return r;
    }

    public static class ReqInfo implements Comparable<ReqInfo> {
        public String url;
        public String code;
        public long time;

        @Override
        public int compareTo(ReqInfo o) {
            return Long.compare(o.time, this.time);
        }
    }

    static ExecutorService e = Executors.newFixedThreadPool(4);

    static boolean endWarmup = false;

    public static void fire(int repeatTimes)  throws Exception {


        try {

            List<String> requests = loadRequests();

            ClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();

            RestTemplate restTemplate = new RestTemplate(requestFactory);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

            HttpEntity entity = new HttpEntity(headers);
            long startAll = System.nanoTime();

            final AtomicLong countReq = new AtomicLong(0);


            for (int repeat = 0; repeat < repeatTimes; repeat++) {
                for (String req : requests) {

                    if (endWarmup) {
                        break;
                    }
                    if (ArrayDataAllocator.ACCOUNTS_TOTAL.get() < 50000) {
                        if ((System.currentTimeMillis() - START_TIME) / 1000 > 30) {
                            //logger.error("break");
                            break;
                        }
                    } else {
                        if ((System.currentTimeMillis() - START_TIME) / 10000 > 60 * 5) {
                            //logger.error("break");
                            break;
                        }
                    }

                    final String uri = "http://127.0.0.1:80" + java.net.URLDecoder.decode(req, "UTF-8");
                    try {
                        //logger.error("exec 1");
                        e.execute(new Runnable() {
                            @Override
                            public void run() {
                                if (endWarmup) {
                                    return;
                                }


                                if (ArrayDataAllocator.ACCOUNTS_TOTAL.get() < 50000) {
                                    if ((System.currentTimeMillis() - START_TIME) / 1000 > 20) {
                                        //logger.error("break");
                                        return;
                                    }
                                } else {
                                    if ((System.currentTimeMillis() - START_TIME) / 10000 > 60 * 12) {
                                        //logger.error("break");
                                        return;
                                    }
                                }

                                //logger.error("run 1");
                                long start = System.nanoTime();
                                try {
                                    HttpEntity<String> response = restTemplate.exchange(uri, HttpMethod.GET, entity, String.class);
                                    long time = System.nanoTime() - start;
                                    //logger.info("{} time; count - {}, code - {}; uri - {}",
                                          //  time, countReq.incrementAndGet(), ((ResponseEntity<String>) response).getStatusCode().value(), uri);

                                    ReqInfo info = new ReqInfo();
                                    info.code = Integer.toString(((ResponseEntity<String>) response).getStatusCode().value());
                                    info.time = time;
                                    info.url = uri;
                                    //logger.error("time: {}; code - {}", System.nanoTime() - start, ((ResponseEntity<String>) response).getStatusCode().value());
                                } catch (Exception e) {
                                    long time = System.nanoTime() - start;
                                    ReqInfo info = new ReqInfo();
                                    info.code = e.getMessage();
                                    info.time = time;
                                    info.url = uri;
                                    //logger.error("errr", e);
                                    //logger.error("{} time; count - {}; no response; error code - {}; uri - {}",
                                            //time, countReq.incrementAndGet(), e.getMessage(), uri);
                                }
                            }
                        });
                    } catch (RestClientException ex) {
                        //logger.error("warmup errro: ", ex);
                        //logger.info("time: {}; error code - {}", System.nanoTime() - start, ex.toString());
                        //logger.info("time: {}; ", System.nanoTime() - start);
                    }
                }
            }
            //logger.info("total count - {}; total time - {}; medium: {}; rps - {} / sec",
            //        countReq.get(), (System.nanoTime() - startAll), (System.nanoTime() - startAll) / countReq.get(),
            //        1_000_000_000 / ((System.nanoTime() - startAll) / countReq.get()));
        } catch (Exception ex) {
            logger.error("warmup errro: ", ex);
            endWarmup = true;
        } finally {
            e.shutdown();
        }
        if (ArrayDataAllocator.ACCOUNTS_TOTAL.get() < 50000) {
            e.awaitTermination(30, TimeUnit.SECONDS);
        } else {
            e.awaitTermination(5, TimeUnit.MINUTES);

        }
        endWarmup = true;
        logger.error("wrmup exit");

    }


    public static void warmUpJVM() throws Exception {

        long start = START_TIME;

        logger.error("start warmUP");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    fire(ArrayDataAllocator.ACCOUNTS_TOTAL.get() > 50_000 ? 200 : 50);
                } catch (Exception e) {
                    logger.error("warmup error");
                }
            }
        }).start();


        /*
        final int totalCalls = 10000;

        warmUpBirthFilter(totalCalls);
        warmUpCityFilter(totalCalls);
        warmUpCountryFilter(totalCalls);
        warmUpEmailFilter(totalCalls);
        warmUpFnamesFilter(totalCalls);
        warmUpSnamesFilter(totalCalls);

        /*
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("sname_eq", new String[]{(String) Dictionaries.snames.keySet().toArray()[1]});
            put("fname_eq", new String[]{(String)Dictionaries.fnames.keySet().toArray()[1]});
            put("email_gt", new String[]{"ab"});
            put("birth_lt", new String[]{Long.toString(System.currentTimeMillis() / 1000)});
            put("interests_contains", new String[]{"Курица"});
            put("sex_eq", new String[]{"m"});
            put("email_domain", new String[]{"mail.ru"});
            put("email_lt", new String[]{"gh"});
            put("status_eq", new String[]{"свободны"});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFilters(params, 32);
                long startApply = System.nanoTime();
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException ignored) {}
        }

        logger.error("end warmup; func calls per filter - {}; total time - {}",
                totalCalls, System.nanoTime() - start);
        System.gc();
        */
    }

    public static void warmUpBirthFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("birth_lt", new String[]{Long.toString(System.currentTimeMillis() / 1000)});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long startApply = System.nanoTime();
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup birth error", e);
            }
        }
        params.remove("birth_lt");
        params.put("birth_gt", new String[]{Long.toString((System.currentTimeMillis() / 1000) - 60*60*24*5000)});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long startApply = System.nanoTime();
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup bith error", e);
            }
        }
    }

    public static void warmUpCityFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("city_eq", new String[]{(String) Dictionaries.cities.keySet().toArray()[3]});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup city error", e);
            }
        }
        params.remove("city_eq");
        params.put("city_any", new String[]{(String) Dictionaries.cities.keySet().toArray()[5]});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup city error", e);
            }
        }
    }

    public static void warmUpCountryFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("country", new String[]{(String) Dictionaries.countries.keySet().toArray()[3]});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup city error", e);
            }
        }
    }

    public static void warmUpEmailFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("email_gt", new String[]{"de"});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup email error", e);
            }
        }
        params.remove("email_gt");
        params.put("email_lt", new String[]{"jz"});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup email error", e);
            }
        }
        params.remove("email_lt");
        params.put("email_domain", new String[]{"mail.ru"});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup email error", e);
            }
        }
    }

    public static void warmUpFnamesFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("fname_eq", new String[]{(String) Dictionaries.fnames.keySet().toArray()[3]});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup fname error", e);
            }
        }
        params.remove("fname_eq");
        params.put("fname_any", new String[]{(String) Dictionaries.fnames.keySet().toArray()[43]});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup fname error", e);
            }
        }
    }

    public static void warmUpSnamesFilter(int totalCalls) {
        Map<String, String[]> params = new HashMap<String, String[]>() {{
            put("sname_eq", new String[]{(String) Dictionaries.snames.keySet().toArray()[3]});
            put("query_id", new String[]{"32"});
            put("limit", new String[]{"32"});
        }};
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup sname error", e);
            }
        }
        params.remove("sname_eq");
        params.put("sname_starts", new String[]{((String) Dictionaries.snames.keySet().toArray()[43]).substring(0, 2)});
        for (int i=0; i < totalCalls; i++) {
            try {
                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, 32);
                long[] filteredSet = Filters.applyFilters(filters);
            } catch (RuntimeException e) {
                logger.error("warmup sname error", e);
            }
        }
    }

}
