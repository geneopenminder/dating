package com.highloadcup;

import com.google.gson.Gson;
import com.highloadcup.filters.*;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.LikesPool;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.LikesUpdater.processAddLikes;
import static com.highloadcup.filters.Filters.transformBitMapToSet;
import static com.highloadcup.filters.Filters.validateFilterParams;
import static com.highloadcup.filters.GroupBitmapsAggregator.nodeCountsfTL;

public class OneNioServer extends HttpServer {

    private static final Logger logger = LogManager.getLogger("Handler");

    public static Gson gson = new Gson();

    static ThreadLocal<StringBuilder> stringBuilderThreadLocal = ThreadLocal.withInitial(() -> new StringBuilder(10000 * 50));

    public OneNioServer(HttpServerConfig config, Object... routers) throws IOException {
        super(config, routers);
    }


    @Path("/test") //likes or ID
    @RequestMethod(Request.METHOD_GET)
    public Response test() {
        return new Response(Response.OK, "{}".getBytes());
    }


    //@Path("/accounts/likes") //likes or ID
    //@RequestMethod(Request.METHOD_POST)
    public Response handleLikes() {
        return new Response(Response.ACCEPTED, "{}".getBytes());
    }

    //@Path("/accounts/new/")
    //@RequestMethod(Request.METHOD_POST)
    public Response handleNew() {
        return new Response(Response.CREATED, "{}".getBytes());
    }

    //@Path("/accounts/group/")
    public Response handleSimple() {
        return Response.ok("{groups: []}");
    }

    /*
Если в GET-запросе передана страна или город с ключами country и city соответственно,
 то нужно искать только среди живущих в указанном месте.

По ключу "accounts" должны быть N пользователей,
сортированных по убыванию их совместимости с обозначенным id.
Число N задаётся в запросе GET-параметром limit и не бывает больше 20.

В итоговом списке необходимо выводить только следующие поля: id, email, status, fname, sname, birth, premium, interests
    * */
    //{"accounts": [ ... ]}




    public static volatile boolean wasGet = false;

    public static AtomicBoolean wasPost = new AtomicBoolean(false);
    public static AtomicBoolean postFinished = new AtomicBoolean(false);

    public void processSuggest(Request request, HttpSession session) throws Exception {
        long start = System.nanoTime();
        try {
            final String uri = request.getURI();
            final int lookingForId = Integer.parseInt(uri.substring(10, uri.indexOf("/", 11)));

            if (ACCOUNTS_TOTAL.get() < lookingForId) {
                totalTime.addAndGet(System.nanoTime() - start);
                returnBad404(session);
                return;
            }



            int limit = 20;
            String city = null;
            String country = null;

            Iterator<Map.Entry<String, String>> i = request.getParameters().iterator();
            while (i.hasNext()) {
                Map.Entry<String, String> e = i.next();
                if (e.getKey().equalsIgnoreCase("limit")) {
                    try {
                        limit = Integer.parseInt(e.getValue());
                    }catch (Exception ex) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }
                if (e.getKey().equalsIgnoreCase("city")) {
                    city = e.getValue().trim();
                    if (city.isEmpty()) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }

                if (e.getKey().equalsIgnoreCase("country")) {
                    country = e.getValue().trim();
                    if (country.isEmpty()) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }
            }

            if (limit <= 0) {
                returnBad400(session);
                return;
            }


            short countryIdx = country != null ? Dictionaries.countries.get(country) : Short.MAX_VALUE;
            short cityIdx = city != null ? Dictionaries.cities.get(city) : Short.MAX_VALUE;


            List<Integer> result = Suggester.getSuggests(lookingForId, limit, countryIdx, cityIdx);

            totalTime.addAndGet(System.nanoTime() - start);
            sendSuggestResponse(session, result, limit);
        } catch (Exception e) {
            //logger.error("suggester err", e);
            returnBad404(session);
        }
    }

    public void processRecommends(Request request, HttpSession session) throws Exception {

        long start = System.nanoTime();
        try {
            final String uri = request.getURI();
            final int lookingForId = Integer.parseInt(uri.substring(10, uri.indexOf("/", 11)));

            if (ACCOUNTS_TOTAL.get() < lookingForId) {
                totalTime.addAndGet(System.nanoTime() - start);
                returnBad404(session);
                return;
            }


            int limit = 20;
            String city = null;
            String country = null;

            Iterator<Map.Entry<String, String>> i = request.getParameters().iterator();
            while (i.hasNext()) {
                Map.Entry<String, String> e = i.next();
                if (e.getKey().equalsIgnoreCase("limit")) {
                    try {
                        limit = Integer.parseInt(e.getValue());
                    }catch (Exception ex) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }
                if (e.getKey().equalsIgnoreCase("city")) {
                    city = e.getValue().trim();
                    if (city.isEmpty()) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }

                if (e.getKey().equalsIgnoreCase("country")) {
                    country = e.getValue().trim();
                    if (country.isEmpty()) {
                        totalTime.addAndGet(System.nanoTime() - start);
                        returnBad400(session);
                        return;
                    }
                }
            }

            if (limit <= 0) {
                returnBad400(session);
                return;
            }
            TreeSet<Recomendator.Lover> lovers = Recomendator.processRecommends(lookingForId, city, country, limit);
            if (lovers == null) {
                totalTime.addAndGet(System.nanoTime() - start);
                returnBad404(session);
                return;
            }

            totalTime.addAndGet(System.nanoTime() - start);
            sendRecommendResponse(session, lovers, limit);
        } catch (Exception e) {
            returnBad404(session);
        }
    }

    public static void returnBad404(final HttpSession session) throws Exception {
        Response response = new Response(Response.NOT_FOUND, "{}".getBytes());
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    public static AtomicLong BAD_400_COUNT = new AtomicLong(0);

    public static void returnBad400(final HttpSession session) throws Exception {
        //BAD_400_COUNT.incrementAndGet();
        Response response = new Response(Response.BAD_REQUEST, "{}".getBytes());
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    public static void returnBad400(Request request, HttpSession session) throws Exception {
        //logger.error("400 uri - {}", request.getURI());
        BAD_400_COUNT.incrementAndGet();
        Response response = new Response(Response.BAD_REQUEST, "{}".getBytes());
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    public static AtomicLong totalTime = new AtomicLong(0);



    //            return DirectMemoryAllocator.allocWithFillFF(MAX_ACCOUNTS) ;//, 0xffffffffffffffffL);

    static ThreadLocal<long[]> fullTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.allocWithFillFF(MAX_ACCOUNTS);
        }
    };


    public static AtomicLong postCount = new AtomicLong(0);
    public static AtomicLong getCount = new AtomicLong(0);

    public static int totalPostCount = 10000;


    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {

        long start = System.nanoTime();

        try {
            final String uri = request.getURI();

            int accountsPos = uri.indexOf("accounts");
            int paramsIdx = uri.indexOf('?');

            if (accountsPos == -1) {
                Response response = new Response(Response.NOT_FOUND, "{}".getBytes());
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
                return;
            }

            final String reqString = uri.substring(accountsPos, paramsIdx);

            //logger.error("handle {}; Thread - {}", uri, Thread.currentThread().getId());
            if (request.getMethod() == Request.METHOD_POST) {
                if(postCount.incrementAndGet() >= totalPostCount - 1 & !postFinished.get()) {
                    logger.error("post ended; new likes - {}", Suggester.NEW_LIKE_OFFSET.get());
                    logger.error("likes instances created - {}", LikesPool.count.get());

                    if (postFinished.compareAndSet(false, true)) {

                        logger.error("BAD_400_COUNT - {}", BAD_400_COUNT.get());


                        new Thread() {
                            @Override
                            public void run() {
                                try {
                                    CityFilter.rewriteNew();
                                    Suggester.updateLikes();
                                    GroupBitmapsAggregator.fillGroupIndex();
                                    GroupBitmapsAggregator.createIndexKeys();
                                    //System.gc();
                                } catch (RuntimeException e) {
                                    logger.error("post update error", e);
                                }
                            }
                        }.start();
                    }
                }

                //logger.error("post count - {}", postCount.get());

                if (wasPost.compareAndSet(false, true)) {
                    logger.error("post started");
                    postFinished.set(false);

                    if (getCount.get() <= 26000) {
                        totalPostCount = 10000; //9462
                    } else {
                        totalPostCount = 90000;
                    }
                    logger.error("set post count - {}", totalPostCount);


                    TimerTask repeatedTask = new TimerTask() {
                        public void run() {
                            if (!wasGet) {
                                //logger.error("l qs - {}; r qs - {}; mem - {}; posts - {}", Suggester.likeUpdates.size(),
                                //        AccountUpdater.loverUpdates.size(), Runtime.getRuntime().freeMemory(), postCount.get());
                                logger.error("mem - {}; posts - {}", Runtime.getRuntime().freeMemory(), postCount.get());
                            }
                        }
                    };
                    Timer timer = new Timer("Timer");

                    long delay  = 1000L * 30;
                    long period = 1000L * 8;
                    timer.scheduleAtFixedRate(repeatedTask, delay, period);


                }
                wasGet = false;


                if (reqString.equalsIgnoreCase("accounts/new/")) {
                    AccountUpdater.processNewAccount(request, session, uri);
                    return;
                } else if (reqString.equalsIgnoreCase("accounts/likes/")) {
                    processAddLikes(request, session, uri);
                    return;
                } else if (uri.startsWith("/accounts/")) {
                    AccountUpdater.processUpdateAccount(request, session, uri);
                    return;
                } else {
                    Response response = new Response(Response.ACCEPTED, "{}".getBytes());
                    response.addHeader("Content-Type: application/json");
                    session.sendResponse(response);
                    return;
                }
            }

            getCount.incrementAndGet();

            if (!wasGet) {
                logger.error("get started, posts - {}", postCount.get());
                postFinished.set(true);
            }
            wasGet = true;

            if (uri.startsWith("/accounts/group/")) {
                if (!uri.substring(0, uri.indexOf("?")).equalsIgnoreCase("/accounts/group/")) {
                    Response response = new Response(Response.NOT_FOUND, "{}".getBytes());
                    response.addHeader("Content-Type: application/json");
                    session.sendResponse(response);

                } else {
                    processGroupRequest(request, session);
                }
            } else if (uri.contains("suggest")) {
                processSuggest(request, session);
            } else if (uri.contains("total")) {
                long totalNs = totalTime.get();
                totalTime.set(0);
                Response response = new Response(Response.OK, ("{total - " + totalNs).getBytes());
                logger.error("total time - {}", totalNs);
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
                return;
            } else if (uri.contains("recommend")) {
                //Response response = Response.ok(Utf8.toBytes("{\"accounts\": []}"));
                //response.addHeader("Content-Type: application/json");
                //session.sendResponse(response);

                processRecommends(request, session);
            } else if (reqString.equalsIgnoreCase("accounts/filter/")) {

                Iterable<Map.Entry<String, String>> params = request.getParameters();

                if (!validateFilterParams(params)) {
                    totalTime.addAndGet(System.nanoTime() - start);
                    Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                    response.addHeader("Content-Type: application/json");
                    session.sendResponse(response);
                    return;
                }

                int limit = 32;

                Iterator<Map.Entry<String, String>> i = params.iterator();
                while (i.hasNext()) {
                    Map.Entry<String, String> e = i.next();
                    if (e.getKey().equalsIgnoreCase("limit")) {
                        limit = Integer.parseInt(e.getValue());
                        break;
                    }
                }

                final List<BaseFilter> filters = Filters.getFiltersByPriority(params, limit);

                if (filters.size() == 0) {
                    long[] fullSet = fullTL.get();
                    final int[] finalSet = transformBitMapToSet(fullSet, limit);
                    sendResponse(session, finalSet, limit, filters);
                } else {
                    if (filters.size() == 1 && filters.get(0).getClass().equals(BadFilter.class)) {
                        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                        response.addHeader("Content-Type: application/json");
                        session.sendResponse(response);
                        return;
                    } else if (filters.size() == 1 && filters.get(0).getClass().equals(EmptyFilter.class)) {
                        Response response = Response.ok(Utf8.toBytes("{\"accounts\": []}"));
                        response.addHeader("Content-Type: application/json");
                        session.sendResponse(response);
                        return;
                    }

                    long[] bitSet = Filters.applyFilters(filters);

                    //COMPOUND_FILTER_PREDICATES
                    FullScanCompoundFilter compoundFilter = Filters.getCompoundFilterTL();

                    final int[] finalSet = compoundFilter.isEnabled() ?
                            compoundFilter.transformBitMapToSet(bitSet, limit) :
                            transformBitMapToSet(bitSet, limit);
                    sendResponse(session, finalSet, limit, filters);
                }
            } else {
                Response response = new Response(Response.NOT_FOUND, "{}".getBytes());
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
            }
        } catch (Exception e) {
            Response response = new Response(Response.BAD_REQUEST, "{}".getBytes());
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
            //logger.error("error: ", e);
            //logger.error("oppps: {}; url - ", e, request.getURI());
        } finally {
            totalTime.addAndGet(System.nanoTime() - start);
            //logger.error("{} req; sendResponse time - {}; json size - {}", counter.getAndIncrement(), (System.nanoTime() - start),  request.getBody().length);
        }
    }

    public static ThreadLocal<byte[]> bodyTL = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[10000];
        }
    };

    public static ThreadLocal<Integer> contentLengthTL = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    @Override
    public byte[] getBody(int contentLength) {
        //logger.error("getBody - {}", contentLength);
        //contentLengthTL.set(contentLength); TODO
        byte[] body = bodyTL.get();
        DirectMemoryAllocator.clear(body, body.length);
        return body;
    }


    public void sendSuggestResponse(HttpSession session, List<Integer> ids, int limit) throws IOException {
        if (ids == null || ids.size() == 0) {
            Response response = Response.ok(Utf8.toBytes("{\"accounts\": []}"));
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
            return;
        }
        final StringBuilder sb = stringBuilderThreadLocal.get();

        sb.delete(0, sb.length());

        sb.append("{\"accounts\": [");

        /*
        * {
             "email": "itwonudiahsu@yandex.ru",
             "id": 94155,
             "status": "заняты",
             "fname": "Никита"
        },{*/

        /*for (int i = 0; i < finalSet.length; i++) {
            if (finalSet[i] == Integer.MAX_VALUE) {
                finalSet[i] = -1;
            } else {
                finalSet[i] = ArrayDataAllocator.(finalSet[i]);
            }
        }*/

        //Arrays.sort(finalSet);
        //ArrayUtils.reverse(finalSet);

        //id, email и те, что были использованы в запросе.

        int count = 0;
        for (int id: ids) {
            if (count++ == limit) {
                break;
            }
            sb.append("{");
            sb.append("\"email\":\"").append(ArrayDataAllocator.emailsPlain[id]).append("\",");
            sb.append("\"id\":").append(id).append(",");
            sb.append("\"status\":\"").append(InternalAccount.getStatus(ArrayDataAllocator.statusGlobal[id])).append("\"");

            if (ArrayDataAllocator.snameGlobal[id] != Short.MAX_VALUE) {
                sb.append(",\"sname\":\"").append(Dictionaries.snamesById.get(ArrayDataAllocator.snameGlobal[id])).append("\"");
            }
            if (ArrayDataAllocator.fnamesGlobal[id] != Short.MAX_VALUE) {
                sb.append(",\"fname\":\"").append(Dictionaries.fnamesById.get(ArrayDataAllocator.fnamesGlobal[id])).append("\"");
            }
            sb.append("},");
        }
        if (sb.charAt(sb.length() - 1) != '[') {
            sb.setLength(sb.length() - 1); //remove last "," symbol
        }
        sb.append("]}");
        Response response = Response.ok(Utf8.toBytes(sb.toString()));
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    public void sendRecommendResponse(HttpSession session, TreeSet<Recomendator.Lover> lovers, int limit) throws IOException {
        if (lovers.size() == 0) {
            Response response = Response.ok(Utf8.toBytes("{\"accounts\": []}"));
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
            return;
        }
        final StringBuilder sb = stringBuilderThreadLocal.get();

        sb.delete(0, sb.length());

        sb.append("{\"accounts\": [");

        /*for (int i = 0; i < finalSet.length; i++) {
            if (finalSet[i] == Integer.MAX_VALUE) {
                finalSet[i] = -1;
            } else {
                finalSet[i] = ArrayDataAllocator.(finalSet[i]);
            }
        }*/

        //Arrays.sort(finalSet);
        //ArrayUtils.reverse(finalSet);

        //id, email и те, что были использованы в запросе.

        int count = 0;
        for (Recomendator.Lover l: lovers) {
             if (count++ == limit) {
                 break;
             }
            sb.append("{");
            if (ArrayDataAllocator.snameGlobal[l.id] != Short.MAX_VALUE) {
                sb.append("\"sname\":\"").append(Dictionaries.snamesById.get(ArrayDataAllocator.snameGlobal[l.id])).append("\",");
            }
            sb.append("\"id\":").append(l.id).append(",");
            if (ArrayDataAllocator.fnamesGlobal[l.id] != Short.MAX_VALUE) {
                sb.append("\"fname\":\"").append(Dictionaries.fnamesById.get(ArrayDataAllocator.fnamesGlobal[l.id])).append("\",");
            }
            sb.append("\"birth\":").append(l.age).append(",");
            sb.append("\"email\":\"").append(ArrayDataAllocator.emailsPlain[l.id]).append("\",");
            sb.append("\"status\":\"").append(InternalAccount.getStatus(l.status)).append("\"");
            if (ArrayDataAllocator.premiumStart[l.id] != 0) {
                sb.append(",");
                sb.append("\"premium\":")
                        .append("{\"start\":").append(ArrayDataAllocator.premiumStart[l.id])
                        .append(",\"finish\":").append(ArrayDataAllocator.premiumFinish[l.id])
                        .append("}");
            }

            sb.append("},");
        }
        if (sb.charAt(sb.length() - 1) != '[') {
            sb.setLength(sb.length() - 1); //remove last "," symbol
        }
        sb.append("]}");
        Response response = Response.ok(Utf8.toBytes(sb.toString()));
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    //TODO toooo slow
    public void sendResponse(HttpSession session, int[] finalSet, int limit, List<BaseFilter> filters) throws IOException {

        if (finalSet == null || finalSet.length == 0) {
            Response response = Response.ok(Utf8.toBytes("{\"accounts\": []}"));
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
            return;
        }
        final StringBuilder sb = stringBuilderThreadLocal.get();

        sb.delete(0, sb.length());

        sb.append("{\"accounts\": [");

        /*for (int i = 0; i < finalSet.length; i++) {
            if (finalSet[i] == Integer.MAX_VALUE) {
                finalSet[i] = -1;
            } else {
                finalSet[i] = ArrayDataAllocator.(finalSet[i]);
            }
        }*/

        //Arrays.sort(finalSet);
        //ArrayUtils.reverse(finalSet);

        //id, email и те, что были использованы в запросе.

        for (int id: finalSet) {
            if (id == 0) {
                break;
            }
            sb.append("{");

            for (BaseFilter f: filters) {
                if (f.getClass().equals(FNameFilter.class) && ArrayDataAllocator.fnamesGlobal[id] != Short.MAX_VALUE) {
                    sb.append("\"fname\":\"").append(Dictionaries.fnamesById.get(ArrayDataAllocator.fnamesGlobal[id])).append("\",");
                }
                if (f.getClass().equals(SNameFilter.class) && ArrayDataAllocator.snameGlobal[id] != Short.MAX_VALUE) {
                    sb.append("\"sname\":\"").append(Dictionaries.snamesById.get(ArrayDataAllocator.snameGlobal[id])).append("\",");
                }
                if (f.getClass().equals(PhoneFilter.class) && ArrayDataAllocator.phoneGlobal[id] != Integer.MAX_VALUE) {
                    final String phone = Long.toString(ArrayDataAllocator.phoneGlobal[id]);
                    sb.append("\"phone\":\"8(").append(phone.substring(1,4)).append(")").append(phone.substring(4)).append("\",");
                }
                if (f.getClass().equals(SexFilter.class)) {
                    sb.append("\"sex\":\"").append(ArrayDataAllocator.sexGlobal[id] == 1 ? "m" : "f").append("\",");
                }

                if (f.getClass().equals(PremiumFilter.class) && ArrayDataAllocator.premiumStart[id] != 0) {
                    sb.append("\"premium\":{\"start\":").append(ArrayDataAllocator.premiumStart[id])
                            .append(",\"finish\":").append(ArrayDataAllocator.premiumFinish[id])
                            .append("},");
                }
                if (f.getClass().equals(BirthFilter.class)) {
                    sb.append("\"birth\":").append(DirectMemoryAllocator.getBirthDateFromFS(id)).append(",");
                }

                if (f.getClass().equals(CountryBitMapFilter.class) && ArrayDataAllocator.countryGlobal[id] != Short.MAX_VALUE) {
                    sb.append("\"country\":\"").append(Dictionaries.countriesById.get(ArrayDataAllocator.countryGlobal[id])).append("\",");
                }
                if (f.getClass().equals(CityFilter.class) && ArrayDataAllocator.cityGlobal[id] != Short.MAX_VALUE) {
                    sb.append("\"city\":\"").append(Dictionaries.citiesById.get(ArrayDataAllocator.cityGlobal[id])).append("\",");
                }
                if (f.getClass().equals(StatusFilter.class)) {
                    sb.append("\"status\":\"").append(InternalAccount.getStatus(ArrayDataAllocator.statusGlobal[id])).append("\",");
                }
            }
            sb.append("\"email\":\"").append(ArrayDataAllocator.emailsPlain[id]).append("\",");
            sb.append("\"id\":").append(id);
            //a.premium = new Account.Premium(ArrayDataAllocator.premiumStart[id], ArrayDataAllocator.premiumStart[id] + ArrayDataAllocator.premiumFinish[id]);

            sb.append("},");
            //a.email = ArrayDataAllocator.;
        }
        if (sb.charAt(sb.length() - 1) != '[') {
            sb.setLength(sb.length() - 1); //remove last "," symbol
        }

        sb.append("]}");

        Response response = Response.ok(Utf8.toBytes(sb.toString()));
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    final static TreeSet<GroupBitmapsAggregator.GroupNode> EMPTY_SET = new TreeSet<>();

    final static Set<String> VALID_KEY_SET = new TreeSet<String>() {{
        add("sex"); add("status"); add("interests"); add("country"); add("city");}};

    ///accounts/group/?query_id=991&keys=interests&joined=2013&limit=30&order=1
    public void processGroupRequest(final Request request, final HttpSession session) throws Exception {

        long start = System.nanoTime();
        //long reqId = counter.incrementAndGet();
        //logger.error("enter request{}  thread{}", reqId, Thread.currentThread().getId());
        try {
            int direction = 1;
            String keys = null;
            int limit = 0;

            //TODO WTF???
            Iterator<Map.Entry<String, String>> i = request.getParameters().iterator();
            while (i.hasNext()) {
                Map.Entry<String, String> e = i.next();
                if (e.getKey().equalsIgnoreCase("order")) {
                    String order = e.getValue();
                    if (order == null || order.isEmpty()) {
                        returnBad(session);
                        return;
                    }
                    direction = Integer.parseInt(order);
                } else if (e.getKey().equalsIgnoreCase("keys")) {
                    keys = e.getValue();
                } else if (e.getKey().equalsIgnoreCase("limit")) {
                    String l = e.getValue();
                    if (l == null || l.isEmpty() || !l.chars().allMatch(Character::isDigit)) {
                        returnBad(session);
                        return;
                    }
                    limit = Integer.parseInt(l);
                }
            }

            String[] keysList = keys.split(",");

            for (String key : keysList) {
                if (!VALID_KEY_SET.contains(key)) {
                    returnBad(session);
                    return;
                }
            }
            final List<BaseFilter> filters = Filters.getFiltersByPriority(request.getParameters(), limit, GroupBitmapsAggregator.GROUP_EXCLUDE_KEYS);

            if (filters.size() == 1 && filters.get(0).getClass().equals(EmptyFilter.class)) {
                totalTime.addAndGet(System.nanoTime() - start);
                Response response = Response.ok(Utf8.toBytes("{\"groups\":[]}"));
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
                return;
            }

            TreeSet<GroupBitmapsAggregator.GroupNode> groups = EMPTY_SET;
            //logger.error("enter request{}  thread{} ; keys - {}", reqId, Thread.currentThread().getId(), keys);

            groups = GroupBitmapsAggregator.getGroups(filters, keys, request.getParameters(), direction, limit);

            if (groups == null) {
                //logger.error("request{}; get - 0", reqId);
                totalTime.addAndGet(System.nanoTime() - start);
                returnBad(session);
                return;
            } else if (groups.isEmpty()) {
                totalTime.addAndGet(System.nanoTime() - start);
                Response response = Response.ok(Utf8.toBytes("{\"groups\":[]}"));
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
                return;
            }
            //logger.error("request{}; get - {}", reqId, tmp.size());


            StringBuilder sb = new StringBuilder();
            sb.append("{\"groups\": [");

            Iterator<GroupBitmapsAggregator.GroupNode> iterator = direction > 0 ? groups.iterator() : groups.descendingIterator();

            //groups.forEach( gr -> {
            //    logger.error("gr keys - {}, ids - {}, count - {}", gr.keys, gr.keysIds, gr.count);
            //});

            boolean useTLMap = false;
            if (filters.size() > 0) {
                useTLMap = true;
            }


            Map<Integer, Integer> nodeCounts = nodeCountsfTL.get();

            int count = 0;
            while (iterator.hasNext()) {
                if (count == limit) {
                    break;
                }
                count++;

                GroupBitmapsAggregator.GroupNode n = iterator.next();

                sb.append("{");
                if (keysList.length == 1) {
                    if (n.keys[0] != null && !n.keys[0].isEmpty()) {
                        sb.append("\"").append(keysList[0]).append("\": \"").append(n.keys[0]).append("\",");
                    }
                } else {
                    for (int j = 0; j < keysList.length; j++) {
                        if (n.keys[j] != null && !n.keys[j].isEmpty()) {
                            sb.append("\"").append(keysList[j]).append("\": \"").append(n.keys[j]).append("\",");
                        }
                    }
                }
                //if (filters.size() == 0) {
                if (!useTLMap) {
                    sb.append("\"count\": ").append(n.count).append("},");
                } else {
                    sb.append("\"count\": ").append(nodeCounts.get(n.uniqId)).append("},");
                }
                //} else {
                //    int c = GroupAggregator.getCountForFilters(keysList, n, filters);
                //    sb.append("\"count\": ").append(c).append("},");
                //}
            }
            totalTime.addAndGet(System.nanoTime() - start);

            sb.setLength(sb.length() - 1); //remove last "," symbol
            sb.append("]}");

            Response response = Response.ok(Utf8.toBytes(sb.toString()));
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
        } catch (Exception e) {
            //logger.error("", e);
            Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);
        } finally {
        }
    }

    public static void returnBad(final HttpSession session) throws Exception {
        Response response = new Response(Response.BAD_REQUEST, "{}".getBytes());
        response.addHeader("Content-Type: application/json");
        session.sendResponse(response);
    }

    public static void createServer() throws Exception {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.reusePort = true;

        acceptor.port = 80;
        acceptor.tcpFastOpen = true;

        //acceptor.recvBuf = 10000;
        //acceptor.sendBuf = 10000;

        config.acceptors = new AcceptorConfig[]{acceptor};
        config.selectors = 4;
        config.minWorkers = 0;
        config.maxWorkers = 0;
        config.threadPriority = Thread.MAX_PRIORITY;
        config.affinity = true;

        OneNioServer server = new OneNioServer(config);
        server.start();
    }

}
