package com.highloadcup;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.LinkedTreeMap;
import com.highloadcup.filters.*;
import com.highloadcup.model.Account;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.RequestExecutorProvider;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.ContentType;
import org.glassfish.grizzly.http.util.Header;
import org.glassfish.grizzly.http.util.HeaderValue;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;
import static com.highloadcup.ArrayDataAllocator.emailsGlobal;
import static com.highloadcup.filters.Filters.transformBitMapToSet;
import static com.highloadcup.filters.Filters.validateFilterParams;

public class RequestHandler extends HttpHandler {

    private static final Logger logger = LogManager.getLogger("Handler");

    public static final HeaderValue SERVER_VERSION =
            HeaderValue.newHeaderValue("GRZLY").prepare();

    static final RequestExecutorProvider EXECUTOR_PROVIDER =
            new RequestExecutorProvider.SameThreadProvider();

    private static final ContentType CONTENT_TYPE_JSON =
            ContentType.newContentType("application/json", "utf-8").prepare();

    private static final ContentType CONTENT_TYPE_HTML =
            ContentType.newContentType("text/html", "utf-8").prepare();

    public static Gson gson = new Gson();

    static long allTime = 0;
    static long count = 1;

    @Override
    public void service(final Request request, final Response response)
            throws Exception {
        //ThreadContext.put("request", UUID.randomUUID().toString());
        response.setHeader(Header.Server, SERVER_VERSION);

        //logger.info("remote addr - " + request.getRemoteAddr());
        //if (Method.GET != request.getMethod() && Method.POST != request.getMethod()) {
            //logger.warn("Incorrect request - not GET or POST method!");
        //    response.getOutputStream().write("Use GET&POST methods!".getBytes());
        //} else {
        //long start = System.nanoTime();
            processRequest(request, response);
        //logger.error("time: {}", System.nanoTime() - start);
            //allTime +=  System.nanoTime() - start;
        //count++;
        //}
        //if (count % 1000 == 0) {
        //    logger.error("mediana: {}", allTime / count);
        //}
        //ThreadContext.clearAll();
    }

    /*
    private static String getRequestEncoding(final HttpServletRequest req) {
        String encoding = "UTF-8";
        if (req.getServletPath().contains("cp")) {
            encoding = "cp1251";
        }
        return encoding;
    }

    private static InputStream getInputStreamGZipIfNeed(final HttpServletRequest req) throws IOException {

        // Обрабатываем запрос в gzip
        if ("gzip".equals(req.getHeader("Content-Encoding"))) {
            return new GZIPInputStream(req.getInputStream());
        } else {
            return req.getInputStream();
        }
    }

    public void readReq() {
        StringWriter writer = new StringWriter();
        try {
            IOUtils.copy(request.getInputStream(), writer, "utf-8");
        } catch (IOException e1) {
            //ignore
        }
        String sParams = writer.toString();

    }

    private static String readRequest(final InputStream is, final String encoding) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BOMInputStream bis = new BOMInputStream(is, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_32BE, ByteOrderMark.UTF_32LE);
        IOUtils.copy(bis, baos);
        return new String(baos.toByteArray(), encoding);
    }
*/

    final static Pattern EMAIL_PTR =
            Pattern.compile("(?:(?:\\r\\n)?[ \\t])*(?:(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*)|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*:(?:(?:\\r\\n)?[ \\t])*(?:(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*)(?:,\\s*(?:(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*|(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)*\\<(?:(?:\\r\\n)?[ \\t])*(?:@(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*)*:(?:(?:\\r\\n)?[ \\t])*)?(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\"(?:[^\\\"\\r\\\\]|\\\\.|(?:(?:\\r\\n)?[ \\t]))*\"(?:(?:\\r\\n)?[ \\t])*))*@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*)(?:\\.(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\[\\] \\000-\\031]+(?:(?:(?:\\r\\n)?[ \\t])+|\\Z|(?=[\\[\"()<>@,;:\\\\\".\\[\\]]))|\\[([^\\[\\]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[ \\t])*))*\\>(?:(?:\\r\\n)?[ \\t])*))*)?;\\s*)");

    final static Pattern PHONE_PTR = Pattern.compile("\\d{10}|(?:\\d{3}-){2}\\d{4}|\\(\\d{3}\\)\\d{3}-?\\d{4}");

    final static Pattern NAME_PTR = Pattern.compile("[а-яА-ЯёЁ]+");

    static boolean validateNewAccount(Account a) {

        if (a.id == 0 || ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1 < a.id) { //TODO
            return false;
        }
        if (a.sex == null || a.sex.length() > 1 || (!a.sex.equalsIgnoreCase("m") && !a.sex.equalsIgnoreCase("f")) ||
                a.email == null || a.email.length() > 100 || !EMAIL_PTR.matcher(a.email).matches() || //checkUniq
                a.status == null ||
                a.birth <= 0 || //Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым
                (a.fname != null && a.fname.length() > 50) || (a.fname != null && !NAME_PTR.matcher(a.fname).matches()) ||
                (a.sname != null && a.sname.length() > 50) || (a.sname != null && !NAME_PTR.matcher(a.sname).matches()) ||
                (a.phone != null && a.phone.length() > 16) || (a.phone != null && !PHONE_PTR.matcher(a.phone).matches()) ||
                (a.city != null && a.city.length() > 50) || (a.city != null && !NAME_PTR.matcher(a.city).matches()) ||
                (a.country != null && a.country.length() > 50) || (a.country != null && !NAME_PTR.matcher(a.country).matches()) ||
                a.joined <= 0 || //снизу 01.01.2011, сверху 01.01.2018.
                a.status == null || a.status.length() < 6 || a.status.length() > 10 || InternalAccount.getStatus(a.status) == 0
                //TODO
                ) {
            return false;
        }
        //.chars().allMatch( Character::isDigit )

        return true;
    }

    static boolean validateUpdateAccount(TreeMap<String, Object> a, int id) {
        try {
            if (a.containsKey("fname")) {
                String fname = (String) a.get("fname");
                if ((fname != null && !fname.isEmpty()) && (
                        fname.length() > 50) || !NAME_PTR.matcher(fname).matches()) {
                    return false;
                }
            }
            if (a.containsKey("sname")) {
                String sname = (String) a.get("sname");
                if ((sname != null && !sname.isEmpty()) && (
                        sname.length() > 50) || !NAME_PTR.matcher(sname).matches()) {
                    return false;
                }
            }
            if (a.containsKey("email")) {
                String email = (String) a.get("email");
                if ((email != null && !email.isEmpty()) && (
                        email.length() > 100) || !EMAIL_PTR.matcher(email).matches()) {
                    return false;
                }
                if (emailsGlobal.containsKey(email.trim().toLowerCase()) &&
                        emailsGlobal.get(email.trim().toLowerCase()).compareTo((id)) != 0) {
                    return false;
                }
            }
            if (a.containsKey("phone")) {
                String phone = (String) a.get("phone");
                if ((phone != null && !phone.isEmpty()) && (
                        phone.length() > 16) || !PHONE_PTR.matcher(phone).matches()) {
                    return false;
                }
            }
            if (a.containsKey("country")) {
                String country = (String) a.get("country");
                if ((country != null && !country.isEmpty()) && (
                        country.length() > 50) || !NAME_PTR.matcher(country).matches()) {
                    return false;
                }
            }
            if (a.containsKey("city")) {
                String city = (String) a.get("city");
                if ((city != null && !city.isEmpty()) && (
                        city.length() > 50) || !NAME_PTR.matcher(city).matches()) {
                    return false;
                }
            }
            if (a.containsKey("status")) {
                String status = (String) a.get("status");
                if ((status != null && !status.isEmpty()) && (
                        status.length() < 6) || status.length() > 10 || InternalAccount.getStatus(status) == 0) {
                    return false;
                }
            }
            if (a.containsKey("premium")) {
                LinkedTreeMap dates = (LinkedTreeMap) a.get("premium");
                if (!dates.containsKey("start") || !dates.containsKey("finish")) {
                    return false;
                }
            }
            //.chars().allMatch( Character::isDigit )
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void processNewAccount(final Request request, final Response response) throws Exception {
        try {
            Account account = gson.fromJson(request.getReader(), Account.class);
            if (!validateNewAccount(account)) {
                response.setStatus(400, "BAD");
            } else {
                UpdateProcessor.processNewAccount(account);
                response.setStatus(201, "OK");
            }
        } catch (JsonSyntaxException |JsonIOException e) {
            response.setStatus(201, "OK");
        }
    }

    private void processUpdateAccount(final Request request, final Response response, String uri) throws Exception {
        try {
            TreeMap o = gson.fromJson(request.getReader(), TreeMap.class);
            int id = Integer.parseInt(uri.substring(10, uri.lastIndexOf("/"))); ///accounts/
            if (ACCOUNTS_TOTAL.get() < id) { //TODO MAX_ACC_ID_ADDED
                response.setStatus(404, "BAD");
                return;
            }
            if (!validateUpdateAccount(o, id)) {
                response.setStatus(400, "BAD");
            } else {
                UpdateProcessor.processUpdateAccount(o, id);
                response.setStatus(202, "OK");
            }
        } catch (JsonSyntaxException |JsonIOException e) {
            response.setStatus(400, "OK");
        }
    }

    private void processRequest(final Request request, final Response response) throws Exception {
        try {
            request.setCharacterEncoding("UTF-8");
            final String uri = request.getDecodedRequestURI();
            //logger.info(uri);

            //final Accounts accounts = new Accounts();

            if (Method.POST == request.getMethod()) {
                if (uri.contains("/accounts/new")) {
                    processNewAccount(request, response);
                } else if (uri.contains("/accounts/likes")) {
                    response.setStatus(202, "OK");
                } else if (uri.contains("/accounts/")) {
                    processUpdateAccount(request, response, uri);
                }
                response.setContentType(CONTENT_TYPE_JSON);
                response.setHeader("Connection", "keep-alive");
                response.setHeader("Transfer-Encoding", "identity");
                response.getNIOWriter().write("{}");
                //logger.error("send post {}");
            } else if (uri.contains("/accounts/filter/")) { //не нужно выводить данные по interests и externalIdLikes
                //пользователи в результате должны быть отсортированы по убыванию значений в поле id
                //GET: /accounts/filter/?status_neq=всё+сложно&birth_lt=643972596&country_eq=Индляндия&limit=5&query_id=110

                //Для каждой подошедшей записи аккаунта не нужно передавать все известные о ней данные,
                // а только поля id, email и те, что были использованы в запросе.

                try {
                    response.setStatus(200, "OK");
                    response.setContentType(CONTENT_TYPE_JSON);
                    if (Method.GET == request.getMethod()) {

                        //Parameters p = request.getParameters();
                        final Map<String,String[]> params = request.getParameterMap();

                        if (!validateFilterParams(params) ||
                                !RequestHelpers.checkForNumeric(params, "limit")) {
                            response.setStatus(400, "Data Error");
                            response.setContentType(CONTENT_TYPE_JSON);
                            response.getOutputStream().write("{}".getBytes());
                            return;
                        }

                        final int limit = Integer.parseInt(request.getParameter("limit"));
                        final List<BaseFilter> filters = Filters.getFiltersByPriority(params, limit);

                        if (filters == null) {
                            response.setStatus(400, "Data Error");
                            response.setContentType(CONTENT_TYPE_JSON);
                            response.getOutputStream().write("{}".getBytes());
                            return;
                        }
                        long[] bitSet = Filters.applyFilters(filters);
                        final int[] finalSet = transformBitMapToSet(bitSet, limit);
                        sendResponse(response, finalSet, limit, filters);
                    }
                } catch (RuntimeException e) {
                    logger.error("error error: ", e);
                    response.setStatus(400, "System Error");
                    response.setContentType(CONTENT_TYPE_JSON);
                    response.getOutputStream().write("{}".getBytes());
                    return;
                }
            } else if (uri.contains("/accounts/group/")) { //не нужно выводить данные по interests и externalIdLikes
                //Полей для группировки всего пять - sex, status, interests, country, city.
                response.setContentType(CONTENT_TYPE_JSON);
                response.setStatus(200, "OK");
                processGroupRequest(request.getParameterMap(), response);
                response.getOutputStream().write("{}".getBytes());
            } else if (uri.contains("/accounts/")) { //не нужно выводить данные по interests и externalIdLikes
                // /accounts/new/
                response.setContentType(CONTENT_TYPE_JSON);
                response.setStatus(202, "OK");
                response.getOutputStream().write("{}".getBytes());
            } else {
                response.setContentType(CONTENT_TYPE_JSON);
                //logger.warn("Incorrect request - wrong URI!");
                response.getOutputStream().write("{}".getBytes());
            }
        } catch (RuntimeException e) {
            logger.error("process request error: ", e);
            response.setStatus(400, "Server Error");
            response.setContentType(CONTENT_TYPE_JSON);
            response.getOutputStream().write("{}".getBytes());
        }

    }

    public static void returnBad(final Response response) throws Exception {
        response.setStatus(400, "BAD");
        response.setContentType(CONTENT_TYPE_JSON);
        response.setHeader("Connection",  "keep-alive");
        response.setHeader("Transfer-Encoding", "identity");
        response.getNIOWriter().write("{}");
    }

    ///accounts/group/?query_id=991&keys=interests&joined=2013&limit=30&order=1
    public void processGroupRequest(Map<String,String[]> params, final Response response) throws Exception {
        if (!RequestHelpers.checkForNumeric(params, "limit") ||
                !RequestHelpers.checkForNumeric(params, "order") ||
                !RequestHelpers.checkForExists(params, "keys")) {
            returnBad(response);
            return;
        }
        int limit = Integer.parseInt(params.get("limit")[0]);
        params.remove("limit");
        params.remove("query_id");
        int direction = Integer.parseInt(params.get("order")[0]);
        params.remove("order");

        String keys = params.get("keys")[0];
        params.remove("keys");

        List<GroupItem> groups = null; //GroupAggregator.getGroups(keys, limit, direction, params);

        if (groups == null || groups.size() == 0) {
            returnBad(response);
            return;
        }

        groups = groups.stream().filter(g -> g.count != 0).collect(Collectors.toList());

        if (groups == null || groups.size() == 0) {
            returnBad(response);
            return;
        }

        Collections.sort(groups, new Comparator<GroupItem>() {
            @Override
            public int compare(GroupItem o1, GroupItem o2) {
                if (direction == 1) {
                    int countCompare = Integer.compare(o1.count, o2.count);
                    if (countCompare != 0) {
                        return countCompare;
                    } else {
                        return o1.keys[0].compareTo(o2.keys[0]);
                    }
                } else {
                    int countCompare = Integer.compare(o2.count, o1.count);
                    if (countCompare != 0) {
                        return countCompare;
                    } else {
                        return o2.keys[0].compareTo(o1.keys[0]);
                    }
                }
            }
        });

        groups = groups.subList(0, limit > groups.size() ? groups.size() : limit);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"groups\": [");
        groups.forEach(g -> {
            sb.append("{");
            if (g.keys[0] != null && !g.keys[0].isEmpty()) {
            sb.append("\"").append(keys).append("\": \"").append(g.keys[0]).append("\",");
            }
            sb.append("\"count\": ").append(g.count).append("},");
        });
        sb.setLength(sb.length() - 1); //remove last "," symbol
        sb.append("]}");

        response.setContentType(CONTENT_TYPE_JSON);
        response.setHeader("Connection",  "keep-alive");
        response.setHeader("Transfer-Encoding", "identity");
        response.getNIOWriter().write(sb.toString());
        //logger.error(sb.toString());
    }

    static ThreadLocal<StringBuilder> stringBuilderThreadLocal = ThreadLocal.withInitial(() -> new StringBuilder(10000 * 50));

    public void sendResponse(final Response response, int[] finalSet, int limit, List<BaseFilter> filters) throws IOException {

        if (finalSet == null || finalSet.length == 0) {
            response.getOutputStream().write("{\"accounts\": []}".getBytes());
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
                    sb.append("{");

                    if (filters.stream().anyMatch( f -> f.getClass().equals(FNameFilter.class)) &&
                            ArrayDataAllocator.fnamesGlobal[id] != Short.MAX_VALUE) {
                        sb.append("\"fname\":\"").append(Dictionaries.fnamesById.get(ArrayDataAllocator.fnamesGlobal[id])).append("\",");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(SNameFilter.class)) &&
                            ArrayDataAllocator.snameGlobal[id] != Short.MAX_VALUE) {
                        sb.append("\"sname\":\"").append(Dictionaries.snamesById.get(ArrayDataAllocator.snameGlobal[id])).append("\",");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(PhoneFilter.class)) &&
                            ArrayDataAllocator.phoneGlobal[id] != Integer.MAX_VALUE) {
                                final String phone = Long.toString(ArrayDataAllocator.phoneGlobal[id]);
                                sb.append("\"phone\":\"8(").append(phone.substring(1,4)).append(")").append(phone.substring(4)).append("\",");
                                //sb.append("\"phone\":\"").append(ArrayDataAllocator.phoneGlobal[id]).append("\",");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(SexFilter.class))) {
                            sb.append("\"sex\":\"").append(ArrayDataAllocator.sexGlobal[id] == 1 ? "m" : "f").append("\",");
                    }

                    if (filters.stream().anyMatch( f -> f.getClass().equals(PremiumFilter.class)) &&
                            ArrayDataAllocator.premiumStart[id] != 0) {
                            sb.append("\"premium\":{\"start\":").append(ArrayDataAllocator.premiumStart[id])
                                    .append(",\"finish\":").append(ArrayDataAllocator.premiumFinish[id])
                                    .append("},");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(BirthFilter.class))) {
                            sb.append("\"birth\":").append(DirectMemoryAllocator.getBirthDateFromFS(id)).append(",");
                    }

                    sb.append("\"email\":\"").append(ArrayDataAllocator.emailsPlain[id]).append("\",");

                    if (filters.stream().anyMatch( f -> f.getClass().equals(CountryBitMapFilter.class)) &&
                            ArrayDataAllocator.countryGlobal[id] != Short.MAX_VALUE) {
                            sb.append("\"country\":\"").append(Dictionaries.countriesById.get(ArrayDataAllocator.countryGlobal[id])).append("\",");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(CityFilter.class)) &&
                            ArrayDataAllocator.cityGlobal[id] != Short.MAX_VALUE) {
                            sb.append("\"city\":\"").append(Dictionaries.citiesById.get(ArrayDataAllocator.cityGlobal[id])).append("\",");
                    }
                    if (filters.stream().anyMatch( f -> f.getClass().equals(StatusFilter.class))) {
                        sb.append("\"status\":\"").append(InternalAccount.getStatus(ArrayDataAllocator.statusGlobal[id])).append("\",");
                    }

                    sb.append("\"id\":").append(id);
                    //a.premium = new Account.Premium(ArrayDataAllocator.premiumStart[id], ArrayDataAllocator.premiumStart[id] + ArrayDataAllocator.premiumFinish[id]);

                    sb.append("},");
                    //a.email = ArrayDataAllocator.;
                }
        if (sb.charAt(sb.length() - 1) != '[') {
            sb.setLength(sb.length() - 1); //remove last "," symbol
        }

        sb.append("]}");

        //byte[] output = sb.toString().getBytes();
        //response.getOutputStream().write(sb.toString().getBytes());

        //response.setContentLength(output.length);
        response.setContentType(CONTENT_TYPE_JSON);
        response.setHeader("Connection",  "keep-alive");
        response.setHeader("Transfer-Encoding", "identity");
        response.getNIOWriter().write(sb.toString());
        //response.flush();
        //response.getNIOOutputStream().write(output);
        //response.flush();

        //BufferedWriter bf = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
        //bf.append(sb);
        //bf.flush();

        /*char[] toOut = new char[sb.length()];
        sb.getChars(0, sb.length(), toOut, 0);


        ByteBuffer bb = Charset.forName("UTF-8").encode(CharBuffer.wrap(toOut));
        response.getOutputStream().write(bb.array());*/

    }

    @Override
    public RequestExecutorProvider getRequestExecutorProvider() {
        return EXECUTOR_PROVIDER;
    }

}
