package com.highloadcup;

import com.google.gson.*;
import com.highloadcup.filters.*;
import com.highloadcup.model.Account;
import com.highloadcup.model.InternalAccount;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.concurrent.*;

import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.OneNioServer.returnBad400;
import static com.highloadcup.OneNioServer.returnBad404;

public class AccountUpdater {

    private static final Logger logger = LogManager.getLogger(AccountUpdater.class);

    private static Gson gson = new Gson();


    public static final ArrayBlockingQueue<Runnable> loverUpdates = new ArrayBlockingQueue<Runnable>(70000);

    public static final ExecutorService executorServiceLU = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            loverUpdates,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r);
                }
            });


    public static ThreadLocal<char[]> bodyTL = new ThreadLocal<char[]>() {
        @Override
        protected char[] initialValue() {
            return new char[10000];
        }
    };

    public static char[] getCharBody(byte[] body, int length) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(body, 0, length);
        Charset cs = Charset.forName("UTF-8"); //UTF_8
        CharsetDecoder cd = cs.newDecoder();

        char[] chars = bodyTL.get();
        DirectMemoryAllocator.clear(chars);

        CharBuffer cb = CharBuffer.wrap(chars, 0, length);
        cd.decode(byteBuffer, cb, true);
        return chars;
    }

    public static void processNewAccount(final Request request, final HttpSession session, String uri) throws Exception {
        int id = 0;
        final byte[] bytesBody = request.getBody();
        final char[] body = getCharBody(bytesBody, request.contentLength);
        try {
            //logger.error("body - {}", new String(body));

            //logger.error("body - {}", new String(body, 0, OneNioServer.contentLengthTL.get()));
            Account acc = JsonParser.parseAccountUpdate(body, request.contentLength);

            if (ACCOUNTS_LOADED.get() >= acc.id || MAX_ACCOUNTS < acc.id) {
                //logger.error("total > id - {} {}", ACCOUNTS_LOADED.get(), acc.id);
                returnBad400(request, session);
                return;
            }

            id = acc.id;

            synchronized (ArrayDataAllocator.emailsGlobal) {
                if (ArrayDataAllocator.emailsGlobal.containsKey(acc.email)) {
                   // logger.error("emailsGlobal contains {}", acc.email);
                    returnBad400(request, session);
                    return;
                }
            }

            //Dictionaries.fillDictsForOneOnAddOrUpdateUnescaped(acc);


            InternalAccount account = InternalAccount.convertFromHashes(acc, true);

            ArrayDataAllocator.fillSingleData(account);

            setMaxAccId(account.id);


            if (acc.likes != null) {
                //synchronized (LikesUpdater.class) {
                    LikesUpdater.processUpdateNewAccLikes(session, acc.id, acc.likes);
                //}
            }

            acc = null;

            ACCOUNTS_TOTAL.incrementAndGet();

            //UpdateProcessor.processUpdateAccount(o, id);

            //TODO
            //synchronized (Filters.class) {
                Filters.FILTERS_BY_CLASS.forEach((key, val) -> {
                    synchronized (val.lock) {
                        val.processNew(account);
                    }
                });
            //}

            account.isPremiumNow = DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, account.id);


           // executorServiceLU.execute(new Runnable() {
             //   @Override
               // public void run() {
                    synchronized (Recomendator.class) {
                        Recomendator.processNewLover(account);
                    }
                //}
            //});

            Response response = new Response(Response.CREATED, "{}".getBytes());
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);

        } catch (Exception e) {
            returnBad400(request, session);
            //logger.error("content length h - {}, val - {}", request.getHeader("Content-Length"), request.contentLength);
            //logger.error("processNewAccount err for id {}, cl - {}, body - {}", id, OneNioServer.contentLengthTL.get(), body, e);
            //final char[] bbb = getCharBody(bytesBody, 5000);
            //logger.error("body {}", bbb);

        }
    }

    public static void processUpdateAccount(final Request request, final HttpSession session, String uri) throws Exception {
        try {
            final byte[] byteBody = request.getBody();
            final char[] body = getCharBody(byteBody, request.contentLength);
            Account acc = JsonParser.parseAccountUpdate(body, request.contentLength);

            int id = 0;

            try {
                id = Integer.parseInt(uri.substring(10, uri.lastIndexOf("/"))); ///accounts/
            } catch (RuntimeException e) {
                returnBad404(session);
                return;
            }

            if (MAX_ACC_ID_ADDED.get() < id || allAccounts[id] != id) {
                returnBad404(session);
                return;
            }
            acc.id = id;

            //changed flags
            boolean emailChanged = false;
            boolean sNameChanged = false;
            boolean fNameChanged = false;
            boolean statusChanged = false;
            boolean phoneChanged = false;
            boolean countryChanged = false;
            boolean cityChanged = false;
            boolean premiumChanged = false;
            boolean interestsChanged = false;
            boolean likesChanged = false;
            boolean needUpdateLover = false;

            ShortArray oldItr = accountsInterests[acc.id];
            short oldCountry = countryGlobal[acc.id];
            short oldCity = cityGlobal[acc.id];
            byte oldStatus = statusGlobal[acc.id];
            boolean wasPremium = false;

            synchronized (ArrayDataAllocator.emailsGlobal) {
                if (acc.email != null) {
                    Integer emailId = ArrayDataAllocator.emailsGlobal.get(acc.email);
                    if (emailId != null && emailId != id) {
                        returnBad400(session);
                        return;
                    } else if (emailId == null) {
                        emailChanged = true;
                    }
                }
            }
            //Dictionaries.fillDictsForOneOnAddOrUpdateUnescaped(acc);

            InternalAccount account = InternalAccount.convertForUpdateFromHashes(acc);

            account.sex = sexGlobal[id];
            account.birth = DirectMemoryAllocator.getBirthDateFromFS(acc.id);

            //synchronized (Filters.class) {
                if (emailChanged) {
                    synchronized (Filters.FILTERS_BY_CLASS.get(EmailFilter.class).lock) {
                        Filters.FILTERS_BY_CLASS.get(EmailFilter.class).processUpdate(account);
                        emailsGlobal.remove(emailsPlain[acc.id]);
                        emailsGlobal.put(account.email, account.id);
                        emailsPlain[acc.id] = account.email;
                        emailDomains[acc.id] = Dictionaries.emailDomainsMap.get(account.email.toLowerCase().trim().split("@")[1]);
                    }
                } else {
                    account.email = emailsPlain[acc.id];
                }

                if (acc.snameHash != 0) {
                    short newIdx = account.sname;
                    short oldIdx = snameGlobal[acc.id];
                    if (newIdx != oldIdx) {
                        sNameChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(SNameFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(SNameFilter.class).processUpdate(account);
                            snameGlobal[acc.id] = newIdx;
                        }
                    }
                }

                if (acc.fnameHash != 0) {
                    short newIdx = account.fname;
                    short oldIdx = fnamesGlobal[acc.id];
                    if (newIdx != oldIdx) {
                        fNameChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(FNameFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(FNameFilter.class).processUpdate(account);
                            fnamesGlobal[acc.id] = newIdx;
                        }
                    }

                }

                if (acc.countryHash != 0) {
                    short newIdx = account.country;
                    if (newIdx != oldCountry) {
                        countryChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(CountryBitMapFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(CountryBitMapFilter.class).processUpdate(account);
                            countryGlobal[acc.id] = newIdx;
                            needUpdateLover = true;
                        }
                    }
                } else {
                    account.country = countryGlobal[acc.id];
                }

                if (acc.cityHash != 0) {
                    short newIdx = account.city;
                    if (newIdx != oldCity) {
                        cityChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(CityFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(CityFilter.class).processUpdate(account);
                            cityGlobal[acc.id] = newIdx;
                            needUpdateLover = true;
                        }
                    }
                } else {
                    account.city = cityGlobal[acc.id];
                }

                if (acc.statusHash != 0) {
                    byte newStatus = account.status;

                    if (oldStatus != newStatus) {
                        statusChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(StatusFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(StatusFilter.class).processUpdate(account);
                            statusGlobal[acc.id] = newStatus;
                            needUpdateLover = true;
                        }
                    }
                } else {
                    account.status = statusGlobal[acc.id];
                }

                if (acc.phoneNum != 0) {
                    long oldPhone = phoneGlobal[acc.id];
                    long newPhone = account.phone;

                    if (oldPhone != newPhone) {
                        phoneChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(PhoneFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(PhoneFilter.class).processUpdate(account);
                        }
                    }
                    phoneGlobal[acc.id] = newPhone;
                } else {
                    account.phone = phoneGlobal[acc.id];
                }

                if (account.premiumStart > 0 && account.premiumFinish > 0) {
                    long oldPremiumStart = premiumStart[acc.id];
                    long oldPremiumFinish = premiumFinish[acc.id];

                    wasPremium = PremiumFilter.isPremium(oldPremiumStart, oldPremiumFinish);

                    if (oldPremiumStart != account.premiumStart || oldPremiumFinish != account.premiumFinish) {
                        premiumChanged = true;
                        synchronized (Filters.FILTERS_BY_CLASS.get(PremiumFilter.class).lock) {
                            Filters.FILTERS_BY_CLASS.get(PremiumFilter.class).processUpdate(account);
                        }
                    }
                    premiumStart[acc.id] = account.premiumStart;
                    premiumFinish[acc.id] = account.premiumFinish;
                    needUpdateLover = true;
                } else {
                    account.premiumStart = premiumStart[acc.id];
                    account.premiumFinish = premiumFinish[acc.id];
                }

                if (account.interestsNumber > 0) {
                    synchronized (Filters.FILTERS_BY_CLASS.get(InterestsBitMapFilter.class).lock) {
                        Filters.FILTERS_BY_CLASS.get(InterestsBitMapFilter.class).processUpdate(account);
                    }
                    interestsChanged = true;
                    needUpdateLover = true;
                }
           // }

            if (acc.likes != null) {
                //synchronized (LikesUpdater.class) {
                LikesUpdater.processUpdateNewAccLikes(session, acc.id, acc.likes);
                //}
            }

            final boolean wasPremiumF = wasPremium;

            if (needUpdateLover) {
               // executorServiceLU.execute(new Runnable() {
                 //   @Override
                   // public void run() {
                synchronized (Recomendator.class) {
                    Recomendator.processUpdateLover(account, oldCity, oldCountry, oldItr, oldStatus, wasPremiumF);
                }
                    //}
                //});
            }


            Response response = new Response(Response.ACCEPTED, "{}".getBytes());
            response.addHeader("Content-Type: application/json");
            session.sendResponse(response);

        } catch (Exception e) {
            //logger.error("update err; body - {}: ", body, e);
            returnBad400(session);
        }
    }

}
