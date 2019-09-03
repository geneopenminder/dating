package com.highloadcup;

import com.google.gson.*;
import com.highloadcup.model.Account;
import com.highloadcup.model.Likes;
import com.highloadcup.pool.LikesPool;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.ConcurrentLinkedQueueCollection;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.highloadcup.AccountUpdater.getCharBody;
import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.OneNioServer.returnBad400;
import static com.highloadcup.Suggester.likesForUser;


public class LikesUpdater {

    private static final Logger logger = LogManager.getLogger(LikesUpdater.class);

    private static Gson gson = new Gson();


    static PoolService<ArrayDataAllocator.LikeTS> pool = new ConcurrentPool<ArrayDataAllocator.LikeTS>(
            new ConcurrentLinkedQueueCollection<ArrayDataAllocator.LikeTS>(), new PoolObjectFactory<ArrayDataAllocator.LikeTS>() {
        @Override
        public ArrayDataAllocator.LikeTS create() {
            return new ArrayDataAllocator.LikeTS();
        }

        @Override
        public boolean readyToTake(ArrayDataAllocator.LikeTS poolObjectFactory) {
            return true;
        }

        @Override
        public boolean readyToRestore(ArrayDataAllocator.LikeTS poolObjectFactory) {
            return true;
        }

        @Override
        public void destroy(ArrayDataAllocator.LikeTS poolObjectFactory) {
            logger.error("destroy");

        }
    }, 5000, 100000, false);
        /*
    *
    liker - id того, кто выставил отметку симпатии;
    likee - id того, кто симпатичен;
    likeTS - время в формате timestamp, когда отметка была выставлена;*/


    public static void processUpdateNewAccLikes(final HttpSession session, int liker, List<Account.Like> likerLikes) throws Exception {

        boolean result = processUpdateNewAccLikes(liker, likerLikes);

        if (!result) {
            for (Account.Like l: likerLikes) {
                LikesPool.returnLike(l);
            }
        }
    }

    public static boolean processAddLikes(int[] likesArr) throws Exception {

        //likee -0, ts -1, liker - 2

        for(int i = 0; i < likesArr.length; i += 3) {
            int likee = likesArr[i];
            int ts = likesArr[i+1];
            int liker = likesArr[i+2];

            if (likee == 0 || liker == 0) {
                break; //TODO
            }

            if (likee > MAX_ACC_ID_ADDED.get() || liker > MAX_ACC_ID_ADDED.get() ||
                    liker <= 0 || likee <= 0 || ts <= 0) {
                return false;
            }
        }
        return Suggester.processAddNewLikes(likesArr);

        /*
        //merge global
        Map<Integer, List<Account.Like>> likesMap = new HashMap<>();

        for (int i = 0; i < NEW_LIKE_OFFSET; i++) {
            List<Account.Like> l = likesMap.get(actives[i]);

            if (l == null) {
                l = new ArrayList<>(10);
                likesMap.put(actives[i],l);
            }
            l.add(new Account.Like(passives[i], ts[i]));
        }

        compressLikesForAvgTS(likesMap);
        */

        //return true;
    }


    public static boolean processUpdateNewAccLikes(int liker, List<Account.Like> likes) throws Exception {
        //int[] actives = new int[likes.size()];
        //int[] passives = new int[likes.size()];
        //int[] ts = new int[likes.size()];

        //int NEW_LIKE_OFFSET = 0;

        for(Account.Like like: likes) {
            if (like.id > MAX_ACC_ID_ADDED.get() || like.id <= 0 || like.ts <= 0) {
                return false;
            }

            //actives[NEW_LIKE_OFFSET] = like.liker;
            //passives[NEW_LIKE_OFFSET] = like.likee;
            //ts[NEW_LIKE_OFFSET] = like.ts;
            //NEW_LIKE_OFFSET++;
        }
        return Suggester.processNewAccountLikesUpdates(liker, likes);

        /*
        //merge global
        Map<Integer, List<Account.Like>> likesMap = new HashMap<>();

        for (int i = 0; i < NEW_LIKE_OFFSET; i++) {
            List<Account.Like> l = likesMap.get(actives[i]);

            if (l == null) {
                l = new ArrayList<>(10);
                likesMap.put(actives[i],l);
            }
            l.add(new Account.Like(passives[i], ts[i]));
        }

        compressLikesForAvgTS(likesMap);
        */

        //return true;
    }

    public static void processAddLikes(final Request request, final HttpSession session, String uri) throws Exception {

        //String body = new String(request.getBody(), 0, OneNioServer.contentLengthTL.get());
        try {
            //ByteArrayInputStream stream = new ByteArrayInputStream(request.getBody());

            //Likes likes = gson.fromJson(new String(request.getBody()), Likes.class);
            //Likes likes = gson.fromJson(body, Likes.class);

            //body = body.replaceAll("\\s+","").replace("\n", "").replace("\r", "");
            //List<Likes.Like> likes =  JsonParser.parseLikes(body.toCharArray());

            final char[] body = getCharBody(request.getBody(), request.contentLength);

            int[] likeArray = JsonParser.parseLikesToArray(body);

            //Likes likes = gson.fromJson(body, Likes.class);

            boolean result = processAddLikes(likeArray);
            if (result) {
                Response response = new Response(Response.ACCEPTED, "{}".getBytes());
                response.addHeader("Content-Type: application/json");
                session.sendResponse(response);
            } else {
                returnBad400(session);
            }
        } catch (Exception e) {
            //logger.error("likes error;", e);
            returnBad400(session);
        }

    }

    static AtomicInteger tsCount = new AtomicInteger(0);

    static ThreadLocal<Map> mapTL = new ThreadLocal<Map>() {
        @Override
        protected Map initialValue() {
            return new HashMap<Integer, Integer>();
        }
    };


    public static void compressLikesForAvgTS(Map<Integer, List<Account.Like>> likesMap) {

        likesMap.forEach((key, value) -> {
            int liker = key;

            List<Account.Like> likesList = value;

            Map<Integer, Integer> offsets = new HashMap<>();

            LikeTS[] pairs = new LikeTS[likesList.size()];
            int[] accLikes = new int[likesList.size()];
            int[] accTs = new int[likesList.size()];

            int offset = 0;
            for (int o = 0; o < likesList.size(); o++) {
                Account.Like l = likesList.get(o);
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

            int[] newLikes = DirectMemoryAllocator.realloc(accLikes, offset);
            int[] newTs = DirectMemoryAllocator.realloc(accTs, offset);

            for (int j = 0; j < offset; j++) {
                newLikes[j] = pairs[j].accId;
                newTs[j] = pairs[j].ts;
            }


            for (int lu = 0; lu < newLikes.length; lu++) {
                int likeToAcc = newLikes[lu] & 0x00FFFFFF;
                int tsAvg = newTs[lu];

                Suggester.LikesForUser lfu = likesForUser[likeToAcc];

                if (lfu == null) {
                    lfu = new Suggester.LikesForUser();
                    likesForUser[likeToAcc] = lfu;
                } else if (lfu.number == lfu.ts.length) {
                    lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + 1);
                    lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + 1);
                }

                lfu.ids[lfu.number] = liker;
                lfu.ts[lfu.number] = tsAvg;
                lfu.number++;

                ArrayDataAllocator.LikeTS[] pairsTS = new ArrayDataAllocator.LikeTS[lfu.ids.length];

                for (int j = 0; j < lfu.ids.length; j++) {
                    pairsTS[j] = new ArrayDataAllocator.LikeTS(lfu.ids[j], lfu.ts[j]);
                }

                Arrays.sort(pairs, new Comparator<ArrayDataAllocator.LikeTS>() {
                    @Override
                    public int compare(ArrayDataAllocator.LikeTS o1, ArrayDataAllocator.LikeTS o2) {
                        return Integer.compare(o1.ts, o2.ts);
                    }
                });

                for (int k = 0; k < pairsTS.length; k++) {
                    lfu.ids[k] = pairsTS[k].accId;
                    lfu.ts[k] = pairsTS[k].ts;
                }
            }

            //int[] newLikes = DirectMemoryAllocator.realloc(accLikes, NEW_LIKE_OFFSET);
            //int[] newTs = DirectMemoryAllocator.realloc(accTs, NEW_LIKE_OFFSET);


            int[] oldLikes = likesGb[key];

            int[] totalLikes = new int[oldLikes.length + newLikes.length];
            System.arraycopy(oldLikes,0, totalLikes, 0, oldLikes.length);

            int offs = oldLikes.length;

            for (int newI = 0; newI < newLikes.length; newI++) {
                int unmasked = newLikes[newI] & 0x00FFFFFF;

                int exist = 0;

                for (int p = 0; p < oldLikes.length; p++) {
                    int oldUnmasked = oldLikes[p] & 0x00FFFFFF;
                    if (oldUnmasked == unmasked) {
                        exist = oldLikes[p];
                        break;
                    }
                }

                if (exist > 0) {
                    int count = (int)((int)(exist & 0xFF000000) >>> 24);
                    count++;
                    totalLikes[offs] = (int)unmasked | (int)(count << 24);

                } else {
                    totalLikes[offs++] = newLikes[newI];
                }
            }

            Arrays.sort(totalLikes);

            int[] newTotalLikes = new int[offs];

            int newOffs = 0;
            for (int nl = totalLikes.length - 1; nl >= totalLikes.length - offs; nl--) {
                newTotalLikes[newOffs++] = totalLikes[nl];
            }

            likesGb[key] = newTotalLikes;

        });
    }

}
