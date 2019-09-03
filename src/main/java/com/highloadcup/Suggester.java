package com.highloadcup;

import com.highloadcup.model.Account;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.LikesPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static com.highloadcup.ArrayDataAllocator.*;

public class Suggester {

    private static final Logger logger = LogManager.getLogger(Suggester.class);


    //similarity = f (me, account),

    //sum ( 1 / abs(my_like['likeTS'] - like['likeTS']) )

    //Если у одного аккаунта есть несколько лайков на одного и того же пользователя с разными датами,
    // то в формуле используется среднее арифметическое их дат.

    //public static int[] idUnions = new int[MAX_ACCOUNTS];
    //public static int[] likeTS = new int[MAX_ACCOUNTS];


    //public static int[][] likesGb = new int[MAX_ACCOUNTS][];
    //public static int[][] likesTSGb = new int[MAX_ACCOUNTS][];

    public static LikesForUser[] likesForUser = new LikesForUser[MAX_ACCOUNTS];

    public static class LikesForUser {
        public int[] ids = new int[10];
        public int[] ts = new int[10];
        public int number = 0;
    }

    //post ended; new likes - 1351318
    //even&odd

    static int[] POST_LIKES = new int[1360000 * 3];

    ///static int[] likerA = new int[1360000];
    //static int[] likeToA = new int[1360000];
    //static int[] likeTs = new int[1360000];

    public static AtomicInteger NEW_LIKE_OFFSET = new AtomicInteger(0);


    public static ConcurrentMap<Integer, Integer> LIKER_DUPLICATES = new ConcurrentHashMap<>();


    //39319 new likes

    public static ExecutorService likesUpdateExecutor = Executors.newFixedThreadPool(2);

    public static AtomicInteger reallocNumber = new AtomicInteger(0);

    static volatile boolean wasError = false;

    public static Map<Integer, Integer> processLikes(int parity) {
        long start = System.nanoTime();

        Map<Integer, Integer> oldLikers = new TreeMap<>();

        for (int i = 0; i < NEW_LIKE_OFFSET.get(); i++) {

            try {
                int liker = POST_LIKES[i*3];
                int likeTo = POST_LIKES[i*3 + 1];
                int likeTs = POST_LIKES[i* 3 + 2];

                if (likeTo%2 != parity) {
                    continue;
                }

                LikesForUser lfu = likesForUser[likeTo];

                int isOldLiker = -1;

                if (lfu != null) {
                    if (!wasError) {
                        if (lfu.number > lfu.ids.length) {
                            logger.error("lfu number > length!!!!!!!!!!!; number {}, length {}", lfu.number, lfu.ids.length);
                        }
                        wasError = true;
                    }
                    int limit = lfu.ids.length > lfu.number ? lfu.number : lfu.ids.length;
                    for (int j = 0; j < limit; j++) {
                        int id = lfu.ids[j];
                        if ((int) (id & 0x00FFFFFF) == liker) {
                            isOldLiker = j;
                            break;
                        }
                    }
                }

                if (lfu == null) {
                    lfu = new LikesForUser();
                    likesForUser[likeTo] = lfu;
                } else if (isOldLiker < 0 && lfu.number >= lfu.ids.length) {
                    reallocNumber.incrementAndGet();

                    int offset = LIKER_DUPLICATES.get(likeTo);

                    lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + offset);
                    lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + offset);
                }

                if (isOldLiker > 0) {

                    lfu.ts[isOldLiker] = likeTs;

                    //int count = (int)((int)(lfu.ids[isOldLiker] & 0xFF000000) >>> 24);
                    //int tsAvg = (int)(((long)lfu.ts[isOldLiker] * (long)count + (long)likeTs) / (long) (count + 1));
                    //lfu.ts[isOldLiker] = tsAvg;
                    //count++;
                    //lfu.ids[isOldLiker] = (int)(lfu.ids[isOldLiker] & 0x00FFFFFF) | (int)(count << 24);
                    //logger.error("rewrite ts");
                    continue;
                }

                int newPos = lfu.number;

                for (int t = 0; t < lfu.number; t++) {
                    int ts = lfu.ts[t];
                    if (likeTs < ts) {
                        newPos = t;
                        //logger.error("new pos - {}", newPos);
                        break;
                    }
                }

                if (newPos != lfu.number) {
                    System.arraycopy(lfu.ids, newPos, lfu.ids, newPos + 1, lfu.ids.length - newPos - 1);
                    lfu.ids[newPos] = liker;
                    System.arraycopy(lfu.ts, newPos, lfu.ts, newPos + 1, lfu.ts.length - newPos - 1);
                    lfu.ts[newPos] = likeTs; //TODO avg TS
                } else {
                    lfu.ids[newPos] = liker;
                    lfu.ts[newPos] = likeTs; //TODO avg TS
                }
                lfu.number++;

            } catch (RuntimeException e) {
                logger.error("updateLikes err", e);
            } finally {
            }
        }
        logger.error("updateLikes end - {}; old likers size - {}", System.nanoTime() - start, oldLikers.size());

        return oldLikers;
    }

    public static class LikesUpdaterCallable implements Callable<Map<Integer, Integer>> {

        private int parity;//

        public LikesUpdaterCallable(int parity) {
            this.parity = parity;
        }

        @Override
        public Map<Integer, Integer> call() throws Exception {
            return processLikes(parity);
        }
    }

    public static void updateLikes() {
        long start = System.nanoTime();

        logger.error("updateLikes start");
        //Map<Integer, LikesForUser> likesFU = new HashMap<>();
        Map<Integer, Integer> oldLikers = new TreeMap<>();

        List<Future> futures = new ArrayList<>(4);

        try {
            futures.add(likesUpdateExecutor.submit(new LikesUpdaterCallable(0)));
            futures.add(likesUpdateExecutor.submit(new LikesUpdaterCallable(1)));

            for (Future future : futures) {
                Map<Integer, Integer> result = (Map)future.get();
                /*logger.error("f result size - {}", result.size());

                if (result != null) {
                    result.forEach( (key, value) -> {

                        //if (!oldLikers.containsKey(key) || oldLikers.get(key) < value) {
                            oldLikers.put(key, value);
                        //}
                    });
                }
                */
            }
        } catch (Exception e) {
            logger.error("updateLikes error", e);
            return;
        }

        logger.error("first part realloc - {}", reallocNumber.get());
        try {
        /*
            for (int i = 0; i < NEW_LIKE_OFFSET.get(); i++) {

                int liker = likerA[i];
                int likeTo = likeToA[i];
                int likeTs = likeTs[i];

                oldLikers.put(likeTo, i);


                LikesForUser lfu = likesForUser[likeTo];

                int isOldLiker = -1;

                if (lfu != null) {
                    if (lfu.number > lfu.ids.length) {
                        logger.error("lfu number > length!!!!!!!!!!!; number {}, length {}", lfu.number, lfu.ids.length);
                    }
                    int limit = lfu.ids.length > lfu.number ? lfu.number : lfu.ids.length;
                    for (int j = 0; j < limit; j++) {
                        int id = lfu.ids[j];
                        if ((int) (id & 0x00FFFFFF) == liker) {
                            isOldLiker = j;
                            break;
                        }
                    }
                }

                if (lfu == null) {
                    lfu = new LikesForUser();
                    likesForUser[likeTo] = lfu;
                } else if (isOldLiker < 0 && lfu.number >= lfu.ids.length) {
                    lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + 3);
                    lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + 3);
                }

                if (isOldLiker > 0) {

                    lfu.ts[isOldLiker] = likeTs;

                    //int count = (int)((int)(lfu.ids[isOldLiker] & 0xFF000000) >>> 24);
                    //int tsAvg = (int)(((long)lfu.ts[isOldLiker] * (long)count + (long)likeTs) / (long) (count + 1));
                    //lfu.ts[isOldLiker] = tsAvg;
                    //count++;
                    //lfu.ids[isOldLiker] = (int)(lfu.ids[isOldLiker] & 0x00FFFFFF) | (int)(count << 24);
                    //logger.error("rewrite ts");
                    continue;
                }

                int newPos = lfu.number;

                for (int t = 0; t < lfu.number; t++) {
                    int ts = lfu.ts[t];
                    if (likeTs < ts) {
                        newPos = t;
                        //logger.error("new pos - {}", newPos);
                        break;
                    }
                }

                if (newPos != lfu.number) {
                    System.arraycopy(lfu.ids, newPos, lfu.ids, newPos + 1, lfu.ids.length - newPos - 1);
                    lfu.ids[newPos] = liker;
                    System.arraycopy(lfu.ts, newPos, lfu.ts, newPos + 1, lfu.ts.length - newPos - 1);
                    lfu.ts[newPos] = likeTs; //TODO avg TS
                } else {
                    lfu.ids[newPos] = liker;
                    lfu.ts[newPos] = likeTs; //TODO avg TS
                }
                lfu.number++;

            }
*/

            reallocNumber.set(0);
            //logger.error("uniq likeTo count - {}", oldLikers.size());

            LIKER_DUPLICATES.keySet().forEach(likeTo -> {
                //LikesForUser oldLfu = entry.getValue();
                LikesForUser lfu = likesForUser[likeTo];
                if (lfu.ids.length > lfu.number) {
                    likesUpdateExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            reallocNumber.incrementAndGet();
                            lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.number);
                            lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.number);
                        }
                    });
                }

            });

            logger.error("second part realloc - {}", reallocNumber.get());

            //LikesForUser lf = likesForUser[24671];

            //for (int l: lf.ids) {
            //    logger.error("like: {}", l);
            //}

        } catch (RuntimeException e) {
            logger.error("updateLikes err", e);
        }

        logger.error("updateLikes end - {}", System.nanoTime() - start);

    }

    //150 mb post full
    public static boolean fillLikeForUser(int liker, int likeToAcc, int ts, int reallocBatch) {
        LikesForUser lfu = likesForUser[likeToAcc];

        int isOldLiker = -1;

        if (lfu != null) {
            for (int i = 0; i < lfu.ids.length; i++) {
                int id = lfu.ids[i];
                if ((int) (id & 0x00FFFFFF) == liker) {
                    isOldLiker = i;
                    break;
                }
            }
        }

        if (isOldLiker == -1) {
            LIKER_DUPLICATES.merge(likeToAcc, 1, Integer::sum);

            int offs = NEW_LIKE_OFFSET.getAndIncrement();
            //POST_LIKES
            if (offs < POST_LIKES.length * 3) {
                POST_LIKES[offs*3] = liker;
                POST_LIKES[offs*3 + 1] = likeToAcc;
                POST_LIKES[offs*3 + 2] = ts;
            }
        }

        /*
        if (lfu == null) {
            lfu = new LikesForUser();
            likesForUser[likeToAcc] = lfu;
        } else if (isOldLiker < 0 && lfu.number == lfu.ts.length) {
            lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + reallocBatch);
            lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + reallocBatch);
        }

        if (isOldLiker < 0) {
            lfu.ids[lfu.number] = liker;
            lfu.ts[lfu.number] = tsAvg;
            lfu.number++;
        } else {*/
        if (isOldLiker > 0) {
            //lfu.ts[isOldLiker] = ts;

            int oldId = lfu.ids[isOldLiker];

            int count = (int)((int)(oldId & 0xFF000000) >>> 24);
            if (count == 0) {
                count++;
            }
            int tsAvg = (int)(((long)lfu.ts[isOldLiker] * (long)count + (long)ts) / (long) (count + 1));

            int oldTs = lfu.ts[isOldLiker];
            int newTsPos = isOldLiker;

            for (int t = 0; t < lfu.ts.length; t++) {
                int _ts = lfu.ts[t];
                if (oldTs < _ts) {
                    newTsPos = t;
                    break;
                }
            }

            count++;

            if (newTsPos == isOldLiker) {
                lfu.ts[isOldLiker] = tsAvg;
                lfu.ids[isOldLiker] = (int) (oldId & 0x00FFFFFF) | (int) (count << 24);
            } else if (newTsPos < isOldLiker){
                int copyCount = isOldLiker - newTsPos;
                System.arraycopy(lfu.ts, newTsPos, lfu.ts, newTsPos + 1, copyCount);
                System.arraycopy(lfu.ids, newTsPos, lfu.ids, newTsPos + 1, copyCount);
                lfu.ts[newTsPos] = tsAvg;
                lfu.ids[newTsPos] = (int) (oldId & 0x00FFFFFF) | (int) (count << 24);
            } else {
                int copyCount = newTsPos - isOldLiker;
                System.arraycopy(lfu.ts, isOldLiker + 1, lfu.ts, isOldLiker, copyCount);
                System.arraycopy(lfu.ids, isOldLiker + 1, lfu.ids, isOldLiker, copyCount);
                lfu.ts[newTsPos] = tsAvg;
                lfu.ids[newTsPos] = (int) (oldId & 0x00FFFFFF) | (int) (count << 24);
            }
        }
            //TODO
        //}


        /*
        //resort
        if (isOldLiker < 0) {
            int newPos = lfu.ts.length - 1;

            for (int i = 0; i < lfu.ts.length - 1; i++) {
                int ts = lfu.ts[i];
                if (tsAvg < ts) {
                    newPos = i;
                    //logger.error("new pos - {}", newPos);
                    break;
                }
            }

            if (newPos != lfu.ts.length - 1) {
                System.arraycopy(lfu.ids, newPos, lfu.ids, newPos + 1, lfu.ids.length - newPos - 1);
                lfu.ids[newPos] = liker;
                System.arraycopy(lfu.ts, newPos, lfu.ts, newPos + 1, lfu.ts.length - newPos - 1);
                lfu.ts[newPos] = tsAvg; //TODO avg TS
            }

        }

        */

        return isOldLiker > 0;
    }

    public static void fillLikesForUser(int liker, int[] idLikes, int[] ts, int reallocBatch) {
        for (int lu = 0; lu < idLikes.length; lu++) {
            int likeToAcc = idLikes[lu] & 0x00FFFFFF;
            int tsAvg = ts[lu];

            LikesForUser lfu = likesForUser[likeToAcc];

            if (lfu == null) {
                lfu = new LikesForUser();
                likesForUser[likeToAcc] = lfu;
            } else if (lfu.number == lfu.ts.length) {
                lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + reallocBatch);
                lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + reallocBatch);
            }
            lfu.ids[lfu.number] = liker;
            lfu.ts[lfu.number] = tsAvg;
            lfu.number++;
        }
    }

    public static void resortLikesForUser(LikesForUser lfu) {
        lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.number);
        lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.number);

        ArrayDataAllocator.LikeTS[] pairs = new ArrayDataAllocator.LikeTS[lfu.ids.length];

        for (int j = 0; j < lfu.ids.length; j++) {
            pairs[j] = new ArrayDataAllocator.LikeTS(lfu.ids[j], lfu.ts[j]);
        }

        Arrays.sort(pairs, new Comparator<ArrayDataAllocator.LikeTS>() {
            @Override
            public int compare(ArrayDataAllocator.LikeTS o1, ArrayDataAllocator.LikeTS o2) {
                return Integer.compare(o1.ts, o2.ts);
            }
        });

        for (int k = 0; k < pairs.length; k++) {
            lfu.ids[k] = pairs[k].accId;
            lfu.ts[k] = pairs[k].ts;
        }
    }

    public static void resortUpdatedLikesForUser(LikesForUser lfu) {
        ArrayDataAllocator.LikeTS[] pairs = new ArrayDataAllocator.LikeTS[lfu.ids.length];

        for (int j = 0; j < lfu.ids.length; j++) {
            pairs[j] = new ArrayDataAllocator.LikeTS(lfu.ids[j], lfu.ts[j]);
        }

        Arrays.sort(pairs, new Comparator<ArrayDataAllocator.LikeTS>() {
            @Override
            public int compare(ArrayDataAllocator.LikeTS o1, ArrayDataAllocator.LikeTS o2) {
                return Integer.compare(o1.ts, o2.ts);
            }
        });

        for (int k = 0; k < pairs.length; k++) {
            lfu.ids[k] = pairs[k].accId;
            lfu.ts[k] = pairs[k].ts;
        }
    }

    public static void initTS() {

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            int[] idLikes = ArrayDataAllocator.likesGb[i];
            if (idLikes == null) {
                continue;
            }
            int[] ts = ArrayDataAllocator.likesTSGb[i];
            fillLikesForUser(i, idLikes, ts, 5);
            ArrayDataAllocator.likesTSGb[i] = null;
        }

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            LikesForUser lfu = likesForUser[i];

            if (lfu == null) {
                continue;
            }
            resortLikesForUser(lfu);
        }

        //clear

        //init phase only
        ArrayDataAllocator.likesTSGb = null;

        /*
        int[] lfu = likesGb[4734];

        //22242
        for (int aId: lfu) {
            int unmasked = aId & 0x00FFFFFF;
            //if (city == 129) {
                LikesForUser l = likesForUser[unmasked];
                if (l == null) {
                    continue;
                }
                for (int nLiker: l.ids) {

                    int unm2 = nLiker & 0x00FFFFFF;

                    short cntr1 = countryGlobal[unm2];

                    int[] lfu2 = likesGb[unm2];
                    for (int finL: lfu2) {
                        int unm3 = finL & 0x00FFFFFF;
                        short cntr = countryGlobal[unm3];
                        if (cntr1 == 410) {
                            logger.error("{} from cntr {} likes - id {} from cntr - {}", unm2, cntr1, unm3, cntr);
                        }
                    }
                }
            //}

        }

        System.exit(3);

*/

    }

    public static final ArrayBlockingQueue<Runnable> likeUpdates = new ArrayBlockingQueue<Runnable>(70000);

    public static final ExecutorService executorService = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            likeUpdates,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r);
                }
            });

    public static boolean processNewAccountLikesUpdates(int liker, List<Account.Like> likes) {

        synchronized (Suggester.class) {
            //executorService.execute(new Runnable() {
            //   @Override
            // public void run() {
            int uniqCount = 0;

            try {
                for (Account.Like like : likes) {

                    //if (countryGlobal[like.liker] == 410 && like.likee == 27107) {
                    //    logger.error("like for 27107 - {}", like.liker);
                    //}


                    if (ArrayDataAllocator.likesGb[liker] == null) {
                        uniqCount++;
                        int[] likee = new int[likes.size()];
                        likee[0] = like.id;
                        ArrayDataAllocator.likesGb[liker] = likee;
                    } else {
                        boolean isOldLike = false;

                        for (int likeFor : ArrayDataAllocator.likesGb[liker]) {
                            if (likeFor == like.id) {
                                isOldLike = true;
                                //logger.error("old like");
                                break;
                            }
                        }

                        if (!isOldLike) {
                            likesGb[liker][uniqCount++] = like.id;
                            //logger.error("new like - {}", like.likee);
                        }
                    }

                    boolean needResort = fillLikeForUser(liker, like.id, like.ts, 1);
                    //if (needResort) {
                    //resortUpdatedLikesForUser(likesForUser[like.likee]);
                    //}
                }

                if (uniqCount != likes.size() && likes.size() > 0) {
                    //logger.error("realloc - {} to {}", likesGb[liker].length, uniqCount);
                    likesGb[liker] = DirectMemoryAllocator.realloc(likesGb[liker], uniqCount);
                }
            } finally {
                for (Account.Like l : likes) {
                    LikesPool.returnLike(l);
                }
            }
            //     }
            //    });
        }
        return true;
    }

    public static boolean processAddNewLikes(int[] likesArr) {
        synchronized (Suggester.class) {

            //   executorService.execute(new Runnable() {
            //     @Override
            //   public void run() {
            int uniqCount = 0;

            for (int i = 0; i < likesArr.length - 3; i += 3) {
                int likee = likesArr[i];
                int ts = likesArr[i + 1];
                int liker = likesArr[i + 2];

                if (likee == 0 || liker == 0) {
                    break;
                }

                if (ArrayDataAllocator.likesGb[liker] == null) {
                    int[] likeeArr = new int[1];
                    likeeArr[0] = likee;
                    ArrayDataAllocator.likesGb[liker] = likeeArr;
                } else {
                    boolean isOldLike = false;

                    for (int likeFor : ArrayDataAllocator.likesGb[liker]) {
                        if ((int) (likeFor & 0x00FFFFFF) == likee) {
                            isOldLike = true;
                            //logger.error("old like");
                            break;
                        }
                    }

                    if (!isOldLike) {
                        int[] newArr = new int[likesGb[liker].length + 1];
                        System.arraycopy(likesGb[liker], 0, newArr, 0, likesGb[liker].length);
                        newArr[newArr.length - 1] = likee;
                        likesGb[liker] = newArr;
                    }
                }

                boolean needResort = fillLikeForUser(liker, likee, ts, 1);
            }

            //if (isNewAcc && uniqCount != likes.size() && likes.size() > 0) {
            //    likesGb[likes.get(0).liker] = DirectMemoryAllocator.realloc(likesGb[likes.get(0).liker], uniqCount);
            //}

            //      }
            // });
        }

        return true;
    }


    public static boolean processNewSuggester(InternalAccount account) {

        return true; //TODO
        /*
        int[] idLikes = ArrayDataAllocator.likesGb[account.id];
        if (idLikes != null) {
            int[] ts = ArrayDataAllocator.likesTSGb[account.id];

            for (int lu = 0; lu < idLikes.length; lu++) {
                int likeToAcc = idLikes[lu] & 0x00FFFFFF;
                int tsAvg = ts[lu];

                LikesForUser lfu = likesForUser[likeToAcc];

                if (lfu == null) {
                    lfu = new LikesForUser();
                    likesForUser[likeToAcc] = lfu;
                } else if (lfu.number == lfu.ts.length) {
                    lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.ids.length + 5);
                    lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.ts.length + 5);
                }
                lfu.ids[lfu.number] = account.id;
                lfu.ts[lfu.number] = tsAvg;
                lfu.number++;
            }
        }


        LikesForUser lfu = likesForUser[account.id];

        if (lfu != null) {
            lfu.ts = DirectMemoryAllocator.realloc(lfu.ts, lfu.number);
            lfu.ids = DirectMemoryAllocator.realloc(lfu.ids, lfu.number);

            ArrayDataAllocator.LikeTS[] pairs = new ArrayDataAllocator.LikeTS[lfu.ids.length];

            for (int j = 0; j < lfu.ids.length; j++) {
                pairs[j] = new ArrayDataAllocator.LikeTS(lfu.ids[j], lfu.ts[j]);
            }

            Arrays.sort(pairs, new Comparator<ArrayDataAllocator.LikeTS>() {
                @Override
                public int compare(ArrayDataAllocator.LikeTS o1, ArrayDataAllocator.LikeTS o2) {
                    return Integer.compare(o1.ts, o2.ts);
                }
            });

            for (int k = 0; k < pairs.length; k++) {
                lfu.ids[k] = pairs[k].accId;
                lfu.ts[k] = pairs[k].ts;
            }
        }

        return true;
        */

    }

    public static int findNearest(int[] ts, int myTs) {
        if (ts[0] >= myTs) {
            return 0;
        } else if (ts[ts.length - 1] <= myTs) {
            return ts.length - 1;
        }
        return findPositionBinarySearch(ts, 0, ts.length - 1, myTs);
    }

    public static int findPositionBinarySearch(int arr[], int l, int r, int ts)
    {
        if ((arr.length == r - l + 1)) {
            if (ts < arr[l]) {
                return l - 1;
            } else if(ts > arr[r]) {
                return r + 1;
            }
        }
        if (r > l)
        {
            int mid = l + (r - l)/2;
            if (ts == arr[mid])
                return mid;
            if (ts < arr[mid]) {
                return findPositionBinarySearch(arr, l, mid - 1, ts);
            } else {
                return findPositionBinarySearch(arr, mid + 1, r, ts);
            }
        } else if (r <= l) {
            return r;
        }
        return -1;
    }



    public static List<Integer> getSuggests(int account, int limit, int countryIdx, int cityIdx) {

        long start = System.nanoTime();

        int[] exist = ArrayDataAllocator.likesGb[account];

        if (exist == null) {
            return null;
        }
        int[] likeAcc = new int[exist.length];
        System.arraycopy(ArrayDataAllocator.likesGb[account], 0, likeAcc, 0, likeAcc.length);
        for (int i = 0; i < likeAcc.length; i++) {
            int likeAccId = likeAcc[i] & 0x00FFFFFF;
            likeAcc[i] = likeAccId;
        }

        long[] myLikeSet = DirectMemoryAllocator.alloc(ArrayDataAllocator.MAX_ACCOUNTS);
        DirectMemoryAllocator.fillSet(myLikeSet, likeAcc);

            //Set<Integer> mySet = new HashSet<Integer>();
            Set<Integer> finalSet = new HashSet<Integer>();

            int[] tmpLikers = new int[128];

            for (int i = 0; i < likeAcc.length; i++) {
                int likeAccount = likeAcc[i];// & 0x00FFFFFF;
                //mySet.add(likeAccount);

                DirectMemoryAllocator.clear(tmpLikers);

                LikesForUser lfuSelf = likesForUser[likeAccount];

                int myLikeTS = 0;

                for (int j = 0; j < lfuSelf.ids.length; j++) {
                    int idAcc = lfuSelf.ids[j] & 0x00FFFFFF;

                    if (idAcc == account) {
                        myLikeTS = lfuSelf.ts[j];
                    }

                }


                //int myLikeTS = ArrayDataAllocator.likesTSGb[account][i];

                LikesForUser lfu = likesForUser[likeAccount];

                if (cityIdx == Short.MAX_VALUE && countryIdx == Short.MAX_VALUE) {

                    int nearest = findNearest(lfu.ts, myLikeTS);

                    //country & city
                    finalSet.add(lfu.ids[nearest] & 0x00FFFFFF);

                    if (nearest == 0) {
                        if (lfu.ids.length > 1) {
                            finalSet.add(lfu.ids[nearest + 1] & 0x00FFFFFF);
                        }
                    } else if (nearest == lfu.ids.length - 1) {
                        finalSet.add(lfu.ids[nearest - 1] & 0x00FFFFFF);
                    } else {
                        finalSet.add(lfu.ids[nearest - 1] & 0x00FFFFFF);
                        finalSet.add(lfu.ids[nearest + 1] & 0x00FFFFFF);
                    }
                } else {

                    for (int unmasked: lfu.ids) {
                        if (cityIdx != Short.MAX_VALUE && cityGlobal[unmasked & 0x00FFFFFF] == cityIdx) {
                            finalSet.add(unmasked & 0x00FFFFFF);
                        } else if (countryIdx != Short.MAX_VALUE && countryGlobal[unmasked & 0x00FFFFFF] == countryIdx) {
                            finalSet.add(unmasked & 0x00FFFFFF);
                        }
                    }

                }

                /*int length = DirectMemoryAllocator.fillIntSetFromTriple(tr.data, tmpLikers);

                for (int l = 0; l < length; l++) {
                    if (ArrayDataAllocator.sexGlobal[tmpLikers[l]] == sex) {
                        finalSet.add(tmpLikers[l]);
                    }
                }*/

            }

        long end1 = System.nanoTime();
            //logger.error("diff1 - {}", end1 - start1);

            long start2 = System.nanoTime();

            TreeSet<Similar> sorted = new TreeSet<Similar>(new Comparator<Similar>() {
                @Override
                public int compare(Similar o1, Similar o2) {
                    int compare = Double.compare(o1.sum, o2.sum);
                    return compare;
                }
            });

            for (int suggester : finalSet) {

                if (suggester == account) {
                    continue;
                }
                int[] ids = ArrayDataAllocator.likesGb[suggester];

                //DirectMemoryAllocator.TripleIntArray tr = LikesFilter.likesByUser[suggester];

                if (ids == null || ids.length == 0) {
                    continue;
                }


                int length = ArrayDataAllocator.likesGb[suggester].length;
                System.arraycopy(ArrayDataAllocator.likesGb[suggester], 0, tmpLikers, 0, length);
                for (int l = 0; l < length; l++) {
                    int tmpLiker = tmpLikers[l] & 0x00FFFFFF;
                    tmpLikers[l] = tmpLiker;
                    if (!DirectMemoryAllocator.isBitSet(myLikeSet, tmpLiker)) {
                        tmpLikers[l] = 0;
                    }
                }

                //logger.error("set: {}", otherSet);

                double sum = 0.0;
                for (int j = 0; j < length; j++) {
                    int liker = tmpLikers[j];
                    if (liker == 0) {
                        continue;
                    }
                    int meTS = getTSFFor(account, liker);
                    int accTS = getTSFFor(suggester, liker);
                    long abs = Math.abs(meTS - accTS);
                    if (abs == 0) {
                        sum += (double)1.0;
                    } else {
                        sum += (double) 1 / (double) abs;
                    }
                }

                sorted.add(new Similar(suggester, sum));
                //double sim = (double)1.0 / (double) abs == 0 ? 1.0 : (double)abs;
            }

        long end2 = System.nanoTime();
        //logger.error("diff2 - {}", end2 - start2);


        Iterator<Similar> iterator = sorted.descendingIterator();
            //int limit = 20;
            int likers = 0;


            List<Integer> result = new ArrayList<>();

            while (iterator.hasNext()) {
                Similar s = iterator.next();

                int[] likes = likesGb[s.id];
                Integer[] recommends = new Integer[likes.length];
                //TODO
                for (int i = 0; i < likes.length; i++) {
                    int nl = likes[i] & 0x00FFFFFF;
                    recommends[i] = nl;
                }
                Arrays.sort(recommends, Collections.reverseOrder());

                likers += recommends.length;
                //String j = Arrays.stream(recommends).mapToObj(Integer::toString).collect(Collectors.joining(","));
                //logger.error("recomm - id {};  s.id: {}", s.id, j);

                Arrays.stream(recommends)
                        .filter( id -> {
                            if (DirectMemoryAllocator.isBitSet(myLikeSet, id)) {
                                return false;
                            } else  {
                                return true;
                            }
                        })
                        /*.filter( acc -> {

                            if (country != Short.MAX_VALUE) {
                                short accCountry = ArrayDataAllocator.countryGlobal[acc];
                                if (accCountry == country) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }

                            if (cityIdx != Short.MAX_VALUE) {
                                short accCity = ArrayDataAllocator.cityGlobal[acc];
                                if (accCity == cityIdx) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }

                            return true;
                        })*/
                        .forEach(result::add);
                if (likers > limit) {
                    break;
                }
            }

            return result;

        //}
        //long finish = System.nanoTime();

       // logger.error("count - {}; total time - {}; req per sec - {}; avg - {}",
         //       count, (finish - start), 1_000_000_000 / ((finish - start)/count), ((finish - start)/count));

        //count - 10000; total time - 254_880_904_270; req per sec - 39; avg - 25488090
        //[ERROR] 2019-02-13 14:50:24.383 [NIO Selector #0] Suggester - count - 1; total time - 1_506_4240; req per sec - 66; avg - 15064240

    }


    public static class Similar {
        public int id;
        public double sum;

        public Similar(int id, double sum) {
            this.id = id;
            this.sum = sum;
        }
    }


    public static int getTSFFor(int acc, int toAcc) {


        LikesForUser lfuSelf = likesForUser[toAcc];

        int myLikeTS = 0;

        for (int j = 0; j < lfuSelf.ids.length; j++) {
            int idAcc = lfuSelf.ids[j] & 0x00FFFFFF;

            if (idAcc == acc) {
                myLikeTS = lfuSelf.ts[j];
            }

        }
        return myLikeTS;


        /*
        int[] likesAcc = ArrayDataAllocator.likesGb[acc];
        for (int i = 0; i < likesAcc.length; i++) {

            int likeAccount = likesAcc[i] & 0x00FFFFFF;
            LikesForUser lfuSelf = likesForUser[likeAccount];

            int myLikeTS = 0;

            if (likeAccount == toAcc) {
                myLikeTS = lfuSelf.ts[i];
            }
            return myLikeTS;
        }
        return 1;
        */
    }

    /*
    public static void addLikeTS(int usrId, int ts) {
        DirectMemoryAllocator.addLikesTSAvg(usrId, idUnions, likeTS, ts);
    }

    public static double calculateSimilarity(int me, int acc) {
        long meTS = DirectMemoryAllocator.getLikesTSAvg(me, likeTS);
        long accTS = DirectMemoryAllocator.getLikesTSAvg(acc, likeTS);

        long abs = Math.abs(meTS - accTS);

        double sim = (double)1.0 / (double) abs == 0 ? 1.0 : (double)abs;
        return sim;
    }*/

}
