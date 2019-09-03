package com.highloadcup;

import com.highloadcup.filters.GroupBitmapsAggregator;
import com.highloadcup.model.Account;
import com.highloadcup.model.Likes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public class JsonParser {

    private static final Logger logger = LogManager.getLogger(JsonParser.class);

    static ThreadLocal<int[]> likesTL = new ThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[300];
        }
    };

    static final char[] LIKES_TO_CHAR_A = "likes".toCharArray();
    static final char[] TS_TO_CHAR_A = "ts\":".toCharArray();
    static final char[] ID_TO_CHAR_A = "id\":".toCharArray();
    static final char[] JOINED_TO_CHAR_A = "joined\":".toCharArray();
    static final char[] BIRTH_TO_CHAR_A = "birth\":".toCharArray();
    static final char[] SEX_TO_CHAR_A = "sex\":\"".toCharArray();
    static final char[] CITY_TO_CHAR_A = "city\":\"".toCharArray();
    static final char[] COUNTRY_TO_CHAR_A = "country\":\"".toCharArray();
    static final char[] FNAME_TO_CHAR_A = "fname\":\"".toCharArray();
    static final char[] SNAME_TO_CHAR_A = "sname\":\"".toCharArray();
    static final char[] EMAIL_TO_CHAR_A = "email\":\"".toCharArray();
    static final char[] STATUS_TO_CHAR_A = "status\":\"".toCharArray();
    static final char[] PHONE_TO_CHAR_A = "phone\":\"".toCharArray();
    static final char[] INTERESTS_TO_CHAR_A = "interests\":[".toCharArray();
    static final char[] PREMIUM_TO_CHAR_A = "premium".toCharArray();
    static final char[] START_TO_CHAR_A = "start".toCharArray();
    static final char[] FINISH_TO_CHAR_A = "finish".toCharArray();


    /*
    static final byte STRING_CODER_LATIN1 = 0;

    static final Method indexOfM;
    static final Method lastIndexOfM;
    static final Unsafe unsafe;

    static final long INT_TYPE_VAL_OFFSET;


    static {
        try {
            indexOfM = "".getClass().getDeclaredMethod("indexOf", byte[].class, byte.class, int.class, String.class, int.class);
            indexOfM.setAccessible(true);

            lastIndexOfM = "".getClass().getDeclaredMethod("lastIndexOf", byte[].class, byte.class, int.class, String.class, int.class);
            lastIndexOfM.setAccessible(true);

            logger.error("successfully get indexOf() method ref");

            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe)f.get(null);

            Integer i = 1;

            Field field = i.getClass().getDeclaredField("value");

            INT_TYPE_VAL_OFFSET= unsafe.objectFieldOffset( field );

        } catch (Exception e) {
            logger.error("can't get indexOf{} method of java.lang.String");
            throw new RuntimeException("reflection fail");
        }

    }*/

    public static int[] parseLikesToArray(char[] json) throws Exception {
        //List<Likes.Like> list = new ArrayList<>();

        //likee -0, ts -1, liker - 2
        int[] finalInt = likesTL.get();
        DirectMemoryAllocator.clear(finalInt);

        int finalOffs = 0;

        int startList = -1;
        int startElement = -1;
        int endElement = -1;
        int startKey = -1;
        int endKey = -1;
        int startValue = -1;
        int endValue = -1;

        //Likes.Like current = null;

        boolean keyIsTs = false;
        boolean keyIsLiker = false;
        for (int i = 10; i < json.length; i++) {

            if (json[i] == '{') {
                startElement = i + 1;
                //current = new Likes.Like();
            } else if (startElement > 0 && json[i] == '\"') {
                if (startKey == -1) {
                    startKey = i + 1;
                    if (json[startKey] != 't') {
                        i += 5;
                    } else {
                        i += 2;
                        keyIsTs = true;
                    }
                } else {
                    endKey = i;
                    if (json[endKey-1] == 'r') {
                        keyIsLiker = true;
                    }
                }
            } else if (endKey > 0 && json[i] == ':') {
                if (startValue == -1) {
                    startValue = i + 1;
                    //i += 2;
                    //if (keyIsTs) {
                    //    i += 8;
                    //}
                }
            } else if (startValue > 0 && (json[i] == ',' || json[i] == '}')) {
                endValue = i;
                int val = parsePositiveInt(json, startValue, endValue - 1);
                int offs = 0;
                if (keyIsTs) {
                    offs = 1;
                } else if (keyIsLiker) {
                    offs = 2;
                }
                finalInt[finalOffs + offs] = val;
                keyIsTs = false;
                keyIsLiker = false;
                //fillLikesValue(current, json, startKey, endKey, startValue, endValue);
                startKey = -1;
                endKey = -1;
                startValue = -1;
                endValue = -1;
                if (json[i] == '}') {
                    //list.add(current);
                    startElement = -1;
                    finalOffs +=3;
                    //current = null;
                }
            } else if (json[i] == ']') {
                break;
            } else if (startValue > 0 && (((short)(json[i]) - 48) < 0 || ((short)(json[i]) - 48) > 9)) {
                throw new RuntimeException("wrong likes");
            }

        }
        return finalInt;
    }


    public static List<Likes.Like> parseLikes(char[] json) throws Exception {
        List<Likes.Like> list = new ArrayList<>();

        int startList = -1;
        int startElement = -1;
        int endElement = -1;
        int startKey = -1;
        int endKey = -1;
        int startValue = -1;
        int endValue = -1;

        Likes.Like current = null;

        for (int i = 0; i < json.length; i++) {

            if (startList == -1 && json[i] != '[') {
                continue;
            } else if (json[i] == '[') {
                startList = i + 1;
            } else if (json[i] == '{') {
                startElement = i + 1;
                current = new Likes.Like();
            } else if (json[i] == ' ' || json[i] == '\n' || json[i] == '\t') {
                continue;
            } else if (startElement > 0 && json[i] == '\"') {
                if (startKey == -1) {
                    startKey = i + 1;
                } else {
                    endKey = i;
                }
            } else if (endKey > 0 && json[i] == ':') {
                if (startValue == -1) {
                    startValue = i + 1;
                }
            } else if (startValue > 0 && (json[i] == ',' || json[i] == '}')) {
                endValue = i;
                fillLikesValue(current, json, startKey, endKey, startValue, endValue);
                startKey = -1;
                endKey = -1;
                startValue = -1;
                endValue = -1;
                if (json[i] == '}') {
                    list.add(current);
                    startElement = -1;
                    current = null;
                }
            } else if (json[i] == ']') {
                break;
            }

        }
        return list;
    }

    public static void fillLikesValue(Likes.Like like, char[] json, int keyStart, int keyEnd, int valueStart, int valueEnd) throws Exception {
        String name = new String(json, keyStart, keyEnd - keyStart).trim();
        Field field = like.getClass().getDeclaredField(name);
        field.setAccessible(true);

        field.set(like, parsePositiveInt(json, valueStart, valueEnd - 1));

        //field.set(like, Integer.parseInt(new String(json, valueStart, valueEnd - valueStart).trim()));
    }

    public static int parsePositiveInt(char[] arr, int start, int end) {
        int res = 0;
        int power = 1;
        for (int i = end; i >= start; i--) {
            char c = arr[i];
            int val = (short)(arr[i]) - 48;
            if (val < 0 || val > 9) {
                throw new RuntimeException("wrong number");
            }
            res += val * power;
            power *= 10;
        }
        return res;
    }

    public static void parseInterests(Account acc, char[] json, int from, int to) {

        acc.interestsNumber = 0;
        int lastItrStart = from + 1;

        for (int i = from; i < to; i++) {
            if (json[i] == ',') {
                long hash = getStrHash(json, lastItrStart, i - 1);
                acc.interestsHashes[acc.interestsNumber++] = hash;
                boolean wasAdded = Dictionaries.fillUnescaped(hash, json, lastItrStart, i - 1,
                        Dictionaries.interests, Dictionaries.interestsById);
                if (wasAdded) {
                    Dictionaries.incInterestsSequence();
                }
                lastItrStart = i + 2;
            }
        }

        long hash = getStrHash(json, lastItrStart, to - 1);
        acc.interestsHashes[acc.interestsNumber++] = hash;
        boolean wasAdded = Dictionaries.fillUnescaped(hash, json, lastItrStart, to - 1,
                Dictionaries.interests, Dictionaries.interestsById);
        if (wasAdded) {
            Dictionaries.incInterestsSequence();
        }

    }

    public static long parsePhone(char[] arr, int start, int end) {
        long res = 0;
        long power = 1;
        for (int i = end - 1; i >= start; i--) {

            if (arr[i] == '(' || arr[i] == ')') {
                continue;
            }
            long val = (short)(arr[i]) - 48;
            if (val < 0 || val > 9) {
                throw new RuntimeException("wrong number");
            }
            res += val * power;
            power *= 10;
        }
        return res;
    }


    public static long getStrHash(char[] body, int from, int to) {
        long hash = 7;
        for (int i = from; i < to; i++) {
            hash = hash*31L + (long)body[i];
        }
        return hash;
    }

    static ThreadLocal<Account> accForUpdTL = new ThreadLocal<Account>() {
        @Override
        protected Account initialValue() {
            return new Account();
        }
    };

    public static Account parseAccountUpdate(char[] json, int length) throws Exception {

        boolean wasAdded = false;

        //String body = new String("");

        //Field valueF = body.getClass().getDeclaredField("value");
        //valueF.setAccessible(true);
        //valueF.set(body, json);

        //char[] json = (char[])field.get(body);

        Account acc = accForUpdTL.get();
        acc.clear();

        //byte[] bytesBody = unescaped.getBytes(Charset.forName("UTF-8"));

        //String body = new String(bytesBody, "UTF8");


        //final String body = StringEscapeUtils.unescapeJava(bodyUnescaped);
        //                JsonReader reader = new JsonReader(new InputStreamReader(is, "UTF-8"));

        int likesStartIdx = indexOf(json, length, LIKES_TO_CHAR_A);
        {
            int idStartIdx = indexOf(json, length, ID_TO_CHAR_A);
            if (idStartIdx > likesStartIdx) {
                idStartIdx = lastIndexOf(json, length, ID_TO_CHAR_A);
            }

            int idEndIdx = -1;

            if (idStartIdx > 0) {
                idEndIdx = indexOf(json, length, ',', idStartIdx + 4);
                if (idEndIdx < 0) {
                    idEndIdx = indexOf(json, length, '}', idStartIdx + 4);
                }
                //String id = body.substring(idStartIdx + 4, idEndIdx);
                //acc.id = Integer.parseInt(id);
                acc.id = parsePositiveInt(json, idStartIdx + 4, idEndIdx - 1);
            }
        }

        {
            int joinedStartIdx = indexOf(json, length, JOINED_TO_CHAR_A);
            int joinedEndIdx = -1;

            if (joinedStartIdx > 0) {
                joinedEndIdx = indexOf(json, length, ',',joinedStartIdx + 8);
                if (joinedEndIdx < 0) {
                    joinedEndIdx = indexOf(json, length, '}', joinedStartIdx + 8);
                }
                //String joined = body.substring(joinedStartIdx + 8, joinedEndIdx);
                //acc.joined = Integer.parseInt(joined);
                acc.joined = parsePositiveInt(json, joinedStartIdx + 8, joinedEndIdx - 1);
            }
        }

        {
            int birthStartIdx = indexOf(json, length, BIRTH_TO_CHAR_A);
            int birthEndIdx = -1;

            if (birthStartIdx > 0) {
                birthEndIdx = indexOf(json, length, ',', birthStartIdx + 7);
                if (birthEndIdx < 0) {
                    birthEndIdx = indexOf(json, length, '}', birthStartIdx + 7);
                }
                //String birth = body.substring(birthStartIdx + 7, birthEndIdx);
                //acc.birth = Integer.parseInt(birth);
                acc.birth = parsePositiveInt(json, birthStartIdx + 7, birthEndIdx - 1);
            }
        }

        {
            int sexStartIdx = indexOf(json, length, SEX_TO_CHAR_A);
            int sexEndIdx = -1;

            if (sexStartIdx > 0) {
                sexStartIdx += 6;
                sexEndIdx = indexOf(json, length, '\"', sexStartIdx);
                String sex = new String(json, sexStartIdx, sexEndIdx - sexStartIdx);
                acc.sex = sex;
            }
        }

        {
            int cityStartIdx = indexOf(json, length, CITY_TO_CHAR_A);
            int cityEndIdx = -1;

            if (cityStartIdx > 0) {
                cityStartIdx += 7;
                cityEndIdx = indexOf(json, length, '\"', cityStartIdx);
                //String city = body.substring(cityStartIdx + 7, cityEndIdx);
                //acc.city = city;
                acc.cityHash = getStrHash(json, cityStartIdx, cityEndIdx);
                boolean result = Dictionaries.fillUnescapedSeq(acc.cityHash, json, cityStartIdx, cityEndIdx,
                        Dictionaries.cities, Dictionaries.citiesById, Dictionaries.citySequence);
                wasAdded = wasAdded == false ? result : true;
            }
        }

        {
            int coutryStartIdx = indexOf(json, length, COUNTRY_TO_CHAR_A);
            int countryEndIdx = -1;
            if (coutryStartIdx > 0) {
                coutryStartIdx += 10;
                countryEndIdx = indexOf(json, length, '\"', coutryStartIdx);
                //String country = body.substring(coutryStartIdx + 10, countryEndIdx);
                //acc.country = country;
                acc.countryHash = getStrHash(json, coutryStartIdx, countryEndIdx);
                boolean result = Dictionaries.fillUnescapedSeq(acc.countryHash, json, coutryStartIdx, countryEndIdx,
                        Dictionaries.countries, Dictionaries.countriesById, Dictionaries.countrySequence);

                if (wasAdded) {
                    //TODO
                    GroupBitmapsAggregator.newCountry((short)(Dictionaries.countrySequence.get() - 1));
                }
                wasAdded = wasAdded == false ? result : true;
            }
        }

        {
            int fNameStartIdx = indexOf(json, length, FNAME_TO_CHAR_A);
            int fNameEndIdx = -1;
            if (fNameStartIdx > 0) {
                fNameStartIdx += 8;
                fNameEndIdx = indexOf(json, length, '\"', fNameStartIdx);
                //String fname = body.substring(fNameStartIdx + 8, fNameEndIdx);
                //acc.fname = fname;
                acc.fnameHash = getStrHash(json, fNameStartIdx, fNameEndIdx);
                boolean result = Dictionaries.fillUnescaped(acc.fnameHash, json, fNameStartIdx, fNameEndIdx,
                        Dictionaries.fnames, Dictionaries.fnamesById);
                wasAdded = wasAdded == false ? result : true;
            }
        }

        {
            int sNameStartIdx = indexOf(json, length, SNAME_TO_CHAR_A);
            int sNameEndIdx = -1;
            if (sNameStartIdx > 0) {
                sNameStartIdx += 8;
                sNameEndIdx = indexOf(json, length, '\"', sNameStartIdx);
                //String sname = body.substring(sNameStartIdx + 8, sNameEndIdx);
                //acc.sname = sname;
                acc.snameHash = getStrHash(json, sNameStartIdx, sNameEndIdx);
                boolean result = Dictionaries.fillUnescaped(acc.snameHash, json, sNameStartIdx, sNameEndIdx,
                        Dictionaries.snames, Dictionaries.snamesById);
                wasAdded = wasAdded == false ? result : true;
            }
        }

        {
            int emailStartIdx = indexOf(json, length, EMAIL_TO_CHAR_A);
            int emailEndIdx = -1;
            if (emailStartIdx > 0) {
                emailStartIdx += 8;
                emailEndIdx = indexOf(json, length, '\"', emailStartIdx);
                //TODO hash
                String email = new String(json, emailStartIdx, emailEndIdx - emailStartIdx);
                acc.email = email;
            }
        }

        {
            int statusStartIdx = indexOf(json, length, STATUS_TO_CHAR_A);
            int statusEndIdx = -1;
            if (statusStartIdx > 0) {
                statusStartIdx += 9;
                statusEndIdx = indexOf(json, length, '\"', statusStartIdx);
                //String status = body.substring(statusStartIdx + 9, statusEndIdx);
                //String stU = StringEscapeUtils.unescapeJava(status);
                //acc.status = status;
                acc.statusHash = getStrHash(json, statusStartIdx, statusEndIdx);
            }
        }

        {
            int phoneStartIdx = indexOf(json, length, PHONE_TO_CHAR_A);
            int phoneEndIdx = -1;
            if (phoneStartIdx > 0) {
                phoneStartIdx += 8;
                phoneEndIdx = indexOf(json, length, '\"', phoneStartIdx);
                //String phone = body.substring(phoneStartIdx + 8, phoneEndIdx);
                //acc.phone = phone;
                acc.phoneNum = parsePhone(json, phoneStartIdx, phoneEndIdx);
            }
        }

        {
            int interestsStartIdx = indexOf(json, length, INTERESTS_TO_CHAR_A);
            int interestsEndIdx = -1;
            if (interestsStartIdx > 0) {
                interestsStartIdx += 12;
                interestsEndIdx = indexOf(json, length, ']', interestsStartIdx);

                parseInterests(acc, json, interestsStartIdx, interestsEndIdx);

                //String interestsString = body.substring(interestsStartIdx + 12, interestsEndIdx);
                //acc.interests = new ArrayList<>();
                //String[] interestsArr = interestsString.split(",");

                //for (String itr: interestsArr) {
                //    acc.interests.add(itr.substring(1, itr.length() - 1));
                //}
            }
        }

        {
            if (indexOf(json, length, PREMIUM_TO_CHAR_A) > 0) {
                if (indexOf(json, length, START_TO_CHAR_A) < 0 || indexOf(json, length, FINISH_TO_CHAR_A) < 0) {
                    throw new RuntimeException("bad premium");
                }
                int premiumStartStartIdx = indexOf(json, length, START_TO_CHAR_A);
                int premiumFinishStartIdx = indexOf(json, length, FINISH_TO_CHAR_A);

                int premiumStartEndIdx = -1;
                if (premiumStartStartIdx > 0) {
                    if (premiumStartStartIdx < premiumFinishStartIdx) {
                        premiumStartEndIdx = indexOf(json, length, ',', premiumStartStartIdx + 7);
                        //String premiumStart = body.substring(premiumStartStartIdx + 7, premiumStartEndIdx);
                        //acc.premiumStart = Integer.parseInt(premiumStart);
                        acc.premiumStart = parsePositiveInt(json, premiumStartStartIdx + 7, premiumStartEndIdx - 1);

                        int premiumFinishEndIdx = indexOf(json, length, '}', premiumFinishStartIdx + 8);
                        //String premiumFinish = body.substring(premiumFinishStartIdx + 8, premiumFinishEndIdx);
                        //acc.premiumFinish = Integer.parseInt(premiumFinish);
                        acc.premiumFinish = parsePositiveInt(json, premiumFinishStartIdx + 8, premiumFinishEndIdx - 1);
                    } else {
                        premiumStartEndIdx = indexOf(json, length, '}', premiumStartStartIdx + 7);
                        //String premiumStart = body.substring(premiumStartStartIdx + 7, premiumStartEndIdx);
                        //acc.premiumStart = Integer.parseInt(premiumStart);
                        acc.premiumStart = parsePositiveInt(json, premiumStartStartIdx + 7, premiumStartEndIdx - 1);

                        int premiumFinishEndIdx = indexOf(json, length, ',', premiumFinishStartIdx + 8);
                        //String premiumFinish = body.substring(premiumFinishStartIdx + 8, premiumFinishEndIdx);
                        //acc.premiumFinish = Integer.parseInt(premiumFinish);
                        acc.premiumFinish = parsePositiveInt(json, premiumFinishStartIdx + 8, premiumFinishEndIdx - 1);
                    }
                }
            }
        }
        {
            if (likesStartIdx > 0) {
                acc.likes = parseLikes(json, length);
            }
        }

        if (wasAdded) {
            Dictionaries.incBaseSequence();
        }
        return acc;
    }

    public static int indexOf(char[] cs, int length, char toFind, int start) {
        for (int i = start; i < length; i++) {
            char c = cs[i];
            if (c == toFind) {
                return i;
            }
        }
        return -1;
    }

    public static int indexOf(char[] json, int length, char[] toFind, int from) {

        int findLength = toFind.length;
        int findLastPos = findLength - 1;

        if (json.length - from < findLength) {
            return -1;
        }

        for (int i = from; i < length; i++) {
            if (toFind[0] == json[i]) {
                if (toFind[findLastPos] == json[i + findLastPos]) {
                    boolean eq = compareInDepth(json, toFind, i, 0, findLength - 1);
                    if (eq) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    public static int indexOf(char[] json, int length, char[] toFind) {

        int findLength = toFind.length;
        int findLastPos = findLength - 1;

        if (json.length < findLength) {
            return -1;
        }

        for (int i = 0; i < length; i++) {
            if (toFind[0] == json[i]) {
                if (toFind[findLastPos] == json[i + findLastPos]) {
                    boolean eq = compareInDepth(json, toFind, i, 0, findLength - 1);
                    if (eq) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    public static int lastIndexOf(char[] json, int length, char[] toFind) {

        int findLength = toFind.length;
        int findLastPos = findLength - 1;

        if (json.length < findLength) {
            return -1;
        }

        for (int i = length - findLength; i >= 0; i--) {
            if (toFind[0] == json[i]) {
                if (toFind[findLastPos] == json[i + findLastPos]) {
                    boolean eq = compareInDepth(json, toFind, i, 0, findLength - 1);
                    if (eq) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    static boolean compareInDepth(char[] json, char[] toFind, int jsonOffset, int position, int limit) {
        if (position == limit) {
            return json[jsonOffset + position] == toFind[position];
        } {
            boolean eq = compareInDepth(json, toFind, jsonOffset, position + 1, limit);
            if (!eq) {
                return eq;
            }
            return json[jsonOffset + position] == toFind[position];
        }

    }

    /*
    public static int indexOf(byte[] json, int length, String src) throws Exception {
        return (int)indexOfM.invoke(null, json, STRING_CODER_LATIN1, length, src, 0);
    }

    public static int indexOf(byte[] json, int length, String src, int start) throws Exception {
        return (int)indexOfM.invoke(null, json, STRING_CODER_LATIN1, length, src, start);
    }

    public static int lastIndexOf(byte[] json, int length, String src) throws Exception {
        return (int)lastIndexOfM.invoke(null, json, STRING_CODER_LATIN1, length, src, length);
    }
    */

    public static List<Account.Like> parseLikes(char[] json, int length) throws Exception {

        //Field field = body.getClass().getDeclaredField("value");
        //field.setAccessible(true);

        List<Account.Like> likes = new ArrayList<>();


        int likesStartIdx = indexOf(json, length, LIKES_TO_CHAR_A) + 5;

        int indexOfLikesEnd = indexOf(json, length, ']', likesStartIdx);

        if (likesStartIdx < 0 || indexOfLikesEnd < 0) {
            return null;
        }

        while(true) {
            Account.Like like = new Account.Like();
            int tsStartIdx = indexOf(json, length, TS_TO_CHAR_A, likesStartIdx);
            int idStartIdx = indexOf(json, length, ID_TO_CHAR_A, likesStartIdx);

            if (tsStartIdx > indexOfLikesEnd) {
                break;
            }

            int tsEndIdx = -1;
            if (tsStartIdx > 0) {
                if (tsStartIdx < idStartIdx) {
                    tsEndIdx = indexOf(json, length, ',', tsStartIdx + 4);
                    //String ts = body.substring(tsStartIdx + 4, tsEndIdx);
                    //like.ts = Integer.parseInt(ts);
                    like.ts = parsePositiveInt(json, tsStartIdx+4, tsEndIdx - 1);


                    int idEndIdx = indexOf(json, length, '}', idStartIdx + 4);
                    //String id = body.substring(idStartIdx + 4, idEndIdx);
                    //like.id = Integer.parseInt(id);
                    like.id = parsePositiveInt(json, idStartIdx+4, idEndIdx-1);
                    likesStartIdx = idEndIdx;
                    likes.add(like);
                } else {
                    tsEndIdx = indexOf(json, length, '}', tsStartIdx + 4);
                    //String ts = body.substring(tsStartIdx + 4, tsEndIdx);
                    //like.ts = Integer.parseInt(ts);
                    like.ts = parsePositiveInt(json, tsStartIdx+4, tsEndIdx - 1);

                    int idEndIdx = indexOf(json, length, ',', idStartIdx + 4);
                    //String id = body.substring(idStartIdx + 4, idEndIdx);
                    //like.id = Integer.parseInt(id);
                    like.id = parsePositiveInt(json, idStartIdx+4, idEndIdx-1);
                    likesStartIdx = tsEndIdx;
                    likes.add(like);
                }
            } else {
                break;
            }
        }

        return likes;
    }

}
