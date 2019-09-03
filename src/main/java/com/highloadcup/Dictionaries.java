package com.highloadcup;

import com.highloadcup.model.Account;
import com.highloadcup.model.Accounts;
import org.apache.commons.lang3.StringEscapeUtils;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Dictionaries {

    public static final HashMap<String, Short> countries = new HashMap<>();
    public static final HashMap<Short, String> countriesById = new HashMap<>();

    public static final HashMap<String, Short> cities = new HashMap<>();
    public static final HashMap<Short, String> citiesById = new HashMap<>();

    public static final HashMap<String, Short> emailDomainsMap = new HashMap<>();
    public static final HashMap<Short, String> emailDomainsById = new HashMap<>();

    public static final HashMap<String, Short> fnames = new HashMap<>();
    public static final HashMap<Short, String> fnamesById = new HashMap<>();

    public static final HashMap<String, Short> snames = new HashMap<>();
    public static final HashMap<Short, String> snamesById = new HashMap<>();

    public static final HashMap<String, Short> interests = new HashMap<>();
    public static final HashMap<Short, String> interestsById = new HashMap<>();

    public static AtomicInteger shortSequence = new AtomicInteger(1);

    public static AtomicInteger citySequence = new AtomicInteger(1);
    public static AtomicInteger countrySequence = new AtomicInteger(1);

    public static AtomicInteger shortSequenceForInterests = new AtomicInteger(1);

    public static ConcurrentMap<String, String> escToUnescaped = new ConcurrentHashMap<>();

    public static ConcurrentMap<Long, String> escToUnescapedHashes = new ConcurrentHashMap<>();

    public static String getUnescaped(String escaped) {
        String unescaped = escToUnescaped.get(escaped);

        if (unescaped == null) {
            unescaped = StringEscapeUtils.unescapeJava(escaped);
            escToUnescaped.putIfAbsent(escaped, unescaped);
        }

        return unescaped;
    }

    public static synchronized boolean fillUnescapedSeq(long hash, char[] json, int from, int to,
                                                     Map<String, Short> ids, Map<Short, String> vals, AtomicInteger sequence) {

        boolean wasAdded = false;

        String unescaped = escToUnescapedHashes.get(hash);
        if (unescaped == null) {
            unescaped = StringEscapeUtils.unescapeJava(new String(json, from, to - from));
            escToUnescapedHashes.putIfAbsent(hash, unescaped);

            Short idx = ids.putIfAbsent(unescaped, (short)sequence.get());
            if (idx == null) {
                wasAdded = true;
                vals.put((short)sequence.get(), unescaped);
                sequence.incrementAndGet();
            }
        }

        return wasAdded;
    }

    public static synchronized boolean fillUnescaped(long hash, char[] json, int from, int to, Map<String, Short> ids, Map<Short, String> vals) {

        boolean wasAdded = false;

        String unescaped = escToUnescapedHashes.get(hash);
        if (unescaped == null) {
            unescaped = StringEscapeUtils.unescapeJava(new String(json, from, to - from));
            escToUnescapedHashes.putIfAbsent(hash, unescaped);

            Short idx = ids.putIfAbsent(unescaped, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                vals.put((short)shortSequence.get(), unescaped);
            }
        }

        return wasAdded;
    }


    public static void fillDictsForOneOnAddOrUpdateUnescaped(Account account) {
        boolean wasAdded = false;

        if (account.status != null && !account.status.isEmpty()) {
            account.status = getUnescaped(account.status);
        }

        if (account.fname != null && !account.fname.isEmpty()) {

            account.fname = getUnescaped(account.fname);

            Short idx = fnames.putIfAbsent(account.fname, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                fnamesById.put((short)shortSequence.get(), account.fname);
            }
        }
        if (account.sname != null && !account.sname.isEmpty()) {

            account.sname = getUnescaped(account.sname);

            Short idx = snames.putIfAbsent(account.sname, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                snamesById.put((short)shortSequence.get(), account.sname);
            }
        }

        if (account.country != null && !account.country.isEmpty()) {

            account.country = getUnescaped(account.country);

            Short idx = countries.putIfAbsent(account.country, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                countriesById.put((short)shortSequence.get(), account.country);
            }
        }

        if (account.city != null && !account.city.isEmpty()) {

            account.city = getUnescaped(account.city);

            Short idx = cities.putIfAbsent(account.city, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                citiesById.put((short)shortSequence.get(), account.city);
            }
        }

        if (account.email != null && !account.email.isEmpty()) {
            final String domain = account.email.toLowerCase().split("@")[1];
            Short idx = emailDomainsMap.putIfAbsent(domain, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                emailDomainsById.put((short)shortSequence.get(), domain);
            }
        }

        if (account.interests != null && !account.interests.isEmpty()) {

            List<String> unescItr = new ArrayList<>();


            account.interests.forEach(interest -> {

                String unescaped = getUnescaped(interest);
                unescItr.add(unescaped);

                if (!interests.containsKey(unescaped)) {
                    short nextId = (short)shortSequenceForInterests.getAndIncrement();
                    interests.put(unescaped, nextId);
                    interestsById.put(nextId, unescaped);
                }
            });

            account.interests = unescItr;
        }

        if (wasAdded) {
            shortSequence.incrementAndGet();
        }
    }

    public static void incBaseSequence() {
        shortSequence.incrementAndGet();
    }

    public static void incInterestsSequence() {
        shortSequenceForInterests.incrementAndGet();
    }

    public static void fillDictsForOneOnAddOrUpdateByHashes(Account account) {
        boolean wasAdded = false;

        if (account.status != null && !account.status.isEmpty()) {
            account.status = getUnescaped(account.status);
        }

        if (account.fname != null && !account.fname.isEmpty()) {

            account.fname = getUnescaped(account.fname);

            Short idx = fnames.putIfAbsent(account.fname, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                fnamesById.put((short)shortSequence.get(), account.fname.trim());
            }
        }
        if (account.sname != null && !account.sname.isEmpty()) {

            account.sname = getUnescaped(account.sname);

            Short idx = snames.putIfAbsent(account.sname.trim(), (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                snamesById.put((short)shortSequence.get(), account.sname.trim());
            }
        }

        if (account.country != null && !account.country.isEmpty()) {

            account.country = getUnescaped(account.country);

            Short idx = countries.putIfAbsent(account.country.trim(), (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                countriesById.put((short)shortSequence.get(), account.country.trim());
            }
        }

        if (account.city != null && !account.city.isEmpty()) {

            account.city = getUnescaped(account.city);

            Short idx = cities.putIfAbsent(account.city.trim(), (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                citiesById.put((short)shortSequence.get(), account.city.trim());
            }
        }

        if (account.email != null && !account.email.isEmpty()) {
            final String domain = account.email.toLowerCase().trim().split("@")[1];
            Short idx = emailDomainsMap.putIfAbsent(domain, (short)shortSequence.get());
            if (idx == null) {
                wasAdded = true;
                emailDomainsById.put((short)shortSequence.get(), domain);
            }
        }

        if (account.interests != null && !account.interests.isEmpty()) {

            List<String> unescItr = new ArrayList<>();


            account.interests.forEach(interest -> {

                String unescaped = getUnescaped(interest);
                unescItr.add(unescaped);

                if (!interests.containsKey(unescaped)) {
                    short nextId = (short)shortSequenceForInterests.getAndIncrement();
                    interests.put(unescaped, nextId);
                    interestsById.put(nextId, unescaped);
                }
            });

            account.interests = unescItr;
        }

        if (wasAdded) {
            shortSequence.incrementAndGet();
        }
    }

    public static void fillDictsForOneOnInit(Account account) {
        boolean wasAdded = false;

        if (account.fname != null && !account.fname.isEmpty()) {
            if(fnames.putIfAbsent(account.fname.trim(), (short)shortSequence.get()) == null) {
                wasAdded = true;
            }
        }
        if (account.sname != null && !account.sname.isEmpty()) {
            if (snames.putIfAbsent(account.sname.trim(), (short)shortSequence.get()) == null) {
                wasAdded = true;
            }
        }

        if (account.country != null && !account.country.isEmpty()) {
            if(countries.putIfAbsent(account.country.trim(), (short)countrySequence.get()) == null) {
                wasAdded = true;
                countrySequence.incrementAndGet();
            }
        }

        if (account.city != null && !account.city.isEmpty()) {
            if(cities.putIfAbsent(account.city.trim(), (short)citySequence.get()) == null) {
                wasAdded = true;
                citySequence.incrementAndGet();
            }
        }

        if (account.email != null && !account.email.isEmpty()) {
            try {
                if(emailDomainsMap.putIfAbsent(account.email.toLowerCase().trim().split("@")[1], (short)shortSequence.get()) == null) {
                    wasAdded = true;
                }
            } catch (Exception e) {
                System.out.println("email error: " + account.email);
            }
        }

        if (account.interests != null && !account.interests.isEmpty()) {
            account.interests.forEach(interest -> {
                if (!interests.containsKey(interest.trim())) {
                    interests.put(interest.trim(), (short)shortSequenceForInterests.getAndIncrement());
                }
            });
        }

        if (wasAdded) {
            shortSequence.incrementAndGet();
        }
    }

    public static void fillDicts(Accounts accounts) {
        accounts.accounts.forEach(Dictionaries::fillDictsForOneOnInit);

        fnames.forEach((key, value) -> fnamesById.put(value, key));
        snames.forEach((key, value) -> snamesById.put(value, key));
        countries.forEach((key, value) -> countriesById.put(value, key));
        cities.forEach((key, value) -> citiesById.put(value, key));
        emailDomainsMap.forEach((key, value) -> emailDomainsById.put(value, key));
        interests.forEach((key, value) -> interestsById.put(value, key));
    }

}
