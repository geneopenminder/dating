package com.highloadcup.model;

import com.highloadcup.DirectMemoryAllocator;

import java.util.List;

public class Account {

    public int id;
    public String email; //unicode <= 100
    public String fname; //unicode <=50; optional
    public String sname; //unicode <=50; optional
    public String phone; //unicode <= 16
    public String country; //unicode <=50; optional
    public String city; //unicode <=50; optional --very rare
    public String status; //"свободны", "заняты", "всё сложно". Не обращайте внимание на странные окончания :)
    public String sex; //"m" означает мужской пол, а "f" - женский
    public int birth; //epoch sec UNIX UTC 01.01.1950 и сверху 01.01.2005

    public int joined; //timestamp 01.01.2011, сверху 01.01.2018
    public List<String> interests; // arrays strings unicode <= 100

    public Premium premium;

    public int premiumStart;
    public int premiumFinish;

    public List<Like> likes;

    //for POST phase

    public long emailHash; //unicode <= 100
    public long fnameHash; //unicode <=50; optional
    public long snameHash; //unicode <=50; optional
    public long phoneHash; //unicode <= 16
    public long countryHash; //unicode <=50; optional
    public long cityHash; //unicode <=50; optional --very rare
    public long statusHash; //"свободны", "заняты", "всё сложно". Не обращайте внимание на странные окончания :)
    public long phoneNum;
    public long[] interestsHashes = new long[10];
    public int interestsNumber;

    public void clear() {
        this.id = 0;
        this.email = null;
        this.fname = null;
        this.sname = null;
        this.phone = null;
        this.sex = null;
        this.birth = 0;
        this.country = null;
        this.city = null;
        this.joined = 0;
        this.status = null;
        this.interests = null;
        this.premium = null;
        this.likes = null;
        this.premiumStart = 0;
        this.premiumFinish = 0;

        this.emailHash = 0;
        this.fnameHash = 0;
        this.snameHash = 0;
        this.phoneHash = 0;
        this.countryHash = 0;
        this.cityHash = 0;
        this.statusHash = 0;
        this.phoneNum = 0;
        DirectMemoryAllocator.clear(this.interestsHashes);
        this.interestsNumber = 0;
    }

    public static class Premium {
        //timestamps from 01.01.2018
        public int start;
        public int finish;

        public Premium() {
        }

        public Premium(int start, int finish) {
            this.start = start;
            this.finish = finish;
        }
    }

    public static class Like {
        public int id;
        public int ts; //timestamp

        public Like() {
        }


        public Like(int id, int ts) {
            this.id = id;
            this.ts = ts;
        }
    }

}
