package com.highloadcup;

import com.highloadcup.filters.CityFilter;
import com.highloadcup.model.Account;

import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UpdateProcessor {

    static final ExecutorService executor = Executors.newFixedThreadPool(1);

    public static boolean processNewAccount(Account account) {

        executor.submit(new AddAccountRunnable(account));

        return true;
    }

    public static class AddAccountRunnable implements Runnable {

        private Account a;

        public AddAccountRunnable(Account a) {
            this.a = a;
        }

        @Override
        public void run() {

        }
    }

    public static boolean processUpdateAccount(TreeMap account, int id) {

        executor.submit(new UpdateAccountRunnable(account, id));

        return true;
    }

    public static class UpdateAccountRunnable implements Runnable {

        private TreeMap a;
        private int id;

        public UpdateAccountRunnable(TreeMap a, int id) {
            this.a = a;
            this.id = id;
        }

        @Override
        public void run() {
            int internalId = (id);
            if (a.containsKey("fname")) {
                //FNameFilter.updateForAccount((String)a.get("fname"), internalId);
            }
            if (a.containsKey("country")) {
                //CountryFilter.updateForAccount((String)a.get("country"), internalId);
            }
            if (a.containsKey("city")) {
                //CityFilter.updateForAccount((String)a.get("city"), internalId);
            }
            if (a.containsKey("sname")) {
                //SNameFilter.updateForAccount((String)a.get("sname"), internalId);
            }
            if (a.containsKey("status")) {
                //StatusFilter.updateForAccount((String)a.get("status"), internalId);
            }
        }
    }

}
