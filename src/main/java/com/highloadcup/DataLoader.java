package com.highloadcup;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.highloadcup.model.Account;
import com.highloadcup.model.Accounts;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.highloadcup.ArrayDataAllocator.NOW_TIMESTAMP;

public class DataLoader {

    private static final Logger logger = LogManager.getLogger(DataLoader.class);

    private static Gson gson = new Gson();

    public static class AccountsId {
        public List<Account> accounts = new ArrayList<>();
    }

    public static class AccountId {
        public int id;
    }

    public static boolean tsInited = false;

    public static void initTS(String ts) {
        if (!tsInited) {
            tsInited = true;
            NOW_TIMESTAMP = Long.parseLong(ts);
            logger.error("now timestamp from file - {}", NOW_TIMESTAMP);
        }
    }

    public static void loadFromJson() throws Exception {


        try (Stream<String> stream = Files.lines(Paths.get("/tmp/data/options.txt"))) {
            stream.forEach(DataLoader::initTS);
        } catch (Exception e) {
            logger.error("no options.txt found");
            //NOW_TIMESTAMP = 1545834025; // - full
            NOW_TIMESTAMP = 1548341893; // - test
        }

        //work vm
        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/opt/data/data.zip")); ///tmp/data

        //work
        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("C:\\work\\highloadcup\\test_data\\data\\data.zip")); //small
        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("C:\\work\\highloadcup\\full_test\\data\\data.zip")); //full
        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("C:\\work\\dating\\data.zip")); ///tmp/data

        //home


        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/opt/highloadcup/data/test_data/data/data.zip")); // small
        //final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/opt/highloadcup/data/full/data/data.zip")); // full

        final BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/tmp/data/data.zip")); ///tmp/data
        final ZipInputStream is = new ZipInputStream(bis);

        //load only account IDs
            /*final List<Integer> allIds = new ArrayList<>(1400000);
            try {
                ZipEntry entry;
                while ((entry = is.getNextEntry()) != null) {
                    JsonReader reader = new JsonReader(new InputStreamReader(is, "UTF-8"));
                    AccountsId accounts = (AccountsId)gson.fromJson(reader, AccountsId.class);
                    accounts.accounts.forEach( a -> allIds.add(a.id));
                    System.gc();
                }
            } finally {
                is.close();
            }

            Collections.sort(allIds);*/

        final ExecutorService e = Executors.newFixedThreadPool(1);

        int count = 0;
        try {
            ZipEntry entry;
            while ((entry = is.getNextEntry()) != null) {
                List<InternalAccount> internalAccountList = new ArrayList<>();
                JsonReader reader = new JsonReader(new InputStreamReader(is, "UTF-8"));
                Accounts accounts = (Accounts)gson.fromJson(reader, Accounts.class);
                count += accounts.accounts.size();
                //logger.error("read another json part; accs - {}", count);
                e.execute(new Runnable() {
                    @Override
                    public void run() {
                    //    logger.error("parse another json part");
                        Dictionaries.fillDicts(accounts);
                        //accountsFull.accounts.addAll(accounts.accounts);
                        accounts.accounts.forEach(a -> internalAccountList.add(InternalAccount.convert(a)));
                        //ArrayDataAllocator.increaseArrays(internalAccountList.size());
                        ArrayDataAllocator.fillData(internalAccountList);
                        //System.gc();
                    }
                });

                    /*if (ArrayDataAllocator.snameGlobal.length > 300000) {
                        break;
                    }*/
                //System.gc();
            }
        } finally {
            is.close();
        }
        e.shutdown();
        e.awaitTermination(5, TimeUnit.MINUTES);
        System.gc();
    }

}
