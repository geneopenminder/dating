package com.highloadcup.model;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.JoinedFilter;
import com.highloadcup.filters.PremiumFilter;

import java.util.ArrayList;
import java.util.List;

import static com.highloadcup.Dictionaries.shortSequenceForInterests;

public class InternalAccount {

    public int id;
    public int birth;
    public int joined;
    public long phone;

    //premium
    public String email;
    public List<String> interests = null;
    public long[] interestsHashes = null;
    public int interestsNumber;
    public int premiumStart;
    public int premiumFinish;

    public boolean isPremiumNow = false;

    public short fname;
    public short sname;
    public short country;
    public short city;

    public byte sex;
    public byte status;

    //public int[] likeTimespamps;
    public int[] likeIds;

    public List<Account.Like> likes;

    static ThreadLocal<InternalAccount> accTL = new ThreadLocal<InternalAccount>() {
        @Override
        protected InternalAccount initialValue() {
            return new InternalAccount();
        }
    };

    public void clear() {
        id = 0;
        birth = 0;
        joined = 0;
        phone = 0;

        //premium
        email = null;
        interests = null;
        interestsHashes = null;
        interestsNumber = 0;
        premiumStart = 0;
        premiumFinish = 0;

        isPremiumNow = false;

        fname = 0;
        sname = 0;
        city = 0;
        country = 0;

        sex = 0;
        status = 0;


        //likeTimespamps = null;
        likeIds = null;

        likes = null;
    }

    public static InternalAccount convert(Account account) {
        return convert(account, false);
    }

    public static InternalAccount convert(Account account, boolean useTl) {
        InternalAccount acc = null;

        if (useTl) {
            acc = accTL.get();
            acc.clear();
        } else {
            acc = new InternalAccount();
        }

        acc.id = account.id;
        acc.fname = account.fname != null ? Dictionaries.fnames.get(account.fname) : Short.MAX_VALUE;
        acc.sname = account.sname != null ? Dictionaries.snames.get(account.sname) : Short.MAX_VALUE;
        acc.email = account.email; // != null ? account.email.split("@")[0] : null;
        acc.status = getStatus(account.status);
        acc.sex = account.sex.equalsIgnoreCase("m") ?
                Byte.valueOf((byte)1) : Byte.valueOf((byte)0); //TODO
        if (account.phone != null && !account.phone.isEmpty()) {
            acc.phone = Long.parseLong(account.phone.replaceAll("\\+", "")
                    .replaceAll("\\(", "")
                    .replaceAll("\\)", "").trim());
        } else {
            acc.phone = Integer.MAX_VALUE;
        }
        acc.birth = account.birth;
        acc.city = account.city != null ? Dictionaries.cities.get(account.city) : Short.MAX_VALUE;
        acc.country = account.country != null ? Dictionaries.countries.get(account.country) : Short.MAX_VALUE;
        acc.joined = (int)(account.joined);

        short joinYear = JoinedFilter.getJoinYear(acc.joined);

        if (account.interests != null && !account.interests.isEmpty()) {
            acc.interests = account.interests;
        }

        if (account.premium != null && account.premium.start != 0 && account.premium.finish != 0) {
            acc.premiumStart = account.premium.start;
            acc.premiumFinish = account.premium.finish;
        }

        if (account.premiumStart > 0) {
            acc.premiumStart = account.premiumStart;
            acc.premiumFinish = account.premiumFinish;
        }

        if (account.likes != null && !account.likes.isEmpty()) {
            acc.likes = account.likes;
            //acc.likeTimespamps = account.likes.stream().mapToInt(like -> like.ts).toArray();
            if (!useTl) {
                acc.likeIds = account.likes.stream().mapToInt(like -> like.id).toArray();
            }
        }
        return acc;
    }

    public static synchronized InternalAccount convertFromHashes(Account account, boolean useTl) {
        InternalAccount acc = null;

        if (useTl) {
            acc = accTL.get();
            acc.clear();
        } else {
            acc = new InternalAccount();
        }

        acc.id = account.id;
        acc.fname = account.fnameHash != 0 ? Dictionaries.fnames.get(Dictionaries.escToUnescapedHashes.get(account.fnameHash)) : Short.MAX_VALUE;
        acc.sname = account.snameHash != 0 ? Dictionaries.snames.get(Dictionaries.escToUnescapedHashes.get(account.snameHash)) : Short.MAX_VALUE;
        acc.email = account.email; // != null ? account.email.split("@")[0] : null;
        acc.status = getStatusByHash(account.statusHash);
        acc.sex = account.sex.equalsIgnoreCase("m") ?
                Byte.valueOf((byte)1) : Byte.valueOf((byte)0); //TODO
        if (account.phoneNum != 0) {
            acc.phone = account.phoneNum;
        } else {
            acc.phone = Integer.MAX_VALUE;
        }
        acc.birth = account.birth;
        acc.city = account.cityHash != 0 ? Dictionaries.cities.get(Dictionaries.escToUnescapedHashes.get(account.cityHash)) : Short.MAX_VALUE;
        acc.country = account.countryHash != 0 ? Dictionaries.countries.get(Dictionaries.escToUnescapedHashes.get(account.countryHash)) : Short.MAX_VALUE;
        acc.joined = (int)(account.joined);

        if (account.interestsNumber > 0) {
            acc.interestsHashes = account.interestsHashes;
            acc.interestsNumber = account.interestsNumber;
        }

        if (account.premium != null && account.premium.start != 0 && account.premium.finish != 0) {
            acc.premiumStart = account.premium.start;
            acc.premiumFinish = account.premium.finish;
        }

        if (account.premiumStart > 0) {
            acc.premiumStart = account.premiumStart;
            acc.premiumFinish = account.premiumFinish;
        }

        if (account.likes != null && !account.likes.isEmpty()) {
            acc.likes = account.likes;
            //acc.likeTimespamps = account.likes.stream().mapToInt(like -> like.ts).toArray();
            if (!useTl) {
                acc.likeIds = account.likes.stream().mapToInt(like -> like.id).toArray();
            }
        }
        return acc;
    }

    static ThreadLocal<InternalAccount> accForUpdTL = new ThreadLocal<InternalAccount>() {
        @Override
        protected InternalAccount initialValue() {
            return new InternalAccount();
        }
    };

    public static synchronized InternalAccount convertForUpdateFromHashes(Account account) {
        InternalAccount acc = accForUpdTL.get();
        acc.clear();

        acc.id = account.id;
        acc.fname = account.fnameHash != 0 ? Dictionaries.fnames.get(Dictionaries.escToUnescapedHashes.get(account.fnameHash)) : Short.MAX_VALUE;
        acc.sname = account.snameHash != 0 ? Dictionaries.snames.get(Dictionaries.escToUnescapedHashes.get(account.snameHash)) : Short.MAX_VALUE;
        acc.email = account.email; // != null ? account.email.split("@")[0] : null;
        acc.status = getStatusByHash(account.statusHash);
        acc.sex = getSex(account.sex);
        if (account.phoneNum != 0) {
            acc.phone = account.phoneNum;
        } else {
            acc.phone = Integer.MAX_VALUE;
        }
        acc.birth = account.birth;
        acc.city = account.cityHash != 0 ? Dictionaries.cities.get(Dictionaries.escToUnescapedHashes.get(account.cityHash)) : Short.MAX_VALUE;
        acc.country = account.countryHash != 0 ? Dictionaries.countries.get(Dictionaries.escToUnescapedHashes.get(account.countryHash)) : Short.MAX_VALUE;
        acc.joined = account.joined > 0 ? (int)(account.joined) : 0;

        if (account.interestsNumber > 0) {
            acc.interestsHashes = account.interestsHashes;
            acc.interestsNumber = account.interestsNumber;
        }

        if (account.premium != null && account.premium.start != 0 && account.premium.finish != 0) {
            acc.premiumStart = account.premium.start;
            acc.premiumFinish = account.premium.finish;
        }

        if (account.premiumStart > 0) {
            acc.premiumStart = account.premiumStart;
            acc.premiumFinish = account.premiumFinish;
        }

        if (account.likes != null && !account.likes.isEmpty()) {
            acc.likes = account.likes;
            //acc.likeTimespamps = account.likes.stream().mapToInt(like -> like.ts).toArray();
            //acc.likeIds = account.likes.stream().mapToInt(like -> like.id).toArray();
        }
        return acc;
    }

    public static InternalAccount convertForUpdate(Account account) {
        InternalAccount acc = accForUpdTL.get();
        acc.clear();

        acc.id = account.id;
        acc.fname = account.fname != null ? Dictionaries.fnames.get(account.fname.trim()) : Short.MAX_VALUE;
        acc.sname = account.sname != null ? Dictionaries.snames.get(account.sname.trim()) : Short.MAX_VALUE;
        acc.email = account.email; // != null ? account.email.split("@")[0] : null;
        acc.status = getStatus(account.status);
        acc.sex = getSex(account.sex);
        if (account.phone != null && !account.phone.isEmpty()) {
            acc.phone = Long.parseLong(account.phone.replaceAll("\\+", "")
                    .replaceAll("\\(", "")
                    .replaceAll("\\)", "").trim());
        } else {
            acc.phone = Integer.MAX_VALUE;
        }
        acc.birth = account.birth;
        acc.city = account.city != null ? Dictionaries.cities.get(account.city.trim()) : Short.MAX_VALUE;
        acc.country = account.country != null ? Dictionaries.countries.get(account.country.trim()) : Short.MAX_VALUE;
        acc.joined = account.joined > 0 ? (int)(account.joined) : 0;

        if (account.interests != null && !account.interests.isEmpty()) {
            acc.interests = account.interests;
        }

        if (account.premium != null && account.premium.start != 0 && account.premium.finish != 0) {
            acc.premiumStart = account.premium.start;
            acc.premiumFinish = account.premium.finish;
        }

        if (account.premiumStart > 0) {
            acc.premiumStart = account.premiumStart;
            acc.premiumFinish = account.premiumFinish;
        }

        if (account.likes != null && !account.likes.isEmpty()) {
            acc.likes = account.likes;
            //acc.likeTimespamps = account.likes.stream().mapToInt(like -> like.ts).toArray();
            //acc.likeIds = account.likes.stream().mapToInt(like -> like.id).toArray();
        }
        return acc;
    }

    public static final List<String> EMPTY_INTERESTS = new ArrayList<>(0);

    public static short getNextId(boolean wasIncremented) {
        return wasIncremented ? (short)Dictionaries.shortSequence.incrementAndGet() : (short)Dictionaries.shortSequence.get();
    }

    public static InternalAccount convertForUpdateOrInsert(Account account, boolean isNew) {
        InternalAccount acc = new InternalAccount();

        acc.id = account.id;

        int internalIdx = (account.id);

        boolean wasIncremented = false;

        if (!isNew) {
            if (account.sname != null) {
                Short snameIdx = Dictionaries.snames.get(account.sname.trim());
                if (snameIdx == null) {
                    wasIncremented = true;
                    Dictionaries.snames.put(account.sname.trim(), getNextId(wasIncremented));
                    Dictionaries.snamesById.put(getNextId(wasIncremented), account.sname.trim());
                    ArrayDataAllocator.snameGlobal[internalIdx] = acc.sname;
                } else {
                    acc.sname = snameIdx;
                    ArrayDataAllocator.snameGlobal[internalIdx] = acc.sname;
                }
            } else {
                acc.sname = ArrayDataAllocator.snameGlobal[internalIdx];
            }

            if (account.fname != null) {
                Short fnameIdx = Dictionaries.fnames.get(account.fname.trim());
                if (fnameIdx == null) {
                    Dictionaries.fnames.put(account.fname.trim(), getNextId(wasIncremented));
                    Dictionaries.fnamesById.put(getNextId(wasIncremented), account.fname.trim());
                    ArrayDataAllocator.fnamesGlobal[internalIdx] = acc.fname;
                } else {
                    acc.fname = fnameIdx;
                    ArrayDataAllocator.fnamesGlobal[internalIdx] = acc.fname;
                }
            } else {
                acc.fname = ArrayDataAllocator.fnamesGlobal[internalIdx];
            }

            if (account.city != null) {
                Short cityIdx = Dictionaries.cities.get(account.city.trim());
                if (cityIdx == null) {
                    Dictionaries.cities.put(account.city.trim(), getNextId(wasIncremented));
                    Dictionaries.citiesById.put(getNextId(wasIncremented), account.city.trim());
                    ArrayDataAllocator.cityGlobal[internalIdx] = acc.city;
                } else {
                    acc.city = cityIdx;
                    ArrayDataAllocator.cityGlobal[internalIdx] = acc.city;
                }
            } else {
                acc.city = ArrayDataAllocator.cityGlobal[internalIdx];
            }

            if (account.country != null) {
                Short cntIdx = Dictionaries.countries.get(account.country.trim());
                if (cntIdx == null) {
                    Dictionaries.countries.put(account.country.trim(), getNextId(wasIncremented));
                    Dictionaries.countriesById.put(getNextId(wasIncremented), account.country.trim());
                    ArrayDataAllocator.countryGlobal[internalIdx] = acc.country;
                } else {
                    acc.country = cntIdx;
                    ArrayDataAllocator.countryGlobal[internalIdx] = acc.country;
                }
            } else {
                acc.country = ArrayDataAllocator.countryGlobal[internalIdx];
            }

            if (account.email != null) {
                if (!account.email.contains("@")) {
                    return null;
                }
                Integer emailId = ArrayDataAllocator.emailsGlobal.get(account.email);
                if (emailId != null) {
                    if (emailId != internalIdx) {
                        return null; //other email - fail
                    }
                } else {
                    //new email
                    //TODO validate email
                    String newEmail = account.email.trim();
                    String oldEmail = ArrayDataAllocator.emailsPlain[internalIdx];
                    ArrayDataAllocator.emailsGlobal.remove(oldEmail);
                    ArrayDataAllocator.emailsGlobal.put(newEmail, internalIdx);
                    ArrayDataAllocator.emailsPlain[internalIdx] = newEmail;

                    String emailDomain = account.email.toLowerCase().trim().split("@")[1];
                    Short domainIdx = Dictionaries.emailDomainsMap.get(emailDomain);

                    if (domainIdx == null) {
                        Dictionaries.emailDomainsMap.put(emailDomain, getNextId(wasIncremented));
                        Dictionaries.emailDomainsById.put(getNextId(wasIncremented), emailDomain);
                        domainIdx = getNextId(wasIncremented);
                    }
                    ArrayDataAllocator.emailDomains[internalIdx] = domainIdx;
                }
            } else {
                acc.email = ArrayDataAllocator.emailsPlain[internalIdx];
            }


            if (account.status != null) {
                acc.status = getStatus(account.status);
                if (acc.status == 0) {
                    return null;
                }
                ArrayDataAllocator.statusGlobal[internalIdx] = acc.status;
            } else {
                acc.status = ArrayDataAllocator.statusGlobal[internalIdx];
            }

            if (account.sex != null) {
                acc.sex = account.sex.equalsIgnoreCase("m") ?
                        Byte.valueOf((byte) 1) : Byte.valueOf((byte) 0);
                ArrayDataAllocator.sexGlobal[internalIdx] = acc.sex;
            } else {
                acc.sex = ArrayDataAllocator.sexGlobal[internalIdx];
            }

            if (account.birth != 0) {
                acc.birth = account.birth;
                DirectMemoryAllocator.putBirthDateToFS(internalIdx, acc.birth);
            } else {
                acc.birth = DirectMemoryAllocator.getBirthDateFromFS(internalIdx);
            }

            if (account.phone != null && !account.phone.isEmpty()) {
                acc.phone = Long.parseLong(account.phone.replaceAll("\\+", "")
                        .replaceAll("\\(", "")
                        .replaceAll("\\)", "").trim());
            } else {
                acc.phone = Integer.MAX_VALUE;
            }


            /*if (account.joined != 0) {
                acc.joined = (int) (account.joined - JoinedFilter.joinedBase);
                ArrayDataAllocator.joined[internalIdx] = acc.joined;
            } else {
                acc.joined = ArrayDataAllocator.joined[internalIdx];
            }*/

            if (account.interests != null && !account.interests.isEmpty()) {
                acc.interests = account.interests;

                ArrayDataAllocator.ShortArray sa = new ArrayDataAllocator.ShortArray();

                sa.array = new short[account.interests.size()];

                for (int i = 0; i < account.interests.size(); i++ ) {
                    Short interestId = Dictionaries.interests.get(account.interests.get(i).trim());
                    if (interestId == null) {
                        short nextId = (short)shortSequenceForInterests.incrementAndGet();
                        Dictionaries.interests.put(account.interests.get(i).trim(), nextId);
                        Dictionaries.interestsById.put(nextId, account.interests.get(i).trim());
                        interestId = nextId;
                    }
                    sa.array[i] = interestId; //Dictionaries.interests.get(account.interests.get(i).trim());
                }
                ArrayDataAllocator.accountsInterests[internalIdx] = sa;
            }
            //else {
            //    acc.interests = ArrayDataAllocator.accountsInterests[internalIdx] == null ? null : EMPTY_INTERESTS;
            //}

            if (account.premium != null && account.premium.start != 0 && account.premium.finish != 0) {

                acc.premiumStart = account.premium.start;
                acc.premiumFinish = account.premium.finish;

                ArrayDataAllocator.premiumStart[internalIdx] = acc.premiumStart;
                ArrayDataAllocator.premiumFinish[internalIdx] = acc.premiumFinish;

                if (PremiumFilter.isPremium(ArrayDataAllocator.premiumStart[internalIdx], ArrayDataAllocator.premiumFinish[internalIdx])) {
                    acc.isPremiumNow = true;
                    boolean wasNowPremium = DirectMemoryAllocator.isBitSet(PremiumFilter.nowPremiumBitMap, internalIdx);
                    if (!wasNowPremium) {
                        DirectMemoryAllocator.setBit(PremiumFilter.nowPremiumBitMap, internalIdx);
                        PremiumFilter.nowCount++;
                    }

                    boolean wasExistPremium = DirectMemoryAllocator.isBitSet(PremiumFilter.existPremiumBitMap, internalIdx);

                    if (!wasExistPremium) {
                        DirectMemoryAllocator.setBit(PremiumFilter.existPremiumBitMap, internalIdx);
                        PremiumFilter.notNullCount++;
                        DirectMemoryAllocator.unsetBit(PremiumFilter.nullPremiumBitMap, internalIdx);
                        PremiumFilter.nullCount--;
                    }

                }
            }
        }

        /*
        if (account.likes != null && !account.likes.isEmpty()) {
            acc.likeTimespamps = account.likes.stream().mapToLong(like -> like.likeTS).toArray();
            acc.likeIds = account.likes.stream().mapToInt(like -> like.id).toArray();
        }*/
        return acc;
    }




    public static byte getSex(String sex) {
        if (sex != null) {
            if (sex.trim().equalsIgnoreCase("m")) {
                return (byte)1;
            } else if (sex.trim().equalsIgnoreCase("f")) {
                return (byte)2;
            } else {
                throw new RuntimeException("wrong sex - " + sex);
            }
        }
        return (byte)0;
    }

     public static byte getStatus(String status) {
        if (status == null || status.isEmpty()) {
            return 0;
        } else if (status.toLowerCase().trim().startsWith("сво")) {
            return 1;
        } else if (status.toLowerCase().trim().startsWith("зан")) {
            return 2;
        } else if (status.toLowerCase().trim().startsWith("вс")) {
            return 3;
        } else {
            throw new RuntimeException("wrong status - " + status);
        }
    }

    public static byte getStatusByHash(long hash) {
        if (hash == 0) {
            return 0;
        } else if (hash == -3285224955899147795L) {
            return 1;
        } else if (hash == -945358381520804763L) {
            return 2;
        } else if (hash == 3119738862799528284L) {
            return 3;
        } else {
            throw new RuntimeException("wrong status hash - " + hash);
        }
    }

    public static String getStatus(int status) {
        if (status == 1) {
            return "свободны";
        } if (status == 2) {
            return "заняты";
        } else {
            return "всё сложно";
        }
    }


}
