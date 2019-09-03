package com.highloadcup.model;

public class FilterBaseAccount implements Comparable<FilterBaseAccount> {

    public int externalId;

    @Override
    public int compareTo(FilterBaseAccount o2) {
        return Integer.compare(o2.externalId, this.externalId); //high to low
    }

}
