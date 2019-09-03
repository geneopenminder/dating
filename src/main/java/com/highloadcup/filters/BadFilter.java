package com.highloadcup.filters;

public class BadFilter extends BaseFilter {

    public BadFilter() {
    }

    public BadFilter(String predicate, String[] value, int limit) {
        super(predicate, value, limit);
    }

    @Override
    public BaseFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        return null;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        return false;
    }
}
