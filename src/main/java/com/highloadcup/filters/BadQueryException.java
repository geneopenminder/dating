package com.highloadcup.filters;

public class BadQueryException extends RuntimeException {

    public BadQueryException(String message) {
        super(message);
    }

}
