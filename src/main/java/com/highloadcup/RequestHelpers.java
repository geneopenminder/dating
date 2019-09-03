package com.highloadcup;

import java.util.Map;

public class RequestHelpers {

    public static boolean checkForNumeric(Map<String,String[]> params, String paramName) {
        if (params == null || !params.containsKey(paramName) || params.get(paramName) == null || params.get(paramName).length == 0
                || params.get(paramName)[0] == null || params.get(paramName)[0].length() == 0 ||
                !params.get(paramName)[0].chars().allMatch(Character::isDigit)) {
            return false;
        }
        return true;
    }

    public static boolean checkForExists(Map<String,String[]> params, String paramName) {
        if (params == null || !params.containsKey(paramName) || params.get(paramName) == null || params.get(paramName).length == 0
                || params.get(paramName)[0] == null || params.get(paramName)[0].length() == 0) {
            return false;
        }
        return true;
    }


}
