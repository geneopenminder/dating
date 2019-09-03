package com.highloadcup.filters;

import java.util.List;

public interface GroupFilter {

    public List<GroupItem> group(long[] set, List<GroupItem> existGroupsm, boolean singleKey, int direction, int limit);

}
