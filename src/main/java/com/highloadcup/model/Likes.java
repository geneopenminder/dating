package com.highloadcup.model;

import java.util.List;

public class Likes {

    public List<Like> likes;

    public static class Like {
        public int likee;
        public int ts;
        public int liker;

        public Like() {
        }

        public Like(int likee, int ts, int liker) {
            this.likee = likee;
            this.ts = ts;
            this.liker = liker;
        }
    }

}
