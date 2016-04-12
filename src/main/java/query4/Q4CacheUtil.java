package query4;

import util.Utility;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Q4CacheUtil {

    private static Utility.Cache<String, Tweet> caches = new Utility.Cache();

    static void processSetCache(String tweetId, String fields, String payload) {
        String[] fieldList = fields.split(",");
        String[] payloadList = payload.split(",");
        Tweet cache;
        if (caches.containsKey(tweetId)) {
            cache = caches.get(tweetId);
        } else {
            cache = new Tweet(tweetId);
            caches.put(tweetId, cache);
        }
        for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
            cache.setField(fieldList[i], payloadList[i]);
        }
        if (payload.endsWith(",")) {
            cache.setField(fieldList[fieldList.length - 1], "");
        }
    }

    static String processGetCache(String tweetId, String fields) {
        if (fields == null || fields.isEmpty()) {
            return "";
        }
        String[] fieldList = fields.split(",");
        StringBuilder builder = new StringBuilder();
        if (caches.containsKey(tweetId)) {
            Tweet cache = caches.get(tweetId);
            for (String field : fieldList) {
                if (cache.getField(field) != null) {
                    builder.append(cache.getField(field) + ",");
                }
            }
            return builder.toString().substring(0, builder.length() - 1);
        }
        return "";
    }

    static class Tweet {
        private Map<String, String> fields;

        public Tweet(String tweetId) {
            fields = new HashMap<>();
        }

        public void setField(String field, String payload) {
            fields.put(field, payload);
        }

        public String getField(String field) {
            return fields.get(field);
        }
    }

    //    field      |          type          |                 example                 |
//            |-----------------|------------------------|-----------------------------------------|
//            | tweetid         | long int               | 15213                                   |
//            | userid          | long int               | 156190000001                            |
//            | username        | string                 | CloudComputing                          |
//            | timestamp       | string                 | Mon Feb 15 19:19:57 2016                |
//            | text            | string                 | Welcome to P4!#CC15619#P3               |
//            | hashtag         | comma separated string | CC15619,P3                              |
//            | ip              | string                 | 128.2.217.13                            |
//            | coordinates     | string                 | -75.14310264,40.05701649                |
//            | repliedby       | comma separated userid | 156190000001,156190000002,156190000003  |
//            | reply_count     | long int               | 3                                       |
//            | mentioned       | comma separated userid | 156190000004,156190000005,156190000006  |
//            | mentioned_count | long int               | 3                                       |
//            | favoritedby     | comma separated userid | 156190000007,156190000008,156190000009  |
//            | favorite_count  | long int               | 3                                       |
//            | useragent       | string                 | Mozilla/5.0 (iPhone; CPU iPhone OS ...) |
//            | filter_level    | string                 | PG-13                                   |
//            | lang            | string                 | American
//    static class Tweet {
//        private String tweetId;
//        private String userId;
//        private String username;
//        private String timestamp;
//        private String text;
//        private String hashtag;
//        private String ip;
//        private String coordinates;
//        private String repliedBy;
//        private String replyCount;
//        private String mentioned;
//        private String mentionedCount;
//        private String favoritedBy;
//        private String favoriteCount;
//        private String userAgent;
//        private String filterLevel;
//        private String lang;
//
//        public Tweet(String tweetId) {
//            this.setTweetId(tweetId);
//        }
//
//        public void setField(String field, String payload) {
//            switch (field) {
//                case "tweetid":
//                    this.setTweetId(payload);
//                    break;
//                case "userid":
//                    this.setUserId(payload);
//                    break;
//                case "username":
//                    this.setUsername(payload);
//                    break;
//                case "timestamp":
//                    this.setTimestamp(payload);
//                    break;
//                case "text":
//                    this.setText(payload);
//                    break;
//                case "hashtag":
//                    this.setHashtag(payload);
//                    break;
//                case "ip":
//                    this.setIp(payload);
//                    break;
//                case "coordinates":
//                    this.setCoordinates(payload);
//                    break;
//                case "repliedby":
//                    this.setRepliedBy(payload);
//                    break;
//                case "reply_count":
//                    this.setReplyCount(payload);
//                    break;
//                case "mentioned":
//                    this.setMentioned(payload);
//                    break;
//                case "mentioned_count":
//                    this.setMentionedCount(payload);
//                    break;
//                case "favoritedby":
//                    this.setFavoritedBy(payload);
//                    break;
//                case "favorited_count":
//                    this.setFavoriteCount(payload);
//                    break;
//                case "useragent":
//                    this.setUserAgent(payload);
//                    break;
//                case "filter_level":
//                    this.setFilterLevel(payload);
//                    break;
//                case "lang":
//                    this.setLang(payload);
//                    break;
//            }
//        }
//
//        public String getField(String field) {
//            String payload = null;
//            switch (field) {
//                case "tweetid":
//                    payload = this.getTweetId();
//                    break;
//                case "userid":
//                    payload = this.getUserId();
//                    break;
//                case "username":
//                    payload = this.getUsername();
//                    break;
//                case "timestamp":
//                    payload = this.getTimestamp();
//                    break;
//                case "text":
//                    payload = this.getText();
//                    break;
//                case "hashtag":
//                    payload = this.getHashtag();
//                    break;
//                case "ip":
//                    payload = this.getIp();
//                    break;
//                case "coordinates":
//                    payload = this.getCoordinates();
//                    break;
//                case "repliedby":
//                    payload = this.getRepliedBy();
//                    break;
//                case "reply_count":
//                    payload = this.getReplyCount();
//                    break;
//                case "mentioned":
//                    payload = this.getMentioned();
//                    break;
//                case "mentioned_count":
//                    payload = this.getMentionedCount();
//                    break;
//                case "favoritedby":
//                    payload = this.getFavoritedBy();
//                    break;
//                case "favorited_count":
//                    payload = this.getFavoriteCount();
//                    break;
//                case "useragent":
//                    payload = this.getUserAgent();
//                    break;
//                case "filter_level":
//                    payload = this.getFilterLevel();
//                    break;
//                case "lang":
//                    payload = this.getLang();
//                    break;
//            }
//            return payload;
//        }
//
//        public String getTweetId() {
//            return tweetId;
//        }
//
//        public void setTweetId(String tweetId) {
//            this.tweetId = tweetId;
//        }
//
//        public String getUserId() {
//            return userId;
//        }
//
//        public void setUserId(String userId) {
//            this.userId = userId;
//        }
//
//        public String getUsername() {
//            return username;
//        }
//
//        public void setUsername(String username) {
//            this.username = username;
//        }
//
//        public String getTimestamp() {
//            return timestamp;
//        }
//
//        public void setTimestamp(String timestamp) {
//            this.timestamp = timestamp;
//        }
//
//        public String getText() {
//            return text;
//        }
//
//        public void setText(String text) {
//            this.text = text;
//        }
//
//        public String getHashtag() {
//            return hashtag;
//        }
//
//        public void setHashtag(String hashtag) {
//            this.hashtag = hashtag;
//        }
//
//        public String getIp() {
//            return ip;
//        }
//
//        public void setIp(String ip) {
//            this.ip = ip;
//        }
//
//        public String getCoordinates() {
//            return coordinates;
//        }
//
//        public void setCoordinates(String coordinates) {
//            this.coordinates = coordinates;
//        }
//
//        public String getRepliedBy() {
//            return repliedBy;
//        }
//
//        public void setRepliedBy(String repliedBy) {
//            this.repliedBy = repliedBy;
//        }
//
//        public String getReplyCount() {
//            return replyCount;
//        }
//
//        public void setReplyCount(String replyCount) {
//            this.replyCount = replyCount;
//        }
//
//        public String getMentioned() {
//            return mentioned;
//        }
//
//        public void setMentioned(String mentioned) {
//            this.mentioned = mentioned;
//        }
//
//        public String getMentionedCount() {
//            return mentionedCount;
//        }
//
//        public void setMentionedCount(String mentionedCount) {
//            this.mentionedCount = mentionedCount;
//        }
//
//        public String getFavoritedBy() {
//            return favoritedBy;
//        }
//
//        public void setFavoritedBy(String favoritedBy) {
//            this.favoritedBy = favoritedBy;
//        }
//
//        public String getFavoriteCount() {
//            return favoriteCount;
//        }
//
//        public void setFavoriteCount(String favoriteCount) {
//            this.favoriteCount = favoriteCount;
//        }
//
//        public String getUserAgent() {
//            return userAgent;
//        }
//
//        public void setUserAgent(String userAgent) {
//            this.userAgent = userAgent;
//        }
//
//        public String getFilterLevel() {
//            return filterLevel;
//        }
//
//        public void setFilterLevel(String filterLevel) {
//            this.filterLevel = filterLevel;
//        }
//
//        public String getLang() {
//            return lang;
//        }
//
//        public void setLang(String lang) {
//            this.lang = lang;
//        }
//    }
}
