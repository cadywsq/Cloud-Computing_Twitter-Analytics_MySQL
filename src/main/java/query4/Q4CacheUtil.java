package query4;

import util.Utility;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Q4CacheUtil {

    private static Utility.Cache<String, Tweet> caches = new Utility.Cache();

    static void setCache(String tweetId, String fields, String payload) {
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

    static String getCache(String tweetId, String fields) {
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
        }
        if (builder.toString().length() > 0) {
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
}
