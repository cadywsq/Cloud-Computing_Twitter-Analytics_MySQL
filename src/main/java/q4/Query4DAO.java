package q4;

import utility.Utility;

import java.util.HashMap;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Query4DAO {

    private Utility.Cache<String, HashMap<String, String>> caches = new Utility.Cache();

    String processSet(String tweetId, String fields, String payload) {
        String[] fieldList = fields.split(",");
        String[] payloadList = payload.split(",");
        if (caches.containsKey(tweetId)) {
            HashMap<String, String> cache = caches.get(tweetId);
            for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
                cache.put(fieldList[i], payloadList[i]);
            }
        } else {
            caches.put(tweetId, new HashMap<String, String>());
        }
        return "success\n";
    }

    String processGet(String tweetId, String fields) {
        if (fields == null || fields.isEmpty()) {
            return "";
        }
        String[] fieldList = fields.split(",");
        StringBuilder builder = new StringBuilder();
        if (caches.containsKey(tweetId)) {
            HashMap<String, String> cache = caches.get(tweetId);
            for (String field : fieldList) {
                if (cache.containsKey(field)) {
                    builder.append(cache.get(field) + ",");
                }
            }
        }
        return builder.toString().substring(0, builder.length() - 1) + "\n";
    }
}
