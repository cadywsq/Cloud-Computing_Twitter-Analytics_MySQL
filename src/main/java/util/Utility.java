package util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Utility {
    public static final String TEAM_ID = "SilverLining";
    public static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    private static final int MAX_ENTRIES = 1000000;

    /**
     * Override LinkedHashMap removeEldestEntry method & override get/put method for synchronization.
     */
    public static class Cache<K, V> extends LinkedHashMap<K, V> {
        // Set initial size to avoid LinkedHashMap to resize.
        public Cache() {
            super((int) (MAX_ENTRIES / 0.75 + 1), 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<K, V> cacheMap) {
            return size() > MAX_ENTRIES;
        }

        @Override
        public synchronized V get(Object key) {
            return super.get(key);
        }

        @Override
        public synchronized V put(K key, V value) {
            return super.put(key, value);
        }
    }


    public static StringBuilder formatResponse() {
        StringBuilder header = new StringBuilder(TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n");
        return header;
    }

}
