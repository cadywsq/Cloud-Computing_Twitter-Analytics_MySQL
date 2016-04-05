package utility;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Utility {
    public static final String TEAM_ID = "SilverLining";
    public static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    private static final int MAX_ENTRIES = 1000000;

    // Name of database
    // private static final String DB_NAME = "chang";
    private static final String URL = "jdbc:mysql://localhost/chang?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private static final String DB_USER = "root";
    private static final String DB_PWD = "123";
    public static BoneCP connectionPool;

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


    public static void InitializePooler() {
        try {
            BoneCPConfig config = new BoneCPConfig();
            config.setJdbcUrl(URL);
            config.setUsername(DB_USER);
            config.setPassword(DB_PWD);
            config.setMinConnectionsPerPartition(5);
            config.setMaxConnectionsPerPartition(1000);
            config.setPartitionCount(1);
            connectionPool = new BoneCP(config);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void ReleasePooler() {
        if (connectionPool != null) {
            connectionPool.shutdown();
        }
    }

    public static Connection GetConnection() {
        Connection connection = null;
        try {
            connection = connectionPool.getConnection();
        } catch (Exception ex) {
            ex.getMessage();
        }
        return connection;
    }

    public static void ReleaseConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ex) {
            ex.getMessage();
        }
    }

    public static String formatResponse() {
        String header = TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n";
        return header + "\n";
    }

}
