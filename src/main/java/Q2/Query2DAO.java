package Q2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class Query2DAO {
//    private static List<Connection> connectionPool = new ArrayList<>();
//    private static BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>(1000);
    // Name of database
    private static final String DB_NAME = "chang";

    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";

    private static final String DB_USER = "root";
    private static final String DB_PWD = "123";
    private static BoneCP connectionPool;

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

    public Query2DAO() {
        if (connectionPool == null) {
            InitializePooler();
        }
    }

    public String getTweetByUserAndHT(String userAndHashtag) {
        // Use PreparedStatement instead of statement for performance optimization
        // so that the SQL statement that is sent gets pre-compiled (i.e. a query plan is prepared) in the DBMS
        Statement stmt = null;
        String res = null;
        Connection conn = null;

        try {
            conn = GetConnection();
            String tableName = "tweets";
            String sql = "SELECT content FROM " + tableName + " WHERE user_ht = '" + userAndHashtag + "';";
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                res = rs.getString("content");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // release connection to pool after use
            ReleaseConnection(conn);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
        return res;
    }
}
