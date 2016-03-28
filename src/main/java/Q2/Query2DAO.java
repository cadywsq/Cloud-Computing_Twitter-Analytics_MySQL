package Q2;

import com.mysql.jdbc.PreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Query2DAO {
    private static BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>(1000);
    // Name of database
    private static final String DB_NAME = "chang";

    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";

    private static final String DB_USER = "root";
    private static final String DB_PWD = "1111";

    public Query2DAO() {
        for (int i = 0; i < 1000; i++) {
            try {
                connectionPool.add(DriverManager.getConnection(URL, DB_USER, DB_PWD));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Get connection from pool to minimize latency of connection
    private Connection getConnection() {
        // If connection pool has idle connection, use it
        try {
            return connectionPool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Put connection back to pool for reuse
    private void releaseConnection(Connection con) {
        connectionPool.offer(con);
    }

    public String getTweetByUserAndHT(String userAndHashtag) {
        // Use PreparedStatement instead of statement for performance optimization
        // so that the SQL statement that is sent gets pre-compiled (i.e. a query plan is prepared) in the DBMS
        PreparedStatement stmt = null;
        String res = null;
        Connection conn = getConnection();
        ;
        try {
            String tableName = "tweets";
            String sql = "SELECT content FROM " + tableName + " WHERE user_ht = '" + userAndHashtag + "';";
            stmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                res = rs.getString("content");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // release connection to pool after use
            releaseConnection(conn);
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
