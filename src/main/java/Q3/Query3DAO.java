package Q3;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

public class Query3DAO {
    private static List<Connection> connectionPool = new ArrayList<Connection>();
    // Use JDBC driver to connect MySQL database
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    // Name of database
    private static final String DB_NAME = "chang";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private static final String DB_USER = "root";
    private static final String DB_PWD = "31415926";
    // Get connection from pool to minimize latency of connection
    private synchronized Connection getConnection() throws Exception {
        // If connection pool has idle connection, use it
        if (connectionPool.size() > 0) {
            return connectionPool.remove(connectionPool.size() - 1);
        }
        // else establish a new connection
        Class.forName(JDBC_DRIVER);
        return DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }
    // Put connection back to pool for reuse
    private synchronized void releaseConnection(Connection con) {
        connectionPool.add(con);
    }
    public List<StringBuilder> getWordCount(int startDate, int endDate, long startUid, long endUid) {
        // Use PreparedStatement instead of statement for performance optimization
        // so that the SQL statement that is sent gets pre-compiled (i.e. a query plan is prepared) in the DBMS
        PreparedStatement stmt = null;
        List<StringBuilder> res = new ArrayList<StringBuilder>();
        Connection conn;
        try {
            conn = getConnection();
            String tableName = "wordcount";
            String sql = "SELECT words FROM " + tableName + " WHERE (date >= " + startDate + " AND " + 
            "date <= " + endDate + " AND user_id >= " + startUid + " AND user_id <= " + endUid + " )";
            stmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
            	System.out.println(rs.getString("words"));
                res.add(new StringBuilder(rs.getString("words")));
            }
            // release connection to pool after use
            releaseConnection(conn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
