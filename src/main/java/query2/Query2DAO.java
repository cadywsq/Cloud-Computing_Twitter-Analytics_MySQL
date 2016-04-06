package query2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static utility.Utility.GetConnection;
import static utility.Utility.InitializePooler;
import static utility.Utility.ReleaseConnection;
import static utility.Utility.connectionPool;

public class Query2DAO {
//    private static List<Connection> connectionPool = new ArrayList<>();
//    private static BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>(1000);
    // Name of database

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
