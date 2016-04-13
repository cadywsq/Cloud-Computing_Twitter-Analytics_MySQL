package query2;

import util.ConnectionHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Query2DAO {

    private final ConnectionHelper connectionHelper;

    public Query2DAO() throws SQLException {
        connectionHelper = new ConnectionHelper();
    }

    public String getTweetByUserAndHT(String userAndHashtag) {
        // Use PreparedStatement instead of statement for performance optimization
        // so that the SQL statement that is sent gets pre-compiled (i.e. a query plan is prepared) in the DBMS
        Statement stmt = null;
        String res = null;
        Connection conn = null;

        try {
            conn = connectionHelper.getConnection();
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
            connectionHelper.releaseConnection(conn);
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
