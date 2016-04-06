package query4;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static utility.Utility.GetConnection;
import static utility.Utility.InitializePooler;
import static utility.Utility.ReleaseConnection;
import static utility.Utility.connectionPool;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class WriteQ4DAO {
    private static final String TABLE_NAME = "newtweets";
    private static final Set<String> storedTweetIds = new HashSet<>();


    public WriteQ4DAO() {
        if (connectionPool == null) {
            InitializePooler();
        }
    }

    String getQuery(String tweetId, String fields, String payload) {
        String[] fieldList = fields.split(",");
        String[] payloadList = payload.split(",");

        String query;
        StringBuilder builder = new StringBuilder();
        StringBuilder allFields = new StringBuilder();
        StringBuilder allPayloads = new StringBuilder();

        if (storedTweetIds.contains(tweetId)) {
            for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
                builder.append(fieldList[i] + "=" + payloadList[i] + ",");
            }
            query = "UPDATE " + TABLE_NAME + " SET " + toString(builder) + " WHERE tweetid=" + tweetId;
        } else {
            for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
                allFields.append(fieldList[i] + ",");
                allPayloads.append(payloadList[i] + ",");
            }
            query = "INSERT INTO " + TABLE_NAME + "(" + toString(allFields) + ")" + " VALUES " + "(" + toString
                    (allPayloads) + ")";
            storedTweetIds.add(tweetId);
        }
        return query;
    }

    private String toString(StringBuilder allFields) {
        return allFields.toString().substring(0, allFields.length() - 1);
    }

    void putData(String query) {
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = GetConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate(query);

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
    }

    String getData(String tweetId, String fields) {
        Statement stmt = null;
        Connection conn = null;
        ResultSet rs;
        StringBuilder builder = new StringBuilder();
        String[] fieldList = fields.split(",");
        for (String field : fieldList) {
            builder.append(field + ",");
        }
        String query = toString(builder);

        StringBuilder result = new StringBuilder();
        try {
            conn = GetConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT " + query + "FROM" + TABLE_NAME + "WHERE tweetid=" + tweetId);
            if (rs.next()) {
                for (String field : fieldList) {
                    result.append(rs.getString(field) + ",");
                }
            }
            return toString(result);
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
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
    }
}
