package query4;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static util.Utility.GetConnection;
import static util.Utility.InitializePooler;
import static util.Utility.ReleaseConnection;
import static util.Utility.connectionPool;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Q4WriteUtil {
    private static final String TABLE_NAME = "newtweets";
    private static final Set<String> storedTweetIds = new HashSet<>();


    public Q4WriteUtil() {
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

    List<RequestQueue.Request> mergeRequests(List<RequestQueue.Request> requestList) {
        ArrayList<RequestQueue.Request> res = new ArrayList<>();

        RequestQueue.Request lastSet = null;
        for (RequestQueue.Request cur: requestList) {
            if (cur.getRequest().getParameter("op").equals("set")) {
                lastSet = cur;
            } else {
                if (lastSet != null) {
                    res.add(lastSet);
                    lastSet = null;
                }
                res.add(cur);
            }
        }
        return res;
    }
}
