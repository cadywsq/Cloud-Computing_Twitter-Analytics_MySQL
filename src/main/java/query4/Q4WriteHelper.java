package query4;

import util.ConnectionHelper;

import javax.servlet.http.HttpServletRequest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Q4WriteHelper {
    private static final String TABLE_NAME = "newtweets";
    private final ConnectionHelper connectionHelper;

    public Q4WriteHelper() throws SQLException {
        connectionHelper = new ConnectionHelper();
    }

    /**
     * Format the string of database SQL query for set requests.
     *
     * @param tweetId
     * @param fields
     * @param payload
     * @return
     */
    String getQuery(String tweetId, String fields, String payload, int seq) {
        String[] fieldList = fields.split(",");
        String[] payloadList = (payload + ",").split(",");
        String query;
        if (seq == 1) {
            StringBuilder allFields = new StringBuilder("tweetid,");
            StringBuilder allPayloads = new StringBuilder("'" + tweetId + "',");
            for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
                allFields.append(fieldList[i] + ",");
                allPayloads.append("'" + payloadList[i] + "'" + ",");
            }
            query = "INSERT INTO " + TABLE_NAME + "(" + toString(allFields) + ")" + " VALUES " + "(" + toString(allPayloads) + ")";

        } else {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < Math.min(fieldList.length, payloadList.length); i++) {
                builder.append(fieldList[i] + "='" + payloadList[i] + "',");
            }
            query = "UPDATE " + TABLE_NAME + " SET " + toString(builder) + " WHERE tweetid=" + tweetId;

        }
        return query;
    }

    private String toString(StringBuilder allFields) {
        if (!allFields.toString().isEmpty()) {
            return allFields.toString().substring(0, allFields.length() - 1);
        }
        return "";
    }

    void putData(String query) {
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = connectionHelper.getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate(query);

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
            conn = connectionHelper.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT " + query + " FROM " + TABLE_NAME + " WHERE tweetid=" + tweetId);
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
            connectionHelper.releaseConnection(conn);
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Merge continuous set request to one, for batch processing.
     *
     * @param requestList List of continuous Requests to be processed.
     * @return List of combined Requests to be processed
     */
    List<RequestQueue.Request> mergeRequests(List<RequestQueue.Request> requestList) {
        ArrayList<RequestQueue.Request> res = new ArrayList<>();
        RequestQueue.Request lastSet = null;
        System.out.println("requestQueue size before merge: " + requestList.size());
        for (RequestQueue.Request cur : requestList) {
            if (cur.getRequest().getParameter("op").equals("set")) {
                HttpServletRequest lastRequest = lastSet.getRequest();
                HttpServletRequest curRequest = cur.getRequest();
                String lastField = lastRequest.getParameter("field");
                String lastPayload = lastRequest.getParameter("payload");

                if (!lastField.isEmpty() && !lastPayload.isEmpty()) {
                    String curField = lastField + "," + curRequest.getParameter("field");
                    String curPayload = lastPayload + "," + curRequest.getParameter("payload");
                    cur.getRequest().setAttribute("field", curField);
                    cur.getRequest().setAttribute("payload", curPayload);
                }
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
