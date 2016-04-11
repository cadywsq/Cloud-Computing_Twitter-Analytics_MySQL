package query3;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static util.Utility.GetConnection;
import static util.Utility.InitializePooler;
import static util.Utility.ReleaseConnection;
import static util.Utility.connectionPool;

public class Query3DAO {

    public  Query3DAO() {
    	if(connectionPool == null) {
    		InitializePooler();
    	}
    }
    
    public int[] getWordCount(int startDate, int endDate, long startUid, long endUid, String word1, String word2, String word3) {
        // Use PreparedStatement instead of statement for performance optimization
        // so that the SQL statement that is sent gets pre-compiled (i.e. a query plan is prepared) in the DBMS
    	Statement stmt = null;
        int[] res = new int[3];
        Connection conn = null;
        try {
            conn = GetConnection();
            String sql = new StringBuilder().append("SELECT words FROM wordcount WHERE user_id BETWEEN ").append(startUid).append(" AND ").append(endUid).append(" AND date BETWEEN ").append(startDate).append(" AND ").append(endDate).toString();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            String temp = null;
            while (rs.next()) {
                temp = ";" + rs.getString("words") + ";";
                res[0] += findNum(temp , word1); 
                res[1] += findNum(temp , word2); 
                res[2] += findNum(temp , word3); 
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
    public int findNum(String temp, String word) {
    	int index = temp.indexOf(word), res = 0;
    	if (index != -1) {
    		for (int i = index + word.length(); temp.charAt(i) != ';'; i++) {
    			res = res * 10 + temp.charAt(i) - 48;
    		}
    	}
    	return res;
    }
}
