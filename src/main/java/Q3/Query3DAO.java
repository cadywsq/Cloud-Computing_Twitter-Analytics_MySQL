package Q3;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
public class Query3DAO {
    // Name of database
   // private static final String DB_NAME = "chang";
    private static final String URL = "jdbc:mysql://localhost/chang";
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
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	public static void ReleasePooler() {
		if(connectionPool != null) {
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
		}	catch (Exception ex) {
			ex.getMessage();
		}
	}
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
