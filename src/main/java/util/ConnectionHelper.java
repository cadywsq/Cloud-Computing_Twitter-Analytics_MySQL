package util;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Siqi Wang siqiw1 on 4/12/16.
 */
public class ConnectionHelper {
    private final BoneCP connectionPool;
    // Name of database
    // private static final String DB_NAME = "chang";
    private static final String URL = "jdbc:mysql://localhost/chang?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    private static final String DB_USER = "root";
    private static final String DB_PWD = "123";

    public ConnectionHelper() throws SQLException {
        BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PWD);
        config.setMinConnectionsPerPartition(5);
        config.setMaxConnectionsPerPartition(1000);
        config.setPartitionCount(1);
        connectionPool = new BoneCP(config);
    }


    public void releasePooler() {
        connectionPool.shutdown();
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            connection = connectionPool.getConnection();
        } catch (Exception ex) {
            ex.getMessage();
        }
        return connection;
    }

    public void releaseConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ex) {
            ex.getMessage();
        }
    }

}
