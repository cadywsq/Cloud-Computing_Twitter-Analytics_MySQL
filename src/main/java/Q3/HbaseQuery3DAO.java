package Q3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public class HbaseQuery3DAO {

    // TODO: change to master node private IP.
    static String zkAddr = "172.31.63.80";
    static Level logLevel = Level.INFO;
    /**
     * The name of your HBase table.
     */
    private static String tableName = "tweets3";
    /**
     * HBase connection.
     */
    static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private final static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;

    public HbaseQuery3DAO() throws IOException {
        initializeConnection();
    }
    /**
     * Initialize HBase connection.
     *
     * @throws IOException
     */
    static void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(logLevel);
        conf = HBaseConfiguration.create();
        // conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.master", "*" + zkAddr + ":9000*");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        conn = HConnectionManager.createConnection(conf);
    }

    public String findWordCount(Long id1, Long id2, int date1, int date2, String w1, String w2, String w3) throws Exception {
        HTableInterface table = null;
        ResultScanner rs = null;
        int[] count = new int[3];
        try {
            table = conn.getTable(Bytes.toBytes(tableName));
            Scan scan =  new Scan();
            byte[] dCol = Bytes.toBytes("did");
            byte[] bCol = Bytes.toBytes("word");
            scan.addColumn(bColFamily, dCol);
            scan.addColumn(bColFamily, bCol);


            List<RowRange> ranges = new ArrayList<RowRange>();
            for (int date = date1; date <= date2; date++) {
                String start = date + "," + id1;
                String end = date + "," + id2;
                ranges.add(new RowRange(Bytes.toBytes(start), true, Bytes.toBytes(end), true));
            }

            Filter filter = new MultiRowRangeFilter(ranges);
            scan.setFilter(filter);
            rs = table.getScanner(scan);

            Result next;
            while ((next=rs.next()) != null) {
                String[] wordcount = new String(next.value()).split(",");
                for(String kv : wordcount){
                    String[] entry = kv.split(":");
                    if (entry[0].equals(w1))
                        count[0] += Integer.valueOf(entry[1]);
                    else if(entry[0].equals(w2))
                        count[1] += Integer.valueOf(entry[1]);
                    else if(entry[0].equals(w3))
                        count[2] += Integer.valueOf(entry[1]);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            rs.close();
            try {
                table.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return String.format("%s:%d\n%s:%d\n%s:%d\n", w1, count[0], w2, count[1], w3, count[2]);
    }
}
