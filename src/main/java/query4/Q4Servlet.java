package query4;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.Utility.formatResponse;

public class Q4Servlet extends HttpServlet {
    private static String[] instances;
    private Map<String, AtomicInteger> map;
    private static final Logger logger = Logger.getLogger("Phase3_Q4");
    private final Q4WriteHelper dao;
    private static final String dns = "http://ec2-54-87-192-127.compute-1.amazonaws.com/q4?";
    public Q4Servlet() throws SQLException {
        dao = new Q4WriteHelper();
    }
    public void initInstance () {
        instances = new String[6];
        instances[0] = "http://ec2-52-91-225-126.compute-1.amazonaws.com/q4?";
        instances[1] = "http://ec2-52-90-232-209.compute-1.amazonaws.com/q4?";
        instances[2] = "http://ec2-52-91-216-37.compute-1.amazonaws.com/q4?";
        instances[3] = "http://ec2-52-91-23-158.compute-1.amazonaws.com/q4?";
        instances[4] = "http://ec2-52-91-60-8.compute-1.amazonaws.com/q4?";
        instances[5] = "http://ec2-54-87-192-127.compute-1.amazonaws.com/q4?";
    }
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        map = new HashMap<>();
        initInstance();
    }
    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        int target = Math.abs(tweetId.hashCode()) % 6;
        if (!instances[target].equals(dns)) {
            System.out.println(req.getQueryString());
            resp.sendRedirect(instances[target] + req.getQueryString());
            return;
        }
        String operation = req.getParameter("op");
        String fields = req.getParameter("fields");
        String payload = req.getParameter("payload").replace(" ", "+");
        String seq = req.getParameter("seq");
        StringBuilder result = formatResponse();

        if (operation.equals("set")) {
            result.append("success");
            sendResponse(result, resp);
        }

        final int seqNum = Integer.parseInt(seq);
        AtomicInteger sequence;
        synchronized (map) {
            if (!map.containsKey(tweetId)) {
                map.put(tweetId, new AtomicInteger(0));
            }
            sequence = map.get(tweetId);
        }

        synchronized (sequence) {
            while (sequence.get() + 1 != seqNum) {
                try {
                    sequence.wait();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sequence.incrementAndGet();
            if (operation.equals("set")) {
                dao.putData(dao.getQuery(tweetId, fields, payload, seqNum));
                sequence.notifyAll();
            } else {
                String response;
                response = dao.getData(tweetId, fields);
                sequence.notifyAll();
                if (response != null && !response.isEmpty() && !response.equals("null") && !response.equals("NULL")) {
                    result.append(response);
                }
                sendResponse(result, resp);
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    private void sendResponse(StringBuilder result, HttpServletResponse response) {
        PrintWriter writer;
        try {
            writer = response.getWriter();
            writer.println(result.toString());
            writer.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed when get writer for response");
        }
    }

    int hashLocation(String key) {
        int hash = 0;
        char[] chars = key.toCharArray();
        for (char c : chars) {
            hash += ~c;
        }
        return Math.abs(hash % 6);
    }
}
