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


    private Map<String, AtomicInteger> map;
    private static final Logger logger = Logger.getLogger("Phase3_Q4");
    private final Q4WriteHelper dao;

    public Q4Servlet() throws SQLException {
        dao = new Q4WriteHelper();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        map = new HashMap<>();
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        String operation = req.getParameter("op");
        String fields = req.getParameter("fields");
        String payload = req.getParameter("payload").replace(" ", "+");
        String seq = req.getParameter("seq");
        StringBuilder result = formatResponse();
//        System.out.println(String.format("tweetid: %s\top: %s\tseq: %s", tweetId, operation, seq));

        // For set request, return response directly
        if (operation.equals("set")) {
            result.append("success");
            sendResponse(result, resp);
        }

        final int seqNum = Integer.parseInt(seq);
        AtomicInteger sequence;
        long startTime = System.nanoTime();
        synchronized (map) {
            if (!map.containsKey(tweetId)) {
                map.put(tweetId, new AtomicInteger(0));
            }
            sequence = map.get(tweetId);
        }
        System.out.println("Time to access map: " + (System.nanoTime() - startTime) + "ns");

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
                dao.putData(dao.getQuery(tweetId, fields, payload));
//                Q4CacheUtil.setCache(tweetId, fields, payload);
                sequence.notifyAll();
            } else {
//                String cached = Q4CacheUtil.getCache(tweetId, fields);
                String response;
//                if (!cached.equals("")) {
//                    response = cached;
//                } else {
                response = dao.getData(tweetId, fields);
//                }
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
}
