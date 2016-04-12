package query4;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.Utility.formatResponse;

public class Q4Servlet extends HttpServlet {

    static class Sequence {
        int sequence;

        public Sequence(int sequence) {
            this.sequence = sequence;
        }
    }

    private static ConcurrentHashMap<String, Sequence> map;
    private static Logger logger = Logger.getLogger("Phase3_Q4");

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        map = new ConcurrentHashMap<>();
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        String operation = req.getParameter("op");
        String fields = req.getParameter("fields");
        String payload = req.getParameter("payload").replace(" ", "+");
        String seq = req.getParameter("seq");
        StringBuilder result = formatResponse();
        System.out.println(String.format("tweetid: %s\top: %s\tseq: %s", tweetId, operation, seq));
        // Initialize connection pool
        new Q4WriteUtil();
        // For set request, return response directly
        if (operation.equals("set")) {
            result.append("success");
            sendResponse(result, resp);
        }

        final int seqNum = Integer.parseInt(seq);
        if (!map.containsKey(tweetId)) {
            map.put(tweetId, new Sequence(0));
        }
        Sequence sequence = map.get(tweetId);
        synchronized (sequence) {
            while (map.get(tweetId).sequence + 1 != seqNum) {
                try {
                    sequence.wait();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        sequence.sequence++;
        synchronized (sequence) {
            if (operation.equals("set")) {
                Q4WriteUtil.putData(Q4WriteUtil.getQuery(tweetId, fields, payload));
                Q4CacheUtil.processSetCache(tweetId, fields, payload);
            } else {
                String cached = Q4CacheUtil.processGetCache(tweetId, fields);
                String response;
                if (!cached.equals("")) {
                    response = cached;
                } else {
                    response = Q4WriteUtil.getData(tweetId, fields);
                }
                if (response != null || !response.isEmpty()) {
                    result.append(response);
                }
                sendResponse(result, resp);
            }
            sequence.notifyAll();
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
