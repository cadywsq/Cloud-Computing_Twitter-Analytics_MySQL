package query4;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.io.PrintWriter;
//import java.net.HttpURLConnection;
//import java.net.MalformedURLException;
//import java.net.URL;
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

    public Q4Servlet() throws SQLException {
        dao = new Q4WriteHelper();
    }
    public void initInstance () {
        instances = new String[6];
        instances[0] = "ec2-52-91-225-126.compute-1.amazonaws.com";
        instances[1] = "ec2-52-90-232-209.compute-1.amazonaws.com";
        instances[2] = "ec2-52-91-216-37.compute-1.amazonaws.com";
        instances[3] = "ec2-52-91-23-158.compute-1.amazonaws.com";
        instances[4] = "ec2-52-91-60-8.compute-1.amazonaws.com";
        instances[5] = "ec2-54-87-192-127.compute-1.amazonaws.com";
    }
    @Override
    public void init(ServletConfig config) throws ServletException {
        initInstance();
        super.init(config);
        map = new HashMap<>();
    }
//    private StringBuilder getHttpResponse(String dcDns, String path) throws MalformedURLException {
//        String submitString = "http://" + dcDns + "/q4?" +path;
//        System.out.println(submitString);
//        URL url = new URL(submitString);
//        HttpURLConnection httpConnection = null;
//        int responseCode = 0;
//        while (true) {
//            try {
//                while (responseCode != HttpURLConnection.HTTP_OK) {
//                    Thread.sleep(1);
//                    httpConnection = (HttpURLConnection) url.openConnection();
//                    responseCode = httpConnection.getResponseCode();
//                }
//                BufferedReader br = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));
//                String response;
//                StringBuilder builder = new StringBuilder();
//                while ((response = br.readLine()) != null) {
//                    builder.append(response);
//                }
//                return builder;
//            } catch (Exception e) {
////                System.out.println("Response Code:" + responseCode);
////                System.out.println("Try Again");
//            }
//        }
//    }
    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        if (req.getParameter("forward") == null) {
            int target = hashLocation(tweetId);
            System.out.println(req.getQueryString());
            resp.sendRedirect("http://" + instances[target] + "/q4?" + req.getQueryString() + "&forward=true");
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

    int hashLocation(String key) {
        int hash = 0;
        char[] chars = key.toCharArray();
        for (char c : chars) {
            hash += ~c;
        }
        return Math.abs(hash % 6);
    }
}
