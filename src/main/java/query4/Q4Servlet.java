package query4;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class Sequence {
    int number;
    public Sequence (int number) {
        this.number = number;
    }
}
public class Q4Servlet extends HttpServlet {
    private static Map<String, Sequence> map;
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        map = new HashMap<String, Sequence>();
    }
    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        final int seq = Integer.parseInt(req.getParameter("seq"));
        if (map.get(tweetId) == null) {
            map.put(tweetId, new Sequence(0));
        }
        Sequence sequence = map.get(tweetId);
        synchronized (sequence) {
            while(map.get(tweetId).number != seq - 1) {
                try {
                    sequence.wait();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        map.get(tweetId).number++;
        sequence.notify();

    }
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
}
