package Q2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Q2Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";

    // Cache 1,000,000 key and value pairs
    private static LRUCache<String, String> cache = LRUCache.newInstance();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String userid = req.getParameter("userid");
        String hashtag = req.getParameter("hashtag");

        StringBuilder builder = new StringBuilder();
        builder.append(TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n");

        String key = userid + "," + hashtag;
        // Try to get value from cache
        String value = cache.get(key);
        // If it's in cached append value to result builder
        if (value != null) {
            builder.append(value);
        } else {
            //Get result from database
            Query2DAO dao = new Query2DAO();

            String tweets = dao.getTweetByUserAndHT(key);
            // Put key and value from database to cache
            cache.put(key, tweets);
            builder.append(tweets);
        }

        builder.append("\n");
        resp.setContentType("text/plain;charset=UTF-8");
        PrintWriter writer = resp.getWriter();
        writer.write(builder.toString());
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO Auto-generated method stub
        super.doPost(req, resp);
    }

    public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private static final long serialVersionUID = 2733236027662897287L;
        private static int size = 1000000;

        private LRUCache() {
            super(size, 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > size;
        }

        public static <K, V> LRUCache<K, V> newInstance() {
            return new LRUCache<K, V>();
        }
    }
}
