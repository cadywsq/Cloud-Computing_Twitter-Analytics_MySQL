package q2;

import utility.Utility;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static utility.Utility.TEAM_AWS_ACCOUNT;
import static utility.Utility.TEAM_ID;

public class Q2Servlet extends HttpServlet {

    // Cache 1,000,000 key and value pairs
    private static Utility.Cache cache = new Utility.Cache();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String userid = req.getParameter("userid");
        String hashtag = req.getParameter("hashtag");
        String key = userid + "#" + hashtag;
        System.out.println(userid + "\t" + hashtag);

        StringBuilder builder = new StringBuilder();
        builder.append(TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n");

        // Try to get value from cache
        String value = (String) cache.get(key);
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
        super.doPost(req, resp);
    }
}
