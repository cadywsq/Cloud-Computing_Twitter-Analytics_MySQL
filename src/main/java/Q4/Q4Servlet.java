package q4;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static utility.Utility.formatResponse;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class Q4Servlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String tweetId = req.getParameter("tweetid");
        // set or get
        String operation = req.getParameter("op");
        String sequence = req.getParameter("seq");
        //comma separated list of fields
        String fields = req.getParameter("fields");
        //comma separated list of base64* encoded fields
        String payload = req.getParameter("payload");

        StringBuilder result = new StringBuilder();
        Query4DAO dao = new Query4DAO();
        result.append(formatResponse());
        if (operation.equals("get")) {
            result.append(dao.processGet(tweetId, fields));
        } else {
            result.append(dao.processSet(tweetId, fields, payload));
        }
        PrintWriter writer = resp.getWriter();
        writer.write(result.toString());
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }


}
