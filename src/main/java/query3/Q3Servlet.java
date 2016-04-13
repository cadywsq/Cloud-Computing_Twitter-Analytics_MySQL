package query3;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

public class Q3Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    private final Query3DAO dao;

    public Q3Servlet() throws SQLException {
        dao = new Query3DAO();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        int startDate = Integer.parseInt(req.getParameter("start_date").replaceAll("-", "")), endDate = Integer.parseInt(req.getParameter("end_date").replaceAll("-", ""));
        long startUid = Long.parseLong(req.getParameter("start_userid")), endUid = Long.parseLong(req.getParameter("end_userid"));

        String[] target = req.getParameter("words").split(",");

        int[] res = dao.getWordCount(startDate, endDate, startUid, endUid, ";" + target[0] + ":", ";" + target[1] + ":", ";" + target[2] + ":");

        resp.setContentType("text/plain;charset=UTF-8");
        PrintWriter writer = resp.getWriter();
        writer.write(formatResponse(target, res));
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(req, resp);
    }

    private String formatResponse(String[] target, int[] res) {
        StringBuilder builder = new StringBuilder();
        builder.append(TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n");
        builder.append(target[0]).append(":").append(res[0]).append("\n");
        builder.append(target[1]).append(":").append(res[1]).append("\n");
        builder.append(target[2]).append(":").append(res[2]).append("\n");
        return builder.toString();
    }

}
