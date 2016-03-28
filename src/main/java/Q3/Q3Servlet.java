package Q3;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Q3Servlet extends HttpServlet {
    private static final String TEAM_ID = "SilverLining";
    private static final String TEAM_AWS_ACCOUNT = "6408-5853-5216";
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	System.out.println("ssss");
    	int startDate =Integer.parseInt(req.getParameter("start_date")), endDate = Integer.parseInt(req.getParameter("end_date"));
    	long startUid = Long.parseLong(req.getParameter("start_userid")), endUid = Long.parseLong(req.getParameter("end_userid"));
    	String[] target = req.getParameter("words").split(",");
    	System.out.println("target1: " + target[0] + " target2: " + target[1] + "target3:" + target[2]);
        StringBuilder builder = new StringBuilder();
        builder.append(TEAM_ID + "," + TEAM_AWS_ACCOUNT + "\n");
        Query3DAO dao = new Query3DAO();
        List<StringBuilder> wordCount = dao.getWordCount(startDate, endDate, startUid, endUid);
        for (String word : target) {
        	int count = 0;
        	for (StringBuilder sb : wordCount) {
        		int index = sb.indexOf(word);
        		int innerCount = 0;
        		if (index != -1) {
        			for (int i = index + word.length() + 1; i < sb.length() && sb.charAt(i) != ','; i++) {
        				innerCount *= 10;
        				innerCount += (int) sb.charAt(i) - 48;
        			}
        		}
        		count += innerCount;
        	}
    		builder.append(word + ":" + count);
    		builder.append("\n");
        }
        resp.setContentType("text/plain;charset=UTF-8");
        PrintWriter writer = resp.getWriter();
        writer.write(builder.toString());
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doGet(req, resp);
    }

}
