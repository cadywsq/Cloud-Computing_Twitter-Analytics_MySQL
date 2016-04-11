package query4;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @author Siqi Wang siqiw1 on 4/10/16.
 */
public class RequestQueue {
    public static class Request implements Comparable<Request> {
        private int seq;
        private HttpServletRequest request;
        private HttpServletResponse response;

        public Request(int seq, HttpServletRequest request, HttpServletResponse response) {
            this.seq = seq;
            this.request = request;
            this.response = response;
        }

        @Override
        public int compareTo(Request o) {
            return Integer.compare(this.seq, o.seq);
        }
    }

    private BlockingQueue<Request> requestQueue = new PriorityBlockingQueue<>();

    /**
     * Get the current continuous requests that can be processed.
     * If current request queue peek() is not last processed seq + 1
     * @return
     */
    public Queue<Request> getRequests() {
        Queue<Request> requests = new PriorityBlockingQueue<>();


    }

    /**
     * Add received request to the request queue.
     * @param request
     */
    public void addRequest(Request request) {

    }
}
