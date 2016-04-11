package query4;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

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
            this.setRequest(request);
            this.setResponse(response);
        }

        @Override
        public int compareTo(Request o) {
            return Integer.compare(this.seq, o.seq);
        }

        public HttpServletRequest getRequest() {
            return request;
        }

        public void setRequest(HttpServletRequest request) {
            this.request = request;
        }

        public HttpServletResponse getResponse() {
            return response;
        }

        public void setResponse(HttpServletResponse response) {
            this.response = response;
        }
    }

    private Queue<Request> requestQueue = new PriorityQueue<>();
    private int lastProcessedSeq;


    /**
     * Get the current continuous requests that can be processed.
     * If current request queue peek() is not last processed seq + 1, return empty queue.
     *
     * @return queue of requests to be processed
     */
    public List<Request> getRequests() {
        List<Request> requestsToProcess = new ArrayList<>();
        Queue<Request> requests = new PriorityQueue<>();

        while (!requestQueue.isEmpty() && lastProcessedSeq + 1 == requests.peek().seq) {
            lastProcessedSeq = requests.peek().seq;
            requestsToProcess.add(requests.poll());
        }
        return requestsToProcess;
    }

    /**
     * Add received request to the request queue.
     *
     * @param request
     */
    public void addRequest(Request request) {
        requestQueue.offer(request);
    }

    public boolean isEmpty() {
        return requestQueue.isEmpty();
    }
}
