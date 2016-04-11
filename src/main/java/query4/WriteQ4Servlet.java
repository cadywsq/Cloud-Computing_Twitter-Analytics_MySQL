package query4;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.Utility.formatResponse;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class WriteQ4Servlet extends HttpServlet {
    private static Logger logger = Logger.getLogger("Phase3_Q4");

    private ConcurrentHashMap<String, Worker> workers = new ConcurrentHashMap<>();
    private ConcurrentLinkedQueue<Worker> idleWorkers = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<String, RequestQueue> requestsMap = new ConcurrentHashMap<>();

    /**
     * Worker thread to process requests for same key, as it should be serialized.
     */
    private class Worker extends Thread {
        private String key = null;
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();

        public Worker() {
            this.start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    tasks.take().run();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Exception is thrown for " + e);
                }
                // If requests for one key is done, move the thread from workers to idleWorkers.
                synchronized (this) {
                    if (tasks.isEmpty()) {
                        assert key != null;
                        workers.remove(key);
                        idleWorkers.offer(this);
                        key = null;
                    }
                }
            }
        }

        public void addTask(Runnable task) {
            tasks.offer(task);
        }
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        final String sequence = req.getParameter("seq");
        final int seq = Integer.valueOf(sequence);

        final Q4WriteUtil dbUtil = new Q4WriteUtil();
        final Q4CacheUtil cacheUtil = new Q4CacheUtil();

        getWorker(tweetId).addTask(new Runnable() {
            @Override
            public void run() {
                StringBuilder result = new StringBuilder();

                RequestQueue requestQueue;
                if (requestsMap.containsKey(tweetId)) {
                    requestQueue = requestsMap.get(tweetId);
                } else {
                    requestQueue = new RequestQueue();
                    requestsMap.put(tweetId, requestQueue);
                }
                requestQueue.addRequest(new RequestQueue.Request(seq, req, resp));
                BlockingQueue<RequestQueue.Request> requestsToProcess = requestQueue.getRequests();
                if (requestQueue.isEmpty()) {
                    requestsMap.remove(tweetId);
                }

                if (!requestsToProcess.isEmpty()) {
                    for (RequestQueue.Request request : requestsToProcess) {
                        HttpServletRequest httpServletReq = request.getRequest();
                        HttpServletResponse httpServletResp = request.getResponse();
                        String operation = httpServletReq.getParameter("op");
                        String fields = httpServletReq.getParameter("fields");
                        String payload = httpServletReq.getParameter("payload");

                        result.append(formatResponse());
                        if (operation.equals("set")) {
                            result.append("success\n");
                            sendResponse(result, httpServletResp);

                            dbUtil.putData(dbUtil.getQuery(tweetId, fields, payload));
                            cacheUtil.processSetCache(tweetId, fields, payload);
                        } else {
                            String cached = cacheUtil.processGetCache(tweetId, fields);
                            String response;
                            if (!cached.equals("")) {
                                response = cached;
                            } else {
                                response = dbUtil.getData(tweetId, fields);
                            }
                            result.append(response + "\n");
                            sendResponse(result, httpServletResp);
                        }
                    }
                }
            }

            private void sendResponse(StringBuilder result, HttpServletResponse response) {
                PrintWriter writer;
                try {
                    writer = response.getWriter();
                    writer.write(result.toString());
                    writer.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Failed when get writer for response");
                }
            }
        });
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    /**
     * When receiving new request, get the thread for specific key from Workers map, if not exists, create new thread.
     *
     * @param key
     * @return worker thread for the key
     */
    private synchronized Worker getWorker(String key) {
        Worker worker = workers.get(key);
        if (worker == null) {
            worker = idleWorkers.poll();
            assert worker.key == null;
            if (worker == null) {
                worker = new Worker(); // Worker's constructor should call start()
            }
            worker.key = key;
            workers.put(key, worker);
        }
        return worker;
    }
}
