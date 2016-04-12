//package query4;
//
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//import static util.Utility.formatResponse;
//
///**
// * @author Siqi Wang siqiw1 on 4/5/16.
// */
//public class Q4ServletPre extends HttpServlet {
//    private static Logger logger = Logger.getLogger("Phase3_Q4");
//
//    private ConcurrentHashMap<String, Worker> workers = new ConcurrentHashMap<>();
//    private ConcurrentLinkedQueue<Worker> idleWorkers = new ConcurrentLinkedQueue<>();
//    private ConcurrentHashMap<String, RequestQueue> requestsMap = new ConcurrentHashMap<>();
//
//    /**
//     * Worker thread to process requests for same key, as it should be serialized.
//     */
//    private class Worker extends Thread {
//        private String key = null;
//        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
//
//        public Worker() {
//            this.start();
//        }
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    tasks.take().run();
//                } catch (Exception e) {
//                    logger.log(Level.WARNING, "Exception is thrown for " + e);
//                }
//                // If requests for one key is done, move the thread from workers to idleWorkers.
//                synchronized (this) {
//                    if (tasks.isEmpty()) {
//                        assert key != null;
//                        workers.remove(key);
//                        idleWorkers.offer(this);
//                        key = null;
//                    }
//                }
//            }
//        }
//
//        public void addTask(Runnable task) {
//            tasks.offer(task);
//        }
//    }
//
//    @Override
//    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
//        final String tweetId = req.getParameter("tweetid");
//        System.out.println("tweetid\t"+ tweetId);
//        final String sequence = req.getParameter("seq");
//        final int seq = Integer.valueOf(sequence);
//        System.out.println("seq\t"+seq);
//
//        getWorker(tweetId).addTask(new Runnable() {
//            @Override
//            public void run() {
//                StringBuilder result = formatResponse();
//                final String op = req.getParameter("op");
//                System.out.println(op);
//                // For set request, return response directly
//                if (op.equals("set")) {
//                    result.append("success\n");
//                    sendResponse(result, resp);
//                    System.out.println("Set request response sent.");
//                }
//
//                RequestQueue requestQueue;
//                List<RequestQueue.Request> requestsToProcess;
//                synchronized (this) {
//                    if (requestsMap.containsKey(tweetId)) {
//                        requestQueue = requestsMap.get(tweetId);
//                        System.out.println("request queue got.");
//                    } else {
//                        requestQueue = new RequestQueue();
//                        requestsMap.put(tweetId, requestQueue);
//                        System.out.println("create new request queue.");
//                    }
//                    requestQueue.addRequest(new RequestQueue.Request(seq, req, resp));
//                    System.out.println("new request added");
//                    requestsToProcess = Q4WriteHelper.mergeRequests(requestQueue.getRequests());
//                    System.out.println("merged requestqueue got.");
//                    System.out.println("requestToProcess size: " + requestsToProcess.size());
//                    if (requestQueue.isEmpty()) {
//                        requestsMap.remove(tweetId);
//                    }
//                }
//
//                if (!requestsToProcess.isEmpty()) {
//                    for (RequestQueue.Request request : requestsToProcess) {
//                        HttpServletRequest httpServletReq = request.getRequest();
//                        HttpServletResponse httpServletResp = request.getResponse();
//                        String operation = httpServletReq.getParameter("op");
//                        String fields = httpServletReq.getParameter("fields");
//                        String payload = httpServletReq.getParameter("payload");
//                        System.out.println("payload\t" + payload);
//
//                        if (operation.equals("set")) {
//                            Q4WriteHelper.putData(Q4WriteHelper.getQuery(tweetId, fields, payload));
//                            System.out.println("Data put to DB completed.");
//                            Q4CacheUtil.setCache(tweetId, fields, payload);
//                            System.out.println("Data cached.");
//                        } else {
//                            String cached = Q4CacheUtil.getCache(tweetId, fields);
//                            String response;
//                            if (!cached.equals("")) {
//                                response = cached;
//                                System.out.println("Get field found in cache.");
//                            } else {
//                                response = Q4WriteHelper.getData(tweetId, fields);
//                                System.out.println("Get from DB completed.");
//                            }
//                            result.append(response + "\n");
//                            sendResponse(result, httpServletResp);
//                            System.out.println("Get request response sent.");
//                        }
//                    }
//                }
//            }
//
//            private void sendResponse(StringBuilder result, HttpServletResponse response) {
//                PrintWriter writer;
//                try {
//                    writer = response.getWriter();
//                    writer.println(result.toString());
//                    System.out.println(result.toString());
//                    writer.close();
//                } catch (IOException e) {
//                    logger.log(Level.WARNING, "Failed when get writer for response");
//                }
//            }
//        });
//    }
//
//    @Override
//    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//        doGet(req, resp);
//    }
//
//    /**
//     * When receiving new request, get the thread for specific key from Workers map, if not exists, create new thread.
//     *
//     * @param key
//     * @return worker thread for the key
//     */
//    private synchronized Worker getWorker(String key) {
//        Worker worker = workers.get(key);
//        if (worker == null) {
//            worker = idleWorkers.poll();
//            assert worker.key == null;
//            if (worker == null) {
//                worker = new Worker(); // Worker's constructor should call start()
//            }
//            worker.key = key;
//            workers.put(key, worker);
//        }
//        return worker;
//    }
//}
