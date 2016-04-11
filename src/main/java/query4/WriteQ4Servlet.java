package query4;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
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

    /**
     * Worker thread to process requests for same key, as it should be serialized.
     */
    private class Worker extends Thread implements Comparable<Worker> {
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

        public void addTask(Worker task) {
            tasks.offer(task);
        }

        @Override
        public int compareTo(Worker o) {
            return this.seq - o.seq;
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        // set or get
        final String operation = req.getParameter("op");
        String sequence = req.getParameter("seq");
        int seq = Integer.valueOf(sequence);
        //comma separated list of fields
        final String fields = req.getParameter("fields");
        //comma separated list of base64* encoded fields
        final String payload = req.getParameter("payload");

        final Q4WriteUtil dbDao = new Q4WriteUtil();
        final Q4CacheUtil cacheDao = new Q4CacheUtil();
        getWorker(tweetId).addTask(new Worker(seq) {
            @Override
            public void run() {
                StringBuilder result = new StringBuilder();
                result.append(formatResponse());
                if (operation.equals("set")) {
                    result.append("success\n");
                    sendResponse(result);
                    dbDao.putData(dbDao.getQuery(tweetId, fields, payload));
                    cacheDao.processSetCache(tweetId, fields, payload);
                } else {
                    String cached = cacheDao.processGetCache(tweetId, fields);
                    String response;
                    if (!cached.equals("")) {
                        response = cached;
                    } else {
                        response = dbDao.getData(tweetId, fields);
                    }
                    result.append(response + "\n");
                    sendResponse(result);
                }

            }

            private void sendResponse(StringBuilder result) {
                PrintWriter writer;
                try {
                    writer = resp.getWriter();
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
