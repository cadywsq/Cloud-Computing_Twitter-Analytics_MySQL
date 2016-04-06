package query4;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String tweetId = req.getParameter("tweetid");
        // set or get
        final String operation = req.getParameter("op");
        String sequence = req.getParameter("seq");
        //comma separated list of fields
        final String fields = req.getParameter("fields");
        //comma separated list of base64* encoded fields
        final String payload = req.getParameter("payload");

        final WriteQ4DAO dao = new WriteQ4DAO();
        getWorker(tweetId).addTask(new Runnable() {
            @Override
            public void run() {
                if (operation.equals("get")) {
                    dao.putData(dao.getQuery(tweetId, fields, payload));
                } else {
                    dao.getData(tweetId, fields);
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
