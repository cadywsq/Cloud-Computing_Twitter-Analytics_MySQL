package q4;

import utility.Utility;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Siqi Wang siqiw1 on 4/5/16.
 */
public class WriteQ4DAO {
    private static Logger logger = Logger.getLogger("Phase3_Q4");

    private static final String dns1 = "";
    private static final String dns2 = "";
    private static final String dns3 = "";
    private static final String dns4 = "";
    private static final String dns5 = "";
    private final Map<Integer, String> dcMap = new HashMap<Integer, String>() {{
        dcMap.put(1, dns1);
        dcMap.put(2, dns2);
        dcMap.put(3, dns3);
        dcMap.put(4, dns4);
        dcMap.put(5, dns5);
    }};

    private ConcurrentHashMap<String, Worker> workers = new ConcurrentHashMap<>();
    private ConcurrentLinkedQueue<Worker> idleWorkers = new ConcurrentLinkedQueue<>();
    private Utility.Cache<String, HashMap<String, String>> caches = new Utility.Cache();

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

}
