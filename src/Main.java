import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * RideSharingSimulation.java
 *
 * Demonstrates:
 *  - Shared task queue (BlockingQueue)
 *  - Worker threads via ExecutorService
 *  - Safe termination using "poison pill" tasks
 *  - Exception handling (InterruptedException, IOExceptions)
 *  - Logging of thread lifecycle and errors
 *  - Aggregation of results and writing them to an output file
 */
public class Main {

    // Simple POJO for tasks. A "poison pill" is represented by isPoison == true
    static class Task {
        final int id;
        final String passenger;
        final String pickup;
        final String destination;
        final boolean isPoison;

        Task(int id, String passenger, String pickup, String destination) {
            this.id = id;
            this.passenger = passenger;
            this.pickup = pickup;
            this.destination = destination;
            this.isPoison = false;
        }

        // Constructor for poison pill
        Task(boolean isPoison) {
            this.id = -1;
            this.passenger = null;
            this.pickup = null;
            this.destination = null;
            this.isPoison = isPoison;
        }

        // Simulate processing work (could throw InterruptedException)
        RideResult process() throws InterruptedException {
            // Simulate variable processing time 300-1200ms
            long durationMs = 300 + (long) (Math.random() * 900);
            Thread.sleep(durationMs);

            // Example "result"
            String driver = "Driver-" + (1 + (int) (Math.random() * 20));
            return new RideResult(id, passenger, pickup, destination, driver, durationMs, LocalDateTime.now());
        }
    }

    // Class to represent processing result
    static class RideResult {
        final int taskId;
        final String passenger;
        final String pickup;
        final String destination;
        final String driverAssigned;
        final long processingMs;
        final LocalDateTime completedAt;

        RideResult(int taskId, String passenger, String pickup, String destination,
                   String driverAssigned, long processingMs, LocalDateTime completedAt) {
            this.taskId = taskId;
            this.passenger = passenger;
            this.pickup = pickup;
            this.destination = destination;
            this.driverAssigned = driverAssigned;
            this.processingMs = processingMs;
            this.completedAt = completedAt;
        }

        @Override
        public String toString() {
            return String.format("Task[%d] passenger=%s pickup=%s dest=%s driver=%s time=%dms at=%s",
                    taskId, passenger, pickup, destination, driverAssigned, processingMs, completedAt);
        }
    }

    // Worker runnable
    static class Worker implements Runnable {
        private final BlockingQueue<Task> queue;
        private final List<RideResult> results;
        private final CountDownLatch doneSignal;
        private final Logger logger;
        private final int workerId;

        Worker(int workerId, BlockingQueue<Task> queue, List<RideResult> results,
               CountDownLatch doneSignal, Logger logger) {
            this.workerId = workerId;
            this.queue = queue;
            this.results = results;
            this.doneSignal = doneSignal;
            this.logger = logger;
        }

        @Override
        public void run() {
            String threadName = "Worker-" + workerId;
            logger.info(threadName + " started.");
            try {
                while (true) {
                    Task task = queue.take(); // blocking
                    if (task.isPoison) {
                        logger.info(threadName + " received poison pill and will exit.");
                        break;
                    }

                    try {
                        logger.info(threadName + " started processing task " + task.id + " (passenger: " + task.passenger + ")");
                        RideResult res = task.process();
                        // Add result to shared results list (synchronized list wrapper used in main)
                        results.add(res);
                        logger.info(threadName + " completed task " + task.id + " -> " + res.driverAssigned);
                    } catch (InterruptedException ie) {
                        // Restore interrupted status
                        Thread.currentThread().interrupt();
                        logger.warning(threadName + " interrupted during task processing: " + ie.getMessage());
                        break;
                    } catch (Exception ex) {
                        logger.log(Level.SEVERE, threadName + " encountered error while processing task " + task.id + ": " + ex.getMessage(), ex);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warning(threadName + " interrupted while waiting on queue: " + ie.getMessage());
            } finally {
                logger.info(threadName + " terminating.");
                doneSignal.countDown();
            }
        }
    }

    // Configure a logger writing to console and a file
    private static Logger configureLogger(String logFilePath) {
        Logger logger = Logger.getLogger("RideSharingSimulation");
        logger.setUseParentHandlers(false); // we will add handlers explicitly
        logger.setLevel(Level.INFO);

        // Remove any existing handlers
        Handler[] existing = logger.getHandlers();
        for (Handler h : existing) {
            logger.removeHandler(h);
        }

        try {
            // Console handler
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            consoleHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(consoleHandler);

            // File handler (append)
            FileHandler fh = new FileHandler(logFilePath, true);
            fh.setLevel(Level.INFO);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException ioe) {
            // If file handler setup fails, still keep console logging
            System.err.println("WARNING: could not create file handler for logger: " + ioe.getMessage());
        }

        return logger;
    }

    public static void main(String[] args) {
        final int NUM_WORKERS = 4;
        final int NUM_TASKS = 25;
        final String RESULTS_OUT = "ride_results.txt";
        final String LOG_FILE = "ridesharing.log";

        Logger logger = configureLogger(LOG_FILE);
        logger.info("=== RideSharingSimulation starting ===");

        // Shared queue and results
        BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
        List<RideResult> results = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch workersDone = new CountDownLatch(NUM_WORKERS);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);

        // Start workers
        for (int i = 1; i <= NUM_WORKERS; i++) {
            executor.submit(new Worker(i, queue, results, workersDone, logger));
        }

        // Enqueue tasks (producer)
        for (int i = 1; i <= NUM_TASKS; i++) {
            Task t = new Task(i, "Passenger-" + i, "Pickup-" + ((i % 10) + 1), "Dest-" + ((i % 7) + 1));
            try {
                queue.put(t); // blocking if queue is full; safe
                logger.info("Main enqueued task " + t.id + " for " + t.passenger);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warning("Main thread interrupted while enqueueing tasks: " + ie.getMessage());
                break;
            }
        }

        // Send poison pills (one per worker) so each worker will exit when done
        for (int i = 0; i < NUM_WORKERS; i++) {
            try {
                queue.put(new Task(true)); // poison pill
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warning("Main interrupted while enqueueing poison pills: " + ie.getMessage());
            }
        }

        // Shutdown executor: workers will exit after processing poison pills
        executor.shutdown();

        // Wait for workers to finish, but guard with timeout to avoid indefinite blocking
        try {
            boolean finished = workersDone.await(60, TimeUnit.SECONDS);
            if (!finished) {
                logger.warning("Not all workers finished within timeout. Forcing shutdown.");
                executor.shutdownNow();
            } else {
                logger.info("All workers have finished processing.");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.warning("Main thread interrupted while waiting for workers to finish: " + ie.getMessage());
            executor.shutdownNow();
        }

        // Write aggregated results to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULTS_OUT, false))) {
            writer.write("RideSharing Simulation Results\n");
            writer.write("Generated at: " + LocalDateTime.now() + "\n");
            writer.write("Total tasks processed: " + results.size() + "\n\n");
            // Copy snapshot to avoid ConcurrentModification (results is synchronized list)
            List<RideResult> snapshot;
            synchronized (results) {
                snapshot = new ArrayList<>(results);
            }
            // Sort results by taskId for nicer output
            snapshot.sort(Comparator.comparingInt(r -> r.taskId));
            for (RideResult r : snapshot) {
                writer.write(r.toString() + "\n");
            }
            logger.info("Successfully wrote results to " + RESULTS_OUT);
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "I/O error while writing results file: " + ioe.getMessage(), ioe);
        }

        logger.info("=== RideSharingSimulation finished ===");
    }
}
