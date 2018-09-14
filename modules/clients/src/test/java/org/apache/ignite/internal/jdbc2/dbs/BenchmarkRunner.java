package org.apache.ignite.internal.jdbc2.dbs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class BenchmarkRunner {
    /** Number of load threads. */
    private static final int LOAD_THREADS = 100;

    /** Load duration. */
    private static final long LOAD_DUR = 300_000L;

    /** Warmup duration. */
    private static final long LOAD_WARMUP_DUR = 10_000L;

    /** Number of load counters. */
    private static final int LOAD_CTR_CNT = 100;

    /** */
    private static final List<String> accIds = new ArrayList<>();

    /** Load counters. */
    private static volatile LongAdder[] loadCtrs;

    /** Warmup flag. */
    private static volatile boolean warmup = true;

    /** Stop flag. */
    private static volatile boolean stop;

    /** */
    private static final String[] connectionStrings = System.getProperty("JDBC_CONN_STRING").split(";");

    static {
        loadCtrs = new LongAdder[LOAD_CTR_CNT];

        for (int i = 0; i < LOAD_CTR_CNT; i++)
            loadCtrs[i] = new LongAdder();
    }

    /**
     * Entry point.
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionStrings[0])) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ACCT_ID FROM TRAN_HISTORY")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString(1);

                        accIds.add(id);
                    }
                }
            }
        }

        runLoad(LOAD_THREADS, LOAD_WARMUP_DUR, LOAD_DUR,
            connectionStrings
        );
    }

    /**
     * Run load.
     *
     * @param threads Threads.
     * @param warmupDur Warmup duration.
     * @param dur Duration.
     * @param connStrs Connection strings.
     * @throws Exception If failed.
     */
    private static void runLoad(int threads, long warmupDur, long dur, String... connStrs) throws Exception {
        ThreadLocalRunner[] runners = new ThreadLocalRunner[threads];

        for (int i = 0; i < threads; i++) {
            ThreadLocalRunner runner = new ThreadLocalRunner(connStrs);

            Thread thread = new Thread(runner);

            thread.setName("load-runner-" + i);

            thread.start();

            runners[i] = runner;
        }

        Thread.sleep(warmupDur);

        warmup = false;

        System.out.println(">>> WARMUP finished.");

        Thread printThread = new Thread(new Runnable() {
            @Override public void run() {
                while (!stop) {
                    try {
                        Thread.sleep(5000L);

                        printResults();

                        LongAdder[] newCtrs = new LongAdder[LOAD_CTR_CNT];

                        for (int i = 0; i < LOAD_CTR_CNT; i++)
                            newCtrs[i] = new LongAdder();

                        loadCtrs = newCtrs;
                    }
                    catch (Exception e) {
                        return;
                    }
                }
            }
        });

        printThread.setName("load-result-printer");
        printThread.start();

        Thread.sleep(dur);

        stop = true;

        for (ThreadLocalRunner runner : runners)
            runner.stop();

        for (ThreadLocalRunner runner : runners)
            runner.awaitStop();

        System.out.println(">>> LOAD finished.");
    }

    /**
     * Print current results.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private static void printResults() {
        StringBuilder sb = new StringBuilder(">>> RESULTS [");

        boolean first = true;

        LongAdder[] ctrs = loadCtrs;

        for (int i = 0; i < LOAD_CTR_CNT; i++) {
            long val = ctrs[i].longValue();

            if (val != 0L) {
                if (first)
                    first = false;
                else
                    sb.append(", ");

                sb.append(((i + 1) * 50) + "=" + val);
            }
        }

        sb.append("]");

        System.out.println(sb.toString());
    }

    /**
     * Log query duration.
     *
     * @param dur Duration.
     */
    private static void logQueryDuration(long dur) {
        if (warmup)
            return;

        if (dur < 0) {
            loadCtrs[0].increment();

            return;
        }

        int dur0 = (int)dur;

        if ((long)dur0 == dur) {
            int slot = dur0 / 50;

            if (slot < LOAD_CTR_CNT)
                loadCtrs[slot].increment();

            return;
        }

        loadCtrs[LOAD_CTR_CNT - 1].increment();
    }

    /**
     * Query Ignite through the given connection.
     *
     * @param conn Connection.
     * @return Duration in milliseconds.
     * @throws Exception if failed.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private static long query(Connection conn) throws Exception {
        long start = System.currentTimeMillis();

        String acctId = accIds.get(ThreadLocalRandom.current().nextInt(accIds.size()));

        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM TRAN_HISTORY WHERE ACCT_ID='" +
            acctId + "' ORDER BY POSTING_DATE LIMIT 50 OFFSET 0")) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // No-op.
                }
            }
        }

        return System.currentTimeMillis() - start;
    }

    /**
     * Thread-local runner.
     */
    private static class ThreadLocalRunner implements Runnable {
        /** Connection strings. */
        private final String[] connStrs;

        /** Connections. */
        private ArrayList<Connection> conns;

        /** Stop latch. */
        private final CountDownLatch stopLatch = new CountDownLatch(1);

        /** Stop flag. */
        private volatile boolean stopped;

        /**
         * Constructor.
         *
         * @param connStrs Connection strings.
         */
        private ThreadLocalRunner(String... connStrs) {
            this.connStrs = connStrs;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                conns = new ArrayList<>(connStrs.length);

                for (String connStr : connStrs) {
                    Connection conn = DriverManager.getConnection(connStr);

                    conns.add(conn);
                }

                while (!stopped)
                    runSingle();
            }
            catch (Exception e) {
                throw new RuntimeException("Thread local runner failed.", e);
            }
            finally {
                try {
                    for (Connection conn : conns)
                        U.closeQuiet(conn);
                }
                finally {
                    stopLatch.countDown();
                }
            }
        }

        /**
         * Run single iteration.
         *
         * @throws Exception If failed.
         */
        private void runSingle() throws Exception {
            Connection conn = conns.get(ThreadLocalRandom.current().nextInt(conns.size()));

            long dur = query(conn);

            logQueryDuration(dur);
        }

        /**
         * Stop runner.
         */
        private void stop() {
            stopped = true;
        }

        /**
         * Await runner stop.
         */
        private void awaitStop() {
            try {
                stopLatch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException("Interrupted while waiting on thread local worker stop.", e);
            }
        }
    }
}
