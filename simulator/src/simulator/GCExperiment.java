package simulator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntLists;
import simulator.HotColdLpidGenerator.HotColdLpidGeneratorFactory;
import simulator.UniformLpidGenerator.UniformLpidGeneratorFactory;
import simulator.ZipfLpidGenerator.ZipfLpidGeneratorFactory;

class Result {
    final double fillFactor;
    final double skew;
    final String writeCost;
    final String gcCost;
    final String E;

    public Result(double fillFactor, double skew, String writeCost, String gcCost, String E) {
        this.fillFactor = fillFactor;
        this.skew = skew;
        this.gcCost = gcCost;
        this.E = E;
        this.writeCost = writeCost;
    }

}

public class GCExperiment {
    private static final int BATCH_BLOCKS = 64;

    private static final int SCALE_FACTOR = 100;

    private static final Random random = new Random(0);

    private static final int THREADS = 4;

    private static final double[] ZIPF_FACTORS = new double[] { 0.5, 0.6, 0.7, 0.8, 0.9, 0.95 };
    private static final double[] VLDB_FACTORS = new double[] { 1 / 1.1, 1 / 1.2, 1 / 1.3, 1 / 1.5, 1 / 1.75, 1 / 2.0 };

    private static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(THREADS, THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static void main(String[] args) throws Exception {
        runHotCold();
        executor.shutdown();
    }

    private static void runHotCold() throws IOException, InterruptedException, ExecutionException {
        int[] hotSkews = { 10, 20, 30, 40, 50 };
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);
        for (int skew : hotSkews) {
            LpidGeneratorFactory gen =
                    skew != 50 ? new HotColdLpidGeneratorFactory(skew) : new UniformLpidGeneratorFactory();
            Param[] params = new Param[] {
                    new Param("Greedy", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new MaxAvail(), null,
                            BATCH_BLOCKS, false),
                    new Param("Min-Decline-NoSort-Write-GC", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE,
                            new MinDecline(), null, BATCH_BLOCKS, false),
                    new Param("Min-Decline-NoSort-Write", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE,
                            new MinDecline(), priorTsSorter, BATCH_BLOCKS, false),
                    new Param("Min-Decline", gen, new SortWriteBuffer(BATCH_BLOCKS * GCSimulator.BLOCK_SIZE),
                            NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, BATCH_BLOCKS, false),
                    new Param("Min-Decline-OPT", gen, NoWriteBuffer.INSTANCE, new OptBlockSelector(),
                            new MinDeclineOpt(), null, BATCH_BLOCKS, false), };
            runExperiments("hotspot-" + skew + (100 - skew), new double[] { 0.8 }, params, skew);
        }
    }

    private static void runSynthetic() throws IOException, InterruptedException, ExecutionException {
        double[] skews = new double[] { 0, 0.5, 0.75, 0.99, 1.35 };
        for (double skew : skews) {
            varFillFactorMultiLog(ZIPF_FACTORS, skew);
        }
        varFillFactorMultiLog(VLDB_FACTORS, 1.0);
    }

    private static void test() throws IOException, InterruptedException, ExecutionException {
        double skew = 0.99;
        double[] fillFactors = { 0.8 };
        Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);
        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
        Param[] params = new Param[] { new Param("LRU", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE,
                new Oldest(), null, BATCH_BLOCKS, false),
                //                new Param("Greedy", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new MaxAvail(), null,
                //                        BATCH_BLOCKS, false),
                //                new Param("Berkeley", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new Berkeley(),
                //                        newestSorter, BATCH_BLOCKS, false),
                //                new Param("Min-Decline", gen, new SortWriteBuffer(BATCH_BLOCKS * Simulator.BLOCK_SIZE),
                //                        NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, BATCH_BLOCKS, false),
                //                new Param("Min-Decline-OPT", gen, NoWriteBuffer.INSTANCE, new OptBlockSelector(), new MinDeclineOpt(),
                //                        null, BATCH_BLOCKS, false),
                //                getMultiLogParam(gen, false), getMultiLogParam(gen, true)
        };
        runExperiments("trend-" + skew, fillFactors, params, skew);
    }

    private static Param getMultiLogParam(LpidGeneratorFactory gen, boolean oracleMode) {
        return new Param("Multi-Log" + (oracleMode ? "-Oracle" : ""), gen, NoWriteBuffer.INSTANCE,
                oracleMode ? new OptBlockSelector() : new MultiLogBlockSelector(), null, null, BATCH_BLOCKS, true);
    }

    private static Param getSortParam(LpidGeneratorFactory gen, int batchBlocks) {
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);
        return new Param("MinDecline", gen, new SortWriteBuffer(batchBlocks * GCSimulator.BLOCK_SIZE),
                NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, batchBlocks, false);
    }

    private static void varFillFactorMultiLog(double[] factors, double skew)
            throws IOException, InterruptedException, ExecutionException {
        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
        Param[] params = new Param[] { getMultiLogParam(gen, false), getMultiLogParam(gen, true) };
        runExperiments("multi-log-skew-" + skew, factors, params, skew);
    }

    private static void varFillFactor(double[] factors, double skew)
            throws IOException, InterruptedException, ExecutionException {

        Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);

        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
        Param[] params = new Param[] {
                new Param("LRU", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new Oldest(), null,
                        BATCH_BLOCKS, false),
                new Param("Greedy", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new MaxAvail(), null,
                        BATCH_BLOCKS, false),
                new Param("Berkeley", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new Berkeley(),
                        newestSorter, BATCH_BLOCKS, false),
                new Param("Min-Decline", gen, new SortWriteBuffer(BATCH_BLOCKS * GCSimulator.BLOCK_SIZE),
                        NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, BATCH_BLOCKS, false),
                new Param("Min-Decline-OPT", gen, NoWriteBuffer.INSTANCE, new OptBlockSelector(), new MinDeclineOpt(),
                        null, BATCH_BLOCKS, false) };
        runExperiments("skew-" + skew, factors, params, skew);
    }

    private static void varSortSize() throws IOException, InterruptedException, ExecutionException {
        double skew = 0.99;
        double[] factors = new double[] { 0.8 };
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);

        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
        Param[] params = new Param[7];

        params[0] = new Param("Min-Decline-" + 0, gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE,
                new MinDecline(), priorTsSorter, BATCH_BLOCKS, false);

        int blocks = 1;
        for (int i = 1; i < params.length; i++) {
            params[i] = new Param("Min-Decline-" + blocks, gen, new SortWriteBuffer(blocks * GCSimulator.BLOCK_SIZE),
                    NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, BATCH_BLOCKS, false);
            blocks *= 4;
        }
        runExperiments("var-sort-batch-size", factors, params, skew);

    }

    private static void runExperiments(String name, double[] factors, Param[] params, double skew)
            throws IOException, InterruptedException, ExecutionException {
        Future[][] results = new Future[factors.length][params.length];
        for (int i = 0; i < factors.length; i++) {
            for (int j = 0; j < params.length; j++) {
                results[i][j] = run(params[j], skew, factors[i]);
            }
        }
        PrintWriter writer = new PrintWriter(new File(name + ".csv"));
        writer.print("fill factor\t");
        for (Param param : params) {
            writer.append(param.name + "-E\t");
            writer.append(param.name + "-write cost\t");
            writer.append(param.name + "-GC cost\t");
        }
        writer.println();

        for (int i = 0; i < factors.length; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(factors[i]);
            sb.append("\t");
            for (int j = 0; j < params.length; j++) {
                Future<Result> future = results[i][j];
                Result result = future.get();
                sb.append(result.E);
                sb.append("\t");
                sb.append(result.writeCost);
                sb.append("\t");
                sb.append(result.gcCost);
                sb.append("\t");
            }
            writer.println(sb.toString());
            writer.flush();
            System.out.println(sb.toString());
            System.out.flush();
        }
        writer.close();

        System.out.println("Completed skew " + skew);
    }

    private static IntArrayList load(double fillFactor) {
        int numKeys = (int) (fillFactor * GCSimulator.TOTAL_PAGES);
        IntArrayList lpids = new IntArrayList(numKeys);
        for (int i = 0; i < numKeys; i++) {
            lpids.add(i + 1);
        }
        IntLists.shuffle(lpids, random);

        return lpids;
    }

    public static Future<Result> run(Param param, double skewness, double fillFactor) throws IOException {
        return executor.submit(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                IntArrayList lpids = load(fillFactor);
                GCSimulator sim = param.multiLog ? new MultiLogSimulator(param, lpids.size())
                        : new GCSimulator(param, lpids.size());
                sim.load(lpids.toIntArray());
                long totalPages = (long) GCSimulator.TOTAL_PAGES * SCALE_FACTOR;
                sim.run(totalPages);
                if (sim.blockSelector instanceof MultiLogBlockSelector) {
                    MultiLogBlockSelector selector = (MultiLogBlockSelector) sim.blockSelector;
                    System.out.println(String.format("user intervals: %d, user lpids: %d, intended: %d, promoted: %d",
                            selector.intervals.size(), selector.userTotal, selector.userIntended,
                            selector.userPromoted));
                    System.out.println(String.format("gc intervals: %d, gc lpids: %d, deomoted: %d",
                            selector.intervals.size(), selector.gcTotal, selector.gcDemoted));
                }
                return new Result(fillFactor, skewness, sim.formatWriteCost(), sim.formatGCCost(), sim.formatE());
            }
        });

    }

}
