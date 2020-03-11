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

    private static final int SCALE_FACTOR = 100;

    private static final Random random = new Random(0);

    private static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static void main(String[] args) throws Exception {
        double[] skews = new double[] { 0, 0.1, 0.5, 0.99, 1.35 };
        for (double skew : skews) {
            varFillFactor(skew);
        }
        executor.shutdown();
    }

    private static void varFillFactor(double skew) throws IOException, InterruptedException, ExecutionException {
        double[] factors = new double[] { 0.5, 0.6, 0.7, 0.8, 0.9, 0.95 };

        Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);

        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
        Param[] params = new Param[] { new Param(gen, NoBlockSelector.INSTANCE, new Oldest(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new MaxAvail(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new Berkeley(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new MinDeclinePriorTs(), 1, 1, priorTsSorter) };

        Future[][] results = new Future[factors.length][params.length];

        for (int i = 0; i < factors.length; i++) {
            for (int j = 0; j < params.length; j++) {
                results[i][j] = run(params[j], skew, factors[i]);
            }
        }

        PrintWriter writer = new PrintWriter(new File("skew" + skew + ".csv"));

        writer.print("fill factor\t");
        for (Param param : params) {
            writer.append(param.scoreComputer.name() + "-E\t");
            writer.append(param.scoreComputer.name() + "-write cost\t");
            writer.append(param.scoreComputer.name() + "-GC cost\t");
        }
        writer.println();

        for (int i = 0; i < factors.length; i++) {
            writer.print(factors[i]);
            writer.print("\t");
            for (int j = 0; j < params.length; j++) {
                Future<Result> future = results[i][j];
                Result result = future.get();
                writer.print(result.E);
                writer.print("\t");
                writer.print(result.writeCost);
                writer.print("\t");
                writer.print(result.gcCost);
                writer.print("\t");
            }
            writer.println();
        }
        writer.close();

        System.out.println("Completed skew " + skew);
    }

    private static IntArrayList load(double fillFactor) {
        int numKeys = (int) (fillFactor * Simulator.TOTAL_PAGES);
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
                Simulator sim = new Simulator(param, lpids.size());
                long totalPages = (long) Simulator.TOTAL_PAGES * SCALE_FACTOR;
                sim.run(totalPages);
                return new Result(fillFactor, skewness, sim.formatWriteCost(), sim.formatGCCost(), sim.formatE());
            }
        });

    }

}
