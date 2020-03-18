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

import simulator.ZipfLpidGenerator.ZipfLpidGeneratorFactory;

public class TraceExperiment {

    private static final String basePath = "/home/luochen/experiment/write";
    //
    private static final int[] scaleFactors = new int[] { 500, 600, 700, 800, 900 };
    private static final double[] stopThresholds = new double[] { 0.6, 0.7, 0.8, 0.9, 0.95 };

    //private static final int[] scaleFactors = new int[] { 500 };
    //private static final double[] stopThresholds = new double[] { 0.6 };

    private static final Random random = new Random(0);

    private static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static void main(String[] args) throws Exception {
        runTrace();
        executor.shutdown();
    }

    private static void runTrace() throws IOException, InterruptedException, ExecutionException {
        Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);

        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(0.0);
        Param[] params = new Param[] { new Param(gen, NoBlockSelector.INSTANCE, new Oldest(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new MaxAvail(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new Berkeley(), 1, 1, newestSorter),
                new Param(gen, NoBlockSelector.INSTANCE, new MinDeclinePriorTs(), 1, 1, priorTsSorter) };

        Future[][] results = new Future[scaleFactors.length][params.length];

        for (int i = 0; i < scaleFactors.length; i++) {
            for (int j = 0; j < params.length; j++) {
                results[i][j] = run(params[j], scaleFactors[i], stopThresholds[i]);
            }
        }

        PrintWriter writer = new PrintWriter(new File("tpcc-lsm.csv"));

        writer.print("fill factor\t");
        for (Param param : params) {
            writer.append(param.scoreComputer.name() + "-E\t");
            writer.append(param.scoreComputer.name() + "-write cost\t");
            writer.append(param.scoreComputer.name() + "-GC cost\t");
        }
        writer.println();

        for (int i = 0; i < scaleFactors.length; i++) {
            writer.print(scaleFactors[i]);
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
    }

    public static Future<Result> run(Param param, int scaleFactor, double stopThreshold) throws IOException {
        return executor.submit(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                int stopPages = (int) (Simulator.TOTAL_PAGES * stopThreshold);
                Simulator sim = new Simulator(param, Simulator.TOTAL_PAGES);

                FileMapper mapper = new FileMapper(Simulator.TOTAL_PAGES);
                TraceReader loadReader = new TraceReader(basePath + "load-" + scaleFactor + ".trace");
                applyTrace(loadReader, mapper, sim, Simulator.TOTAL_PAGES / 10, Simulator.TOTAL_PAGES);

                System.out.println(String.format("Scale factor %d completed loading. Current pages %.3f: %d/%d",
                        scaleFactor, (double) mapper.getUsedLpids() / Simulator.TOTAL_PAGES, mapper.getUsedLpids(),
                        Simulator.TOTAL_PAGES));
                sim.resetStats();

                TraceReader runReader = new TraceReader(basePath + "run-" + scaleFactor + ".trace");
                applyTrace(runReader, mapper, sim, Simulator.TOTAL_PAGES / 10, stopPages);

                System.out.println(String.format("Scale factor %d completed running. Current pages %.3f: %d/%d",
                        scaleFactor, (double) mapper.getUsedLpids() / Simulator.TOTAL_PAGES, mapper.getUsedLpids(),
                        Simulator.TOTAL_PAGES));

                return new Result(scaleFactor, 0, sim.formatWriteCost(), sim.formatGCCost(), sim.formatE());
            }
        });
    }

    private static void applyTrace(TraceReader reader, FileMapper mapper, Simulator sim, int progress, int stopPages)
            throws IOException {
        TraceOperation op = new TraceOperation();
        int i = 0;
        while (reader.read(op)) {
            if (op.op == TraceReader.WRITE) {
                sim.write(mapper.write(op.file, op.page));
            } else if (op.op == TraceReader.DELETE) {
                mapper.delete(op.file);
            } else {
                throw new IllegalStateException("Unknown operation " + op.op);
            }
            if (++i % progress == 0) {
                System.out.println(String.format("Completed %d operations. Current pages %d/%d ", i,
                        mapper.getUsedLpids(), Simulator.TOTAL_PAGES));
            }
            if (mapper.getUsedLpids() >= stopPages) {
                reader.close();
                return;
            }
        }
    }

}
