package simulator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import simulator.TPCCLpidGenerator.TPCCLpidGeneratorFactory;
import simulator.ZipfLpidGenerator.ZipfLpidGeneratorFactory;

public class TraceExperiment {
    //

    private static final Logger LOGGER = LogManager.getLogger(TraceExperiment.class);

    private static final int BATCH_BLOCKS = 64;
    private static final String basePath = "/home/luochen/experiment/memory/";
    private static final int[] scaleFactors = new int[] { 350, 420, 490, 560 };
    private static final double[] stopThresholds = new double[] { 0.6, 0.7, 0.8, 0.9 };
    //
    //    private static final String basePath = "/Users/luochen/Desktop/trace/";
    //    private static final int[] scaleFactors = new int[] { 560 };
    //    private static final double[] stopThresholds = new double[] { 0.9 };

    private static final int THREADS = 4;

    private static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(THREADS, THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public static void main(String[] args) throws Exception {
        runTrace();
        executor.shutdown();
    }

    private static void runTrace() throws IOException, InterruptedException, ExecutionException {
        Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
        Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);

        LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(0.0);
        Param[] params = new Param[] {
                //                new Param("LRU", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new Oldest(), null,
                //                        BATCH_BLOCKS, false),
                //                new Param("Greedy", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new MaxAvail(), null,
                //                        BATCH_BLOCKS, false),
                //                new Param("Berkeley", gen, NoWriteBuffer.INSTANCE, NoBlockSelector.INSTANCE, new Berkeley(),
                //                        newestSorter, BATCH_BLOCKS, false),
                //                new Param("MultiLog", gen, NoWriteBuffer.INSTANCE, new MultiLogBlockSelector(), null, null, 1, true),
                new Param("MultiLog-OPT", new TPCCLpidGeneratorFactory(), NoWriteBuffer.INSTANCE,
                        new OptBlockSelector(), null, null, 1, true),
                //                new Param("Min-Decline", gen, new SortWriteBuffer(BATCH_BLOCKS * Simulator.BLOCK_SIZE),
                //                        NoBlockSelector.INSTANCE, new MinDecline(), priorTsSorter, BATCH_BLOCKS, false),
                new Param("Min-Decline-OPT", new TPCCLpidGeneratorFactory(), NoWriteBuffer.INSTANCE,
                        new OptBlockSelector(), new MinDeclineOpt(), priorTsSorter, BATCH_BLOCKS, false), };

        Future[][] results = new Future[scaleFactors.length][params.length];

        for (int i = 0; i < scaleFactors.length; i++) {
            for (int j = 0; j < params.length; j++) {
                results[i][j] = run(params[j], scaleFactors[i], stopThresholds[i]);
            }
        }

        PrintWriter writer = new PrintWriter(new File("tpcc-btree.csv"));
        writer.print("fill factor\t");
        for (Param param : params) {
            writer.append(param.name + "-E\t");
            writer.append(param.name + "-write cost\t");
            writer.append(param.name + "-GC cost\t");
        }
        writer.println();

        for (int i = 0; i < scaleFactors.length; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(scaleFactors[i]);
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
            System.out.println(sb.toString());
            writer.println(sb.toString());
            writer.flush();
        }
        writer.close();
    }

    public static Future<Result> run(Param param, int scaleFactor, double stopThreshold) throws IOException {
        return executor.submit(new Callable<Result>() {
            @Override
            public Result call() throws Exception {
                int stopPages = (int) (Simulator.TOTAL_PAGES * stopThreshold);
                Simulator sim = param.multiLog ? new MultiLogSimulator(param, Simulator.TOTAL_PAGES)
                        : new Simulator(param, Simulator.TOTAL_PAGES);
                FileMapper mapper = new FileMapper(Simulator.TOTAL_PAGES, sim);

                if (sim.gen instanceof TPCCLpidGenerator) {
                    trainGenerator((TPCCLpidGenerator) sim.gen, mapper, scaleFactor);
                }
                sim.blockSelector.init(sim);

                TraceReader loadReader = new TraceReader(basePath + "load-" + scaleFactor + ".trace");
                applyTrace("load", loadReader, mapper, sim, Simulator.TOTAL_PAGES / 10, Integer.MAX_VALUE);

                System.out.println(String.format("Scale factor %d completed loading. Current pages %.3f: %d/%d",
                        scaleFactor, (double) mapper.getUsedLpids() / Simulator.TOTAL_PAGES, mapper.getUsedLpids(),
                        Simulator.TOTAL_PAGES));
                sim.resetStats();

                TraceReader runReader = new TraceReader(basePath + "run-" + scaleFactor + ".trace");
                applyTrace("run", runReader, mapper, sim, Simulator.TOTAL_PAGES / 10, stopPages);

                sim.writeBuffer.flush(sim);
                System.out.println(String.format("Scale factor %d completed running. Current pages %.3f: %d/%d",
                        scaleFactor, (double) mapper.getUsedLpids() / Simulator.TOTAL_PAGES, mapper.getUsedLpids(),
                        Simulator.TOTAL_PAGES));

                return new Result(scaleFactor, 0, sim.formatWriteCost(), sim.formatGCCost(), sim.formatE());
            }
        });
    }

    private static void trainGenerator(TPCCLpidGenerator gen, FileMapper mapper, int scaleFactor) throws Exception {
        TraceReader loadReader = new TraceReader(basePath + "load-" + scaleFactor + ".trace");
        TraceReader runReader = new TraceReader(basePath + "run-" + scaleFactor + ".trace");

        TraceOperation op = new TraceOperation();
        while (loadReader.read(op)) {
            if (op.op == TraceReader.WRITE) {
                gen.add(mapper.write(op.file, op.page));
            } else {
                throw new IllegalStateException("Unknown operation " + op.op);
            }
        }
        loadReader.close();

        while (runReader.read(op)) {
            if (op.op == TraceReader.WRITE) {
                gen.add(mapper.write(op.file, op.page));
            } else {
                throw new IllegalStateException("Unknown operation " + op.op);
            }
        }
        runReader.close();
        gen.compute();
    }

    private static void applyTrace(String phase, TraceReader reader, FileMapper mapper, Simulator sim, int progress,
            int stopPages) throws IOException {
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
                LOGGER.error("Simulation {} completed {}/{}. E: {}, write cost: {}, GC cost: {}", sim.param.name, i,
                        mapper.getUsedLpids(), sim.formatE(), sim.formatWriteCost(), sim.formatGCCost());
            }
            if (mapper.getUsedLpids() >= stopPages) {
                reader.close();
                return;
            }
        }
    }

}
