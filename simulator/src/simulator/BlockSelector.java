package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

interface BlockSelector {
    public void init(Simulator sim);

    public int selectUser(Simulator sim, int lpid, Block block);

    public int selectGC(Simulator sim, IntArrayList lpids, Block block);

    public String name();

    public BlockSelector clone();

    public double updateFreq(int line);

    public int maxBatchSize();

}

class NoBlockSelector implements BlockSelector {

    public static final NoBlockSelector INSTANCE = new NoBlockSelector();

    private NoBlockSelector() {
    }

    @Override
    public void init(Simulator sim) {
        sim.addLine();
    }

    @Override
    public String name() {
        return "none";
    }

    @Override
    public int selectUser(Simulator sim, int lpid, Block block) {
        return 0;
    }

    @Override
    public int selectGC(Simulator sim, IntArrayList lpids, Block block) {
        return 0;
    }

    @Override
    public NoBlockSelector clone() {
        return this;
    }

    @Override
    public double updateFreq(int line) {
        return 0;
    }

    @Override
    public int maxBatchSize() {
        return Integer.MAX_VALUE;
    }
}

class OptBlockSelector implements BlockSelector {
    private static final Logger LOGGER = LogManager.getLogger(OptBlockSelector.class);

    private int[] indexes;
    private double[] probs;

    @Override
    public void init(Simulator sim) {
        LpidGenerator gen = sim.gen;
        indexes = new int[gen.maxLpid() + 1];
        Arrays.fill(indexes, -1);
        double min = gen.getMinProb();
        double max = gen.getMaxProb();
        if (min > max) {
            System.out.println("Fail to initialize opt block selector with min prob " + min + " max prob " + max);
            return;
        }
        Preconditions.checkState(min <= max);
        List<Double> list = new ArrayList<>();
        double curr = max;
        while (curr > min) {
            list.add(curr);
            curr /= 2;
        }
        list.add(curr);
        System.out.println(name() + " has " + list.size() + " lines");
        Preconditions.checkState(list.size() >= 1);
        probs = new double[list.size()];
        for (int i = 0; i < probs.length; i++) {
            probs[i] = list.get(i);
            sim.addLine();
        }
        LOGGER.error("opt block selector has {} logs", probs.length);
        int progress = gen.maxLpid() / 10;
        for (int i = 1; i <= gen.maxLpid(); i++) {
            double prob = gen.getProb(i);
            if (prob > 0) {
                boolean found = false;
                for (int k = probs.length - 1; k >= 0; k--) {
                    if (prob >= probs[k]) {
                        indexes[i] = k;
                        found = true;
                    }
                }
                assert found;
            }
            if (i % progress == 0) {
                LOGGER.error(String.format("Computed %d/%d probs", i, gen.maxLpid()));
            }
        }

    }

    @Override
    public OptBlockSelector clone() {
        return new OptBlockSelector();
    }

    @Override
    public String name() {
        return "opt";
    }

    @Override
    public int selectGC(Simulator sim, IntArrayList lpids, Block block) {
        return block.line;
    }

    @Override
    public int selectUser(Simulator sim, int lpid, Block block) {
        if (block != null) {
            return block.line;
        } else {
            return indexes[lpid];
        }
    }

    @Override
    public double updateFreq(int line) {
        return probs[line];
    }

    @Override
    public int maxBatchSize() {
        return Integer.MAX_VALUE;
    }
}

class MultiLogBlockSelector implements BlockSelector {

    private static final Logger LOGGER = LogManager.getLogger(MultiLogBlockSelector.class);
    static final int MAX_LOG_INDEX = 60;

    final LongArrayList intervals = new LongArrayList();
    long userTotal;
    long userIntended;
    long userPromoted;

    long gcTotal;
    long gcDemoted;

    public MultiLogBlockSelector() {
    }

    @Override
    public void init(Simulator sim) {
        intervals.add(1);
        sim.addLine();
    }

    @Override
    public MultiLogBlockSelector clone() {
        return new MultiLogBlockSelector();
    }

    @Override
    public String name() {
        return "multi-log";
    }

    public long getInterval(int line) {
        return intervals.getLong(line);
    }

    @Override
    public int selectGC(Simulator sim, IntArrayList lpids, Block block) {
        assert lpids.size() > 0;
        gcTotal++;
        if (block.line == MAX_LOG_INDEX) {
            return block.line;
        }
        Line line = sim.lines.get(block.line);
        double validProb = line.validProb();
        double prob = Math.pow(validProb, lpids.size());
        if (ThreadLocalRandom.current().nextDouble() <= 1 - prob) {
            // demote
            gcDemoted++;
            if (block.line + 1 == sim.lines.size()) {
                // add a new line
                sim.addLine();
                intervals.add(intervals.getLong(intervals.size() - 1) * 2);
            }
            return block.line + 1;
        } else {
            return block.line;
        }
    }

    @Override
    public double updateFreq(int line) {
        return 1.0 / intervals.getLong(line);
    }

    @Override
    public int selectUser(Simulator sim, int lpid, Block block) {
        userTotal++;
        if (block == null) {
            return 0;
        } else {
            Line line = sim.lines.get(block.line);
            double sizeRatio = line.sizeRatio(sim);
            double interval = line.ts - block.lineTs();
            assert interval >= 0;
            double expectedInterval = sizeRatio * sim.gen.maxLpid() * (1 - line.validProb()) / 2;
            boolean promote = false;
            if (interval < expectedInterval) {
                userIntended++;
                double prob = (expectedInterval - interval) / expectedInterval;
                promote = ThreadLocalRandom.current().nextDouble() <= prob;
            }
            if (promote && block.line > 0) {
                userPromoted++;
                return block.line - 1;
            } else {
                return block.line;
            }
        }
    }

    @Override
    public int maxBatchSize() {
        return Integer.MAX_VALUE;
    }

    public void print() {
        LOGGER.error("block selector has {} logs", intervals.size());
    }
}

class HotColdBlockSelector implements BlockSelector {
    private static final int COLD_INDEX = 0;
    private static final int HOT_INDEX = 1;

    private double baseProb;

    @Override
    public void init(Simulator sim) {
        baseProb = 1.0 / sim.gen.maxLpid();
        sim.addLine();
        sim.addLine();
    }

    @Override
    public int selectGC(Simulator sim, IntArrayList lpids, Block block) {
        assert lpids.size() == 1;
        int lpid = lpids.getInt(0);
        if (sim.gen.getProb(lpid) < baseProb) {
            return COLD_INDEX;
        } else {
            return HOT_INDEX;
        }
    }

    @Override
    public int selectUser(Simulator sim, int lpid, Block block) {
        if (sim.gen.getProb(lpid) < baseProb) {
            return COLD_INDEX;
        } else {
            return HOT_INDEX;
        }
    }

    @Override
    public HotColdBlockSelector clone() {
        return new HotColdBlockSelector();
    }

    @Override
    public double updateFreq(int line) {
        return 0;
    }

    @Override
    public String name() {
        return "hot-cold";
    }

    @Override
    public int maxBatchSize() {
        return 1;
    }
}
