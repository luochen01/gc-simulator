package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

interface BlockSelector {
    public void init(Simulator sim);

    public int selectUser(Simulator sim, int lpid, Block block);

    public int selectGC(Simulator sim, IntArrayList lpids, Block block);

    public String name();

    public BlockSelector clone();

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
}

class OptBlockSelector implements BlockSelector {
    private int[] indexes;

    @Override
    public void init(Simulator sim) {
        LpidGenerator gen = sim.gen;
        indexes = new int[gen.maxLpid() + 1];
        Arrays.fill(indexes, -1);
        double min = gen.getMinProb();
        double max = gen.getMaxProb();
        Preconditions.checkState(min <= max);
        List<Double> list = new ArrayList<>();
        double curr = 2 * min;
        while (curr <= max) {
            list.add(curr);
            curr *= 2;
        }
        list.add(curr);
        System.out.println(name() + " has " + list.size() + " lines");
        Preconditions.checkState(list.size() >= 1);
        double[] probs = new double[list.size()];
        for (int i = 0; i < probs.length; i++) {
            probs[i] = list.get(i);
            sim.addLine();
        }
        int pos = 0;
        for (int i = gen.maxLpid(); i >= 1; i--) {
            while (gen.getProb(i) > probs[pos]) {
                pos++;
            }
            indexes[i] = pos;
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
        return indexes[lpids.getInt(0)];
    }

    @Override
    public int selectUser(Simulator sim, int lpid, Block block) {
        return indexes[lpid];
    }
}

class MultiLogBlockSelector implements BlockSelector {

    final LongArrayList intervals = new LongArrayList();
    long userTotal;
    long userIntended;
    long userPromoted;

    long gcTotal;
    long gcDemoted;

    @Override
    public void init(Simulator sim) {
        intervals.add(1000);
        sim.lines.add(new Line(0));
        sim.userBlocks.add(sim.getFreeBlock(0));
        sim.gcBlocks.add(sim.getFreeBlock(0));

    }

    @Override
    public MultiLogBlockSelector clone() {
        return new MultiLogBlockSelector();
    }

    @Override
    public String name() {
        return "multi-log";
    }

    @Override
    public int selectGC(Simulator sim, IntArrayList lpids, Block block) {
        assert lpids.size() > 0;
        gcTotal++;
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
    public int selectUser(Simulator sim, int lpid, Block block) {
        userTotal++;
        if (block == null) {
            long interval = sim.currentTs;
            return addToBlock(sim, interval);
        } else {
            Line line = sim.lines.get(block.line);
            long interval = sim.currentTs - block.writeTs();
            double expectedInterval = Simulator.TOTAL_BLOCKS * (1 - line.validProb()) / 2;
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

    private int addToBlock(Simulator sim, long interval) {
        long last = intervals.getLong(intervals.size() - 1);
        if (last >= interval) {
            final int len = intervals.size();
            for (int i = 0; i < len; i++) {
                if (intervals.getLong(i) >= interval) {
                    return i;
                }
            }
            throw new IllegalStateException();
        } else {
            while (last < interval) {
                last *= 2;
                intervals.add(last);
                sim.addLine();
            }
            return intervals.size() - 1;
        }
    }
}
