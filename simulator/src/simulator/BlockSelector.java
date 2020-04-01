package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

interface BlockSelector {
    public int init(Simulator sim);

    public int selectUser(Simulator sim, int lpid, long priorTs, Block[] outputBlocks);

    public int selectGC(Simulator sim, int lpid, Block block, Block[] outputBlocks);

    public String name();

    public BlockSelector clone();
}

class NoBlockSelector implements BlockSelector {

    public static final NoBlockSelector INSTANCE = new NoBlockSelector();

    private NoBlockSelector() {
    }

    @Override
    public int init(Simulator sim) {
        return 1;
    }

    @Override
    public String name() {
        return "none";
    }

    @Override
    public int selectUser(Simulator sim, int lpid, long priorTs, Block[] outputBlocks) {
        return 0;
    }

    @Override
    public int selectGC(Simulator sim, int lpid, Block block, Block[] outputBlocks) {
        return 0;
    }

    @Override
    public NoBlockSelector clone() {
        return this;
    }
}

class OptBlockSelector implements BlockSelector {
    private double probs[];
    private int[] indexes;

    @Override
    public synchronized int init(Simulator sim) {
        if (probs != null) {
            return probs.length;
        }
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
        probs = new double[list.size()];
        for (int i = 0; i < probs.length; i++) {
            probs[i] = list.get(i);
        }

        int pos = 0;

        for (int i = gen.maxLpid(); i >= 1; i--) {
            while (gen.getProb(i) > probs[pos]) {
                pos++;
            }
            indexes[i] = pos;
        }

        return probs.length;
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
    public int selectGC(Simulator sim, int lpid, Block block, Block[] outputBlocks) {
        return indexes[lpid];
    }

    @Override
    public int selectUser(Simulator sim, int lpid, long priorTs, Block[] outputBlocks) {
        return indexes[lpid];
    }

}

class ColdHotBlockSelector implements BlockSelector {
    private static final int COLD_INDEX = 0;
    private static final int HOT_INDEX = 1;

    private double baseProb;

    @Override
    public int init(Simulator sim) {
        baseProb = 1.0 / sim.gen.maxLpid();
        return 2;
    }

    @Override
    public ColdHotBlockSelector clone() {
        return new ColdHotBlockSelector();
    }

    @Override
    public int selectGC(Simulator simulator, int lpid, Block block, Block[] outputBlocks) {
        assert outputBlocks.length >= 2;
        if (block.updateFreq() < baseProb) {
            return COLD_INDEX;
        } else {
            return HOT_INDEX;
        }
    }

    @Override
    public int selectUser(Simulator simulator, int lpid, long priorTs, Block[] outputBlocks) {
        assert outputBlocks.length >= 2;
        if (simulator.gen.getProb(lpid) < baseProb) {
            return COLD_INDEX;
        } else {
            return HOT_INDEX;
        }
    }

    @Override
    public String name() {
        return "opt";
    }
}
