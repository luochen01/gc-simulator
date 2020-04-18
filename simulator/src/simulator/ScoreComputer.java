package simulator;

public interface ScoreComputer {
    public static final double MAX_SCORE = Double.MAX_VALUE;

    public double compute(GCSimulator sim, Block block);

    public String name();
}

class MinDecline implements ScoreComputer {

    @Override
    public double compute(GCSimulator sim, Block block) {
        double E = (double) block.avail / GCSimulator.BLOCK_SIZE;
        double age = sim.currentTs - block.priorTs();
        return (1 - E) / (E * E) / age;
    }

    @Override
    public String name() {
        return "min-decline";
    }
}

class MinDeclineOpt implements ScoreComputer {
    @Override
    public double compute(GCSimulator sim, Block block) {
        if (block.count - block.avail == 0) {
            return 0;
        }
        double E = (double) block.avail / GCSimulator.BLOCK_SIZE;
        double updateFreq = block.updateFreqSum / (block.count - block.avail);
        return updateFreq * (1 - E) / (E * E);
    }

    @Override
    public String name() {
        return "min-decline-opt";
    }
}

class Berkeley implements ScoreComputer {

    public static final double FULL_LEVEL = 0.95;

    @Override
    public double compute(GCSimulator sim, Block block) {
        int active = GCSimulator.BLOCK_SIZE - block.avail;
        if ((double) active / GCSimulator.BLOCK_SIZE >= FULL_LEVEL) {
            return Double.MAX_VALUE;
        }
        double age = (sim.currentTs - block.newestTs);
        return (double) (GCSimulator.BLOCK_SIZE + active) / (GCSimulator.BLOCK_SIZE - active) / age;
    }

    @Override
    public String name() {
        return "berkeley";
    }
}

class MaxAvail implements ScoreComputer {

    @Override
    public double compute(GCSimulator sim, Block block) {
        double avail = Math.max(1, block.avail);
        return 1 / avail;
    }

    @Override
    public String name() {
        return "max-avail";
    }
}

class Oldest implements ScoreComputer {
    @Override
    public double compute(GCSimulator sim, Block block) {
        double age = Math.max(sim.currentTs - block.closedTs, 1.0) / 1000 / 1000;
        return 1 / age;
    }

    @Override
    public String name() {
        return "oldest";
    }
}
