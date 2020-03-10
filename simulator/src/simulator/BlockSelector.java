package simulator;

interface BlockSelector {
    default public void init(Simulator sim) {

    }

    public int selectUser(Simulator sim, int lpid, long priorTs, Block[] outputBlocks);

    public int selectGC(Simulator sim, Block block, Block[] outputBlocks);

    public String name();
}

class NoBlockSelector implements BlockSelector {

    public static final NoBlockSelector INSTANCE = new NoBlockSelector();

    private NoBlockSelector() {
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
    public int selectGC(Simulator sim, Block block, Block[] outputBlocks) {
        return 0;
    }
}

class AdaptiveBlockSelector implements BlockSelector {
    @Override
    public int selectGC(Simulator sim, Block block, Block[] outputBlocks) {
        int index = 0;
        double aggTs = block.aggTsSum;
        for (int i = 0; i < outputBlocks.length; i++) {
            if (outputBlocks[i].count == 0) {
                return i;
            }

            double diff1 = Math.abs(aggTs - outputBlocks[index].aggTsSum);
            double diff2 = Math.abs(aggTs - outputBlocks[i].aggTsSum);
            if (diff2 < diff1) {
                index = i;
            }
        }
        return index;
    }

    @Override
    public int selectUser(Simulator sim, int lpid, long priorTs, Block[] outputBlocks) {
        int index = 0;
        for (int i = 0; i < outputBlocks.length; i++) {
            if (outputBlocks[i].count == 0) {
                return i;
            }

            double diff1 = Math.abs(priorTs - outputBlocks[index].priorTs());
            double diff2 = Math.abs(priorTs - outputBlocks[i].priorTs());
            if (diff2 < diff1) {
                index = i;
            }
        }
        return index;
    }

    @Override
    public String name() {
        return "timestamp";
    }

}

class OptBlockSelector implements BlockSelector {
    private static final int COLD_INDEX = 0;
    private static final int HOT_INDEX = 1;

    private double baseProb;

    @Override
    public void init(Simulator sim) {
        baseProb = 1.0 / sim.gen.maxLpid();
    }

    @Override
    public int selectGC(Simulator simulator, Block block, Block[] outputBlocks) {
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
