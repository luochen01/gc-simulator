package simulator;

import java.util.ArrayDeque;
import java.util.Queue;

class Line {
    final int lineIndex;

    long ts;
    private int validLpids;
    private Queue<Block> blocks = new ArrayDeque<>();

    public Line(int lineIndex) {
        this.lineIndex = lineIndex;
    }

    public double validProb() {
        if (blocks.isEmpty()) {
            return 0;
        } else {
            double alpha = ((double) blocks.size() * Simulator.BLOCK_SIZE - validLpids) / validLpids;
            double prob = Math.pow(Math.E, -0.9 * alpha) / (1 + alpha);
            return prob;
        }
    }

    public void add(Block block) {
        blocks.add(block);
        validLpids += block.validLpids();
        assert validLpids >= 0 && numLpids() >= validLpids;
    }

    public Block poll() {
        Block block = blocks.poll();
        if (block != null) {
            validLpids -= block.validLpids();
            assert validLpids >= 0 && numLpids() >= validLpids;
        }
        return block;
    }

    public double getAlpha() {
        return ((double) numLpids() - validLpids) / validLpids;
    }

    public double getAlpha(Simulator sim, double additionalBlocks) {
        return ((additionalBlocks + blocks.size()) * Simulator.BLOCK_SIZE - validLpids) / validLpids;
    }

    public double getBeta(Simulator sim) {
        return ((double) numLpids() - validLpids) / sim.maxLpid;
    }

    public int numBlocks() {
        return blocks.size();
    }

    public int numLpids() {
        return blocks.size() * Simulator.BLOCK_SIZE;
    }

    public double sizeRatio(Simulator sim) {
        return (double) validLpids / sim.maxLpid;
    }

    public void invalidateLpid() {
        validLpids--;
        assert validLpids >= 0;
    }

    @Override
    public String toString() {
        return String.format("line %d, valid %d, avail %d", lineIndex, validLpids, numLpids() - validLpids);
    }

}

class Block {
    final int blockIndex;
    final int lpids[];
    int count = 0;
    int avail = 0;
    long newestTs = 0;
    double writeTsSum = 0;
    double priorTsSum = 0;
    double lineTsSum = 0;
    long closedTs;
    double updateFreqSum = 0;
    int line;

    // used by GC
    double score = 0;

    public enum State {
        Used,
        Open,
        Free
    }

    State state = State.Free;

    public Block(int index, int size) {
        this.blockIndex = index;
        this.lpids = new int[size];
    }

    public void invalidate(long ts, int index, double updateFreq) {
        // mark it as invalid
        assert (lpids[index] >= 0);
        lpids[index] = -1;

        updateFreqSum -= updateFreq;
        this.avail++;
    }

    public void add(int lpid, long ts, double lineTs, double priorTs, double updateFreq, long newestTs) {
        this.writeTsSum += ts;
        this.priorTsSum += priorTs;
        this.lineTsSum += lineTs;
        this.lpids[count] = lpid;
        this.updateFreqSum += updateFreq;
        assert (this.updateFreqSum >= 0);
        this.newestTs = Math.max(this.newestTs, newestTs);
        count++;
    }

    public void reset() {
        closedTs = 0;
        count = 0;
        avail = 0;
        writeTsSum = 0;
        priorTsSum = 0;
        lineTsSum = 0;
        newestTs = 0;
        state = State.Free;
        updateFreqSum = 0;
        line = -1;
    }

    public double updateFreq() {
        return updateFreqSum / (count - avail);
    }

    public double priorTs() {
        return priorTsSum / count;
    }

    public double writeTs() {
        return writeTsSum / count;
    }

    public double lineTs() {
        return lineTsSum / count;
    }

    public int validLpids() {
        return count - avail;
    }

}