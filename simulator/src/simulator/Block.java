package simulator;

class Line {
    final int lineIndex;
    int totalBlocks;
    int totalAvail;

    public Line(int lineIndex) {
        this.lineIndex = lineIndex;
    }

    public double validProb() {
        if (totalBlocks == 0) {
            return 0;
        } else {
            double prob = 1 - (double) totalAvail / (totalBlocks * Simulator.BLOCK_SIZE);
            assert prob >= 0 && prob <= 1;
            return prob;
        }
    }

    public int numLpids() {
        return totalBlocks * Simulator.BLOCK_SIZE;
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

    public void add(int lpid, long ts, double priorTs, double updateFreq, long newestTs) {
        this.writeTsSum = writeTsSum + ts;
        this.priorTsSum = this.priorTsSum + priorTs;
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
        newestTs = 0;
        state = State.Free;
        updateFreqSum = 0;
        line = -1;
    }

    public double updateFreq() {
        return updateFreqSum / (count - avail);
    }

    public long priorTs() {
        return (long) (priorTsSum / count);
    }

    public long writeTs() {
        return (long) (writeTsSum / count);
    }

}