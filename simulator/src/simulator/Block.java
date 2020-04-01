package simulator;

class Block {
    final int blockIndex;
    final int lpids[];
    int count = 0;
    int avail = 0;
    long newestTs = 0;
    double aggTsSum = 0;
    double priorTsSum = 0;
    double updateTsSum = 0;
    boolean isGC = false;
    long closedTs;
    double updateFreqSum = 0;

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

        this.updateTsSum += ts;
        updateFreqSum -= updateFreq;
        this.avail++;
    }

    public void add(int lpid, long ts, double priorTs, double updateFreq, long newestTs) {
        this.aggTsSum = aggTsSum + ts;
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
        aggTsSum = 0;
        priorTsSum = 0;
        newestTs = 0;
        updateTsSum = 0;
        state = State.Free;
        isGC = false;
        updateFreqSum = 0;
    }

    public double updateFreq() {
        return updateFreqSum / (count - avail);
    }

    public double priorTs() {
        return priorTsSum / count;
    }

    public double aggTs() {
        return aggTsSum / count;
    }

}