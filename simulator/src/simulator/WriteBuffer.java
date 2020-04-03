package simulator;

import java.util.Arrays;

interface WriteBuffer {
    public WriteBuffer clone();

    public void write(Simulator sim, int lpid, long ts, Block block);

    public void flush(Simulator sim);
}

class NoWriteBuffer implements WriteBuffer {
    public static final NoWriteBuffer INSTANCE = new NoWriteBuffer();

    private NoWriteBuffer() {

    }

    @Override
    public void write(Simulator sim, int lpid, long ts, Block block) {
        sim.writeLpidToBlock(lpid, ts);
    }

    @Override
    public NoWriteBuffer clone() {
        return INSTANCE;
    }

    @Override
    public void flush(Simulator sim) {

    }
}

class SortWriteBuffer implements WriteBuffer {

    private static class Entry implements Comparable<Entry> {
        int lpid;
        long ts;
        long sortTs;

        public void reset(int lpid, long ts, Block block) {
            this.lpid = lpid;
            this.ts = ts;
            this.sortTs = block != null ? block.priorTs() : 0;
        }

        @Override
        public int compareTo(Entry o) {
            int cmp = Long.compare(sortTs, o.sortTs);
            if (cmp == 0) {
                return Integer.compare(lpid, o.lpid);
            } else {
                return cmp;
            }
        }
    }

    private final Entry[] entries;
    private int index = 0;
    private boolean reverse = false;

    public SortWriteBuffer(int size) {
        this.entries = new Entry[size];
        for (int i = 0; i < size; i++) {
            this.entries[i] = new Entry();
        }
    }

    @Override
    public WriteBuffer clone() {
        return new SortWriteBuffer(entries.length);
    }

    @Override
    public void write(Simulator sim, int lpid, long ts, Block block) {
        entries[index++].reset(lpid, ts, block);
        if (index == entries.length) {
            flush(sim);
        }
    }

    @Override
    public void flush(Simulator sim) {
        Arrays.sort(entries, 0, index);

        int prevLpid = -1;
        if (reverse) {
            for (int i = index - 1; i >= 0; i--) {
                Entry e = entries[i];
                if (e.lpid != prevLpid) {
                    sim.writeLpidToBlock(e.lpid, e.ts);
                    prevLpid = e.lpid;
                }
            }
        } else {
            for (int i = 0; i < index; i++) {
                Entry e = entries[i];
                if (e.lpid != prevLpid) {
                    sim.writeLpidToBlock(e.lpid, e.ts);
                    prevLpid = e.lpid;
                }
            }
        }
        reverse = !reverse;
        index = 0;
    }

}