package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import simulator.Block.State;

class Param {
    private final BlockSelector blockSelector;
    private final WriteBuffer writeBuffer;
    final ScoreComputer scoreComputer;
    final LpidGeneratorFactory genFactory;
    final int batchBlocks;
    final Comparator<Block> sorter;

    public Param(LpidGeneratorFactory genFactory, WriteBuffer writeBuffer, BlockSelector blockSelector,
            ScoreComputer scoreComputer, Comparator<Block> sorter, int batchBlocks) {
        this.genFactory = genFactory;
        this.writeBuffer = writeBuffer;
        this.blockSelector = blockSelector;
        this.scoreComputer = scoreComputer;
        this.sorter = sorter;
        this.batchBlocks = batchBlocks;

    }

    public BlockSelector createBlockSelector() {
        return blockSelector.clone();
    }

    public WriteBuffer createWriteBuffer() {
        return writeBuffer.clone();
    }

    @Override
    public String toString() {
        return genFactory + "/" + blockSelector.name() + "/" + scoreComputer.name();
    }

}

public class Simulator {

    public static final int TOTAL_BLOCKS = 12800; // 100GB
    public static final int BLOCK_SIZE = 512; // 2MB
    public static final int TOTAL_PAGES = TOTAL_BLOCKS * BLOCK_SIZE;

    public static final int GC_TRIGGER_BLOCKS = 32;

    public final Block[] blocks;
    public final Deque<Block> freeBlocks = new LinkedList<>();
    public int usedBlocks = 0;
    public long[] mappingTable = new long[TOTAL_PAGES];
    public long currentTs = 0;
    public long movedPages = 0;
    public long movedBlocks = 0;
    public long writes = 0;

    public long prevWrites = 0;
    public long prevMovedPages = 0;
    public long prevMovedBlocks = 0;

    public final List<Line> lines = new ArrayList<>();
    public final List<Block> userBlocks = new ArrayList<>();
    public final List<Block> gcBlocks = new ArrayList<>();

    public int maxLpid = -1;
    public final Param param;

    public final LpidGenerator gen;
    public final BlockSelector blockSelector;
    public final WriteBuffer writeBuffer;
    private boolean gcReversed;

    public Simulator(Param param, int maxLpid) {
        this.param = param;
        Arrays.fill(mappingTable, -1);
        blocks = new Block[TOTAL_BLOCKS];
        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            blocks[i] = new Block(i, BLOCK_SIZE);
            blocks[i].reset();
            freeBlocks.addLast(blocks[i]);
        }
        this.gen = param.genFactory.create(maxLpid);
        this.writeBuffer = param.createWriteBuffer();
        this.blockSelector = param.createBlockSelector();
        this.blockSelector.init(this);
    }

    public void load(int[] lpids) {
        this.maxLpid = lpids.length;
        int progress = lpids.length / 10;
        for (int i = 0; i < lpids.length; i++) {
            write(lpids[i]);
            if (i % progress == 0) {
                System.out.println(String.format("Simulation %s/%s loaded %d/%d.", gen.name(),
                        param.scoreComputer.name(), i, progress));
            }
        }
        writeBuffer.flush(this);
        resetStats();
    }

    public void addLine() {
        int line = lines.size();
        lines.add(new Line(line));
        userBlocks.add(getFreeBlock(line));
        gcBlocks.add(getFreeBlock(line));
    }

    public void run(long totalPages) {
        // load the dataset
        int parts = 100;
        long progress = totalPages / parts;

        for (int i = 1; i <= 10; i++) {
            for (int j = 0; j < progress; j++) {
                int lpid = gen.generate();
                write(lpid);
            }
            System.out
                    .println(String.format("Simulation %s/%.3f/%s completed %d/%d. E: %s, write cost: %s, GC cost: %s",
                            gen.name(), (double) maxLpid / TOTAL_PAGES, param.scoreComputer.name(), i * progress,
                            totalPages, formatE(), formatWriteCost(), formatGCCost()));

            if (i == parts / 2) {
                prevWrites = writes;
                prevMovedBlocks = movedBlocks;
                prevMovedPages = movedPages;
            }
        }
    }

    public void delete(int lpid) {
        long addr = mappingTable[lpid];
        if (addr != -1) {
            int blockIndex = getBlockIndex(addr);
            assert (blocks[blockIndex].state != State.Free);
            blocks[blockIndex].invalidate(currentTs, getPageIndex(addr), gen.getProb(lpid));
            mappingTable[lpid] = -1;
        }
    }

    public void write(int lpid) {
        long addr = mappingTable[lpid];
        Block prevBlock = null;
        if (addr != -1) {
            prevBlock = blocks[getBlockIndex(addr)];
        }
        writeBuffer.write(this, lpid, currentTs, prevBlock);
        writes++;
        currentTs++;
    }

    public void writeLpidToBlock(int lpid, long ts) {
        long addr = mappingTable[lpid];
        Block prevBlock = null;
        if (addr != -1) {
            prevBlock = blocks[getBlockIndex(addr)];
            prevBlock.invalidate(currentTs, getPageIndex(addr), gen.getProb(lpid));
            lines.get(prevBlock.line).totalAvail++;
        }
        int index = blockSelector.selectUser(this, lpid, prevBlock);
        Block userBlock = userBlocks.get(index);
        if (userBlock.count == BLOCK_SIZE) {
            userBlock.state = State.Used;
            userBlock.closedTs = currentTs;
            userBlock = getFreeBlock(index);
            userBlocks.set(index, userBlock);
        }
        userBlock.add(lpid, ts, prevBlock != null ? prevBlock.writeTs() : 0, gen.getProb(lpid), ts);
        updateMappingTable(lpid, userBlock.blockIndex, userBlock.count - 1);
        while (freeBlocks.size() <= GC_TRIGGER_BLOCKS) {
            runGC();
        }
    }

    private void runGC() {
        // select the best GC block
        IntArrayList lpids = new IntArrayList();
        PriorityQueue<Block> queue = new PriorityQueue<>((b1, b2) -> -Double.compare(b1.score, b2.score));

        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            Block block = blocks[i];
            if (block.state == State.Used) {
                block.score = param.scoreComputer.compute(this, block);
                queue.add(block);
                if (queue.size() > param.batchBlocks) {
                    queue.poll();
                }
            }
        }
        List<Block> blocks = new ArrayList<>(queue);
        if (param.sorter != null) {
            blocks.sort(param.sorter);
            if (gcReversed) {
                Collections.reverse(blocks);
            }
            gcReversed = !gcReversed;
        }
        for (Block minBlock : blocks) {
            int i = 0;
            double priorTs = minBlock.priorTs();
            long writeTs = minBlock.writeTs();
            while (i < BLOCK_SIZE) {
                lpids.clear();
                // skip invalid lpids
                while (i < BLOCK_SIZE && minBlock.lpids[i] < 0) {
                    i++;
                }
                // find contiguous valid lpids
                while (i < BLOCK_SIZE && minBlock.lpids[i] >= 0) {
                    lpids.add(minBlock.lpids[i]);
                    i++;
                }
                if (!lpids.isEmpty()) {
                    int index = this.blockSelector.selectGC(this, lpids, minBlock);
                    Block gcBlock = gcBlocks.get(index);
                    // process lpids
                    int count = lpids.size();
                    for (int j = 0; j < count; j++) {
                        if (gcBlock.count == BLOCK_SIZE) {
                            gcBlock.state = State.Used;
                            gcBlock.closedTs = currentTs;
                            gcBlock = getFreeBlock(index);
                            gcBlocks.set(index, gcBlock);
                        }
                        movedPages++;
                        int lpid = lpids.getInt(j);
                        gcBlock.add(lpid, writeTs, priorTs, gen.getProb(lpid), minBlock.newestTs);
                        updateMappingTable(lpid, gcBlock.blockIndex, gcBlock.count - 1);
                    }
                }
            }
            Line line = lines.get(minBlock.line);
            line.totalBlocks--;
            line.totalAvail -= minBlock.avail;
            minBlock.reset();
            minBlock.state = State.Free;
            freeBlocks.addLast(minBlock);
            usedBlocks--;
            movedBlocks++;
        }
    }

    public Block getFreeBlock(int line) {
        Block block = freeBlocks.pollFirst();
        assert (block != null);
        assert (block.state == State.Free);
        block.reset();
        block.state = State.Open;
        block.line = line;
        usedBlocks++;

        lines.get(line).totalBlocks++;
        return block;
    }

    private void updateMappingTable(int lpid, int blockIndex, int pageIndex) {
        assert (blockIndex >= 0);
        assert (pageIndex >= 0);
        long index = (((long) blockIndex) << 32) + pageIndex;
        mappingTable[lpid] = index;
    }

    private int getBlockIndex(long addr) {
        return (int) (addr >> 32);
    }

    public int getPageIndex(long addr) {
        return (int) addr;
    }

    public String formatGCCost() {
        long movedPages = this.movedPages - prevMovedPages;
        long writes = this.writes - prevWrites;

        return String.format("%.3f", (double) movedPages / Math.max(1, writes));
    }

    public String formatE() {
        long movedPages = this.movedPages - prevMovedPages;
        long movedBlocks = this.movedBlocks - prevMovedBlocks;

        return String.format("%.3f", 1 - (double) movedPages / BLOCK_SIZE / movedBlocks);
    }

    public String formatWriteCost() {
        long movedPages = this.movedPages - prevMovedPages;
        long movedBlocks = this.movedBlocks - prevMovedBlocks;

        return String.format("%.3f", 2.0 / (1 - (double) movedPages / BLOCK_SIZE / movedBlocks));
    }

    public void resetStats() {
        this.movedBlocks = 0;
        this.prevMovedBlocks = 0;
        this.movedPages = 0;
        this.prevMovedPages = 0;
        this.writes = 0;
        this.prevWrites = 0;
    }

}
