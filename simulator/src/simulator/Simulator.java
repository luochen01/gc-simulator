package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import simulator.Block.State;

class Param {
    private final BlockSelector blockSelector;
    final ScoreComputer scoreComputer;
    final LpidGeneratorFactory genFactory;

    final Comparator<Block> sorter;

    public Param(LpidGeneratorFactory genFactory, BlockSelector blockSelector, ScoreComputer scoreComputer,
            Comparator<Block> sorter) {
        this.genFactory = genFactory;
        this.blockSelector = blockSelector;
        this.scoreComputer = scoreComputer;
        this.sorter = sorter;
    }

    public BlockSelector createBlockSelector() {
        return blockSelector.clone();
    }

    @Override
    public String toString() {
        return genFactory + "/" + blockSelector.name() + "/" + scoreComputer.name();
    }

}

public class Simulator {

    public static final int TOTAL_BLOCKS = 12800; // 100GB
    //public static final int TOTAL_BLOCKS = 1280; // 100GB

    public static final int BLOCK_SIZE = 2048; // 8MB

    public static final int TOTAL_PAGES = TOTAL_BLOCKS * BLOCK_SIZE;

    public static final int GC_START_BLOCKS = 32;

    public static final int GC_STOP_BLOCKS = 64;

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

    public final Block[] userBlocks;
    public final Block[] gcBlocks;

    public final Param param;

    public final LpidGenerator gen;
    public final BlockSelector blockSelector;

    private final PriorityQueue<Block> queue = new PriorityQueue<>((b1, b2) -> Double.compare(b1.score, b2.score));
    private final List<Block> list = new ArrayList<>();

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
        this.blockSelector = param.createBlockSelector();
        int lines = this.blockSelector.init(this);
        userBlocks = new Block[lines];
        for (int i = 0; i < lines; i++) {
            userBlocks[i] = getFreeBlock(false);
        }
        gcBlocks = new Block[lines];
        for (int i = 0; i < lines; i++) {
            gcBlocks[i] = getFreeBlock(true);
        }
    }

    public void load(int[] lpids) {
        int progress = lpids.length / 10;
        for (int i = 0; i < lpids.length; i++) {
            write(lpids[i]);
            if (i % progress == 0) {
                System.out.println(String.format("Simulation %s/%s loaded %d/%d.", gen.name(),
                        param.scoreComputer.name(), i, progress));
            }
        }
        resetStats();
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
            System.out.println(String.format("Simulation %s/%s completed %d/%d. E: %s, write cost: %s, GC cost: %s",
                    gen.name(), param.scoreComputer.name(), i * progress, totalPages, formatE(), formatWriteCost(),
                    formatGCCost()));

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
        long priorTs = 0;

        if (addr != -1) {
            int blockIndex = getBlockIndex(addr);
            assert (blocks[blockIndex].state != State.Free);
            blocks[blockIndex].invalidate(currentTs, getPageIndex(addr), gen.getProb(lpid));
            priorTs = (long) blocks[blockIndex].aggTs();
        }
        int index = this.blockSelector.selectUser(this, lpid, priorTs, userBlocks);
        if (userBlocks[index].count == BLOCK_SIZE) {
            userBlocks[index].state = State.Used;
            userBlocks[index].closedTs = currentTs;
            userBlocks[index] = getFreeBlock(false);
        }
        userBlocks[index].add(lpid, currentTs, priorTs, gen.getProb(lpid), currentTs);

        writes++;
        currentTs++;
        updateMappingTable(lpid, userBlocks[index].blockIndex, userBlocks[index].count - 1);
        while (freeBlocks.size() <= GC_START_BLOCKS) {
            runGC();
        }
    }

    private void runGC() {
        // select the best GC block
        queue.clear();
        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            Block block = blocks[i];
            if (block.state == State.Used) {
                block.score = param.scoreComputer.compute(this, block);
                queue.add(block);
            }
        }

        double freedBlocks = 0;
        int targetBlocks = GC_STOP_BLOCKS - freeBlocks.size();
        list.clear();
        while (!queue.isEmpty() && freedBlocks < targetBlocks) {
            Block block = queue.poll();
            freedBlocks += (double) block.avail / BLOCK_SIZE;
            list.add(block);
        }

        list.sort(param.sorter);
        for (Block minBlock : list) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                int lpid = minBlock.lpids[i];
                double priorTs = minBlock.priorTs();
                double aggTs = minBlock.aggTsSum / BLOCK_SIZE;
                if (lpid >= 0) {
                    movedPages++;
                    int index = this.blockSelector.selectGC(this, lpid, minBlock, gcBlocks);
                    if (gcBlocks[index].count == BLOCK_SIZE) {
                        gcBlocks[index].state = State.Used;
                        gcBlocks[index].closedTs = currentTs;
                        gcBlocks[index] = getFreeBlock(true);
                    }
                    gcBlocks[index].add(lpid, (long) aggTs, priorTs, gen.getProb(lpid), minBlock.newestTs);
                    updateMappingTable(lpid, gcBlocks[index].blockIndex, gcBlocks[index].count - 1);
                }
            }
            // System.out.println("GC block with avail " + minBlock.avail + "/" + BLOCK_SIZE + " newest ts
            // "
            // + minBlock.newestTs + " current ts " + currentTs);
            minBlock.reset();
            minBlock.state = State.Free;
            freeBlocks.addLast(minBlock);
            usedBlocks--;
            movedBlocks++;
        }

    }

    public Block getFreeBlock(boolean isGC) {
        Block block = freeBlocks.pollFirst();
        assert (block != null);
        assert (block.state == State.Free);
        block.reset();
        block.state = State.Open;
        block.isGC = isGC;
        usedBlocks++;
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
