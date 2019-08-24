package simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

import simulator.Block.State;

class Block {
    final int blockIndex;
    final long tss[];
    final int lpids[];
    int count = 0;
    int avail = 0;
    double aggFreq = 0;
    double aggTs = 0;
    boolean isGC = false;
    long closedTs;

    LinkedList<Long> lastUpdateTs = new LinkedList<>();

    public enum State {
        Used,
        Open,
        Free
    }

    State state = State.Free;

    public Block(int index, int size) {
        this.blockIndex = index;
        this.tss = new long[size];
        this.lpids = new int[size];
    }

    public void update(long ts) {
        lastUpdateTs.add(ts);
        if (lastUpdateTs.size() > Simulator.NUM_UPDATE_TS) {
            lastUpdateTs.removeFirst();
        }
    }

    public double updateFrequency(long currentTs) {
        if (lastUpdateTs.isEmpty()) {
            return 1;
        } else {
            return (double) 1000 * 1000 * lastUpdateTs.size() / Math.max(1, currentTs - lastUpdateTs.getFirst());
        }
    }

    public void add(int lpid, long ts, double freq) {
        if (count >= this.tss.length) {
            throw new IllegalStateException();
        }
        this.aggFreq = (aggFreq * count + ts) / (count + 1);
        this.aggTs = (aggTs * count + ts) / (count + 1);
        this.lpids[count] = lpid;
        this.tss[count] = ts;
        count++;
    }

    public void reset() {
        closedTs = 0;
        aggFreq = 0;
        count = 0;
        avail = 0;
        aggTs = 0;
        Arrays.fill(tss, 0);
        Arrays.fill(lpids, 0);
        state = State.Free;
        isGC = false;

        lastUpdateTs.clear();
    }

    public double std() {
        double total = 0;
        for (int i = 0; i < count; i++) {
            total += (tss[i] - aggTs) * (tss[i] - aggTs);
        }
        total /= count;
        return Math.sqrt(total);
    }
}

interface LpidGenerator {
    public int generate(int maxLpid);

    public String name();
}

class UniformLpidGenerator implements LpidGenerator {
    private final Random rand = new Random();

    @Override
    public int generate(int maxLpid) {
        return rand.nextInt(maxLpid);
    }

    @Override
    public String name() {
        return "uniform";
    }
}

class ZipfLpidGenerator implements LpidGenerator {
    private ZipfDistribution rand;

    @Override
    public int generate(int maxLpid) {
        if (rand == null) {
            rand = new ZipfDistribution(maxLpid, 0.99);
        }
        return rand.sample();
    }

    @Override
    public String name() {
        return "zipf";
    }
}

interface GCBlockSelector {
    public int select(Block block, Block[] outputBlocks, long currentTs);

    public String name();
}

class AdaptiveBlockSelector implements GCBlockSelector {
    @Override
    public int select(Block block, Block[] outputBlocks, long currentTs) {
        int index = 0;
        double freq = block.updateFrequency(currentTs);
        for (int i = 0; i < outputBlocks.length; i++) {
            if (outputBlocks[i].count == 0) {
                return i;
            }

            double diff1 = Math.abs(freq - outputBlocks[index].aggFreq);
            double diff2 = Math.abs(freq - outputBlocks[i].aggFreq);
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

class AgeBlockSelector implements GCBlockSelector {

    private final int interval;

    public AgeBlockSelector(int interval) {
        this.interval = interval;
    }

    @Override
    public int select(Block block, Block[] outputBlocks, long currentTs) {
        for (int i = 0; i < outputBlocks.length; i++) {
            if (block.aggFreq >= currentTs - i * interval) {
                return i;
            }
        }
        return outputBlocks.length - 1;
    }

    @Override
    public String name() {
        return "age-" + interval;
    }
}

class RoundRobinSelector implements GCBlockSelector {
    private int index = 0;

    @Override
    public int select(Block block, Block[] outputBlocks, long currentTs) {
        index = (index + 1) % outputBlocks.length;
        return index;
    }

    @Override
    public String name() {
        return "round-robin";
    }

}

interface ScoreComputer {
    public double compute(Block block, long currentTs);

    public String name();
}

class MinDecline implements ScoreComputer {

    public static double AVAIL_POW = 2;
    public static double AGE_POW = 1;

    @Override
    public double compute(Block block, long currentTs) {
        double E = (double) block.avail / Simulator.BLOCK_SIZE;
        double updateFreq = block.updateFrequency(currentTs);
        return (1 - E) / (E * E) * updateFreq;
    }

    @Override
    public String name() {
        return "min-decline";
    }
}

class MinDeclineTest implements ScoreComputer {
    public static double AVAIL_POW = 2;
    public static double AGE_POW = 1;

    @Override
    public double compute(Block block, long currentTs) {
        double E = (double) block.avail / Simulator.BLOCK_SIZE;
        double updateFreq = block.updateFrequency(currentTs);
        return (1 - E) / Math.pow(E, AVAIL_POW) / Math.pow(updateFreq, AGE_POW);
    }

    @Override
    public String name() {
        return "min-decline";
    }
}

class MaxAvail implements ScoreComputer {

    @Override
    public double compute(Block block, long currentTs) {
        double avail = Math.max(0.1, block.avail);
        return 1 / avail;
    }

    @Override
    public String name() {
        return "max-avail";
    }
}

class Oldest implements ScoreComputer {
    @Override
    public double compute(Block block, long currentTs) {
        double age = Math.max(currentTs - block.closedTs, 1.0) / 1000 / 1000;
        return 1 / age;
    }

    @Override
    public String name() {
        return "oldest";
    }
}

class RandomScore implements ScoreComputer {
    private Random rand = new Random();

    @Override
    public double compute(Block block, long currentTs) {
        return rand.nextDouble();
    }

    @Override
    public String name() {
        return "random";
    }
}

public class Simulator {

    public static final int TOTAL_BLOCKS = 1280;

    public static final int MIN_BLOCKS = 8;

    public static final int BLOCK_SIZE = 2048; // 8MB

    public static final int TOTAL_PAGES = TOTAL_BLOCKS * BLOCK_SIZE;

    public static double FILL_FACTOR = 0.8;

    public static int GC_LINES = 4;

    public static int NUM_UPDATE_TS = 1;

    public final Block[] blocks;
    public final Deque<Block> freeBlocks = new LinkedList<>();
    public int usedBlocks = 0;
    public long[] mappingTable = new long[TOTAL_PAGES];
    public long currentTs = 0;
    public long movedPages = 0;

    public Block openBlock;
    public Block[] gcBlocks = new Block[GC_LINES];

    private final LpidGenerator gen;
    private final GCBlockSelector selector;
    private final ScoreComputer computer;

    public Simulator(LpidGenerator gen, GCBlockSelector selector, ScoreComputer computer) {
        this.gen = gen;
        this.selector = selector;
        this.computer = computer;
        Arrays.fill(mappingTable, -1);
        blocks = new Block[TOTAL_BLOCKS];
        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            blocks[i] = new Block(i, BLOCK_SIZE);
            blocks[i].reset();
            freeBlocks.addLast(blocks[i]);
        }
        openBlock = getFreeBlock(false);
        for (int i = 0; i < GC_LINES; i++) {
            gcBlocks[i] = getFreeBlock(true);
        }
    }

    public void run(long totalPages) {

        for (long i = 0; i < totalPages; i++) {
            int lpid = gen.generate((int) (TOTAL_PAGES * FILL_FACTOR));

            if (openBlock.count == BLOCK_SIZE) {
                openBlock.state = State.Used;
                openBlock.closedTs = currentTs;
                openBlock = getFreeBlock(false);
            }
            openBlock.add(lpid, currentTs, 0);
            if (mappingTable[lpid] != -1) {
                int blockIndex = getBlockIndex(lpid);
                assert (blocks[blockIndex].state != State.Free);
                blocks[blockIndex].avail++;
                blocks[blockIndex].update(currentTs);
            }
            currentTs++;
            updateMappingTable(lpid, openBlock.blockIndex, openBlock.count - 1);
            while (TOTAL_BLOCKS - usedBlocks <= MIN_BLOCKS) {
                runGC();
            }

            if (i % 10000 == 0) {
                //System.out.println("finished pages " + i);
            }
        }
    }

    private void runGC() {
        // select the best GC block
        Block minBlock = null;
        double minScore = 0;
        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            Block block = blocks[i];
            if (block.state != State.Used) {
                continue;
            }
            double score = computer.compute(block, currentTs);
            if (minBlock == null || score < minScore) {
                minBlock = block;
                minScore = score;
            }
        }
        assert (minBlock != null);
        //        System.out.println("GC block with avail " + minBlock.avail + " age " + (int) (currentTs - minBlock.aggTs) / 1000
        //                + "K" + " score " + minScore);

        int index = selector.select(minBlock, gcBlocks, currentTs);

        double updateFreq = minBlock.updateFrequency(currentTs);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            int lpid = minBlock.lpids[i];
            if (getBlockIndex(lpid) == minBlock.blockIndex && getPageIndex(lpid) == i) {
                movedPages++;
                if (gcBlocks[index].count == BLOCK_SIZE) {
                    gcBlocks[index].state = State.Used;
                    gcBlocks[index].closedTs = currentTs;
                    gcBlocks[index] = getFreeBlock(true);
                }
                gcBlocks[index].add(lpid, (long) minBlock.aggTs, updateFreq);
                updateMappingTable(lpid, gcBlocks[index].blockIndex, gcBlocks[index].count - 1);
            }
        }

        minBlock.reset();
        minBlock.state = State.Free;
        freeBlocks.addLast(minBlock);
        usedBlocks--;
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

    private int getBlockIndex(int lpid) {
        long index = mappingTable[lpid];
        if (index == -1) {
            return -1;
        } else {
            return (int) (index >> 32);
        }
    }

    public int getPageIndex(int lpid) {
        long index = mappingTable[lpid];
        if (index == -1) {
            return -1;
        } else {
            return (int) index;
        }
    }

    public double averageStd() {
        int count = 0;
        double total = 0;
        for (int i = 0; i < TOTAL_BLOCKS; i++) {
            if (blocks[i].state == State.Used && blocks[i].isGC) {
                count++;
                total += blocks[i].std();
            }
        }
        return total / count;
    }

    public String formatMovedPages() {
        return String.format("%.2f", (double) movedPages / 1000 / 1000);
    }

    public static void main(String[] args) {
        Simulator.GC_LINES = 4;
        double[] factors = new double[] { 0.7, 0.8, 0.9 };
        int[] histories = new int[] { 1, 3, 10, 20, 50, 100 };
        for (double factor : factors) {
            Simulator.FILL_FACTOR = factor;
            System.out.println("fill factor " + factor);

            long totalPages = Simulator.TOTAL_PAGES * 10;

            for (int history : histories) {
                Simulator.NUM_UPDATE_TS = history;
                Simulator sim = new Simulator(new ZipfLpidGenerator(), new AdaptiveBlockSelector(), new MinDecline());
                sim.run(totalPages);
                System.out.println(history + "\t" + sim.formatMovedPages());
            }
        }
    }

    public static void mainOld(String[] args) {
        double[] factors = new double[] { 0.7, 0.8, 0.9 };
        for (double factor : factors) {
            Simulator.FILL_FACTOR = factor;
            System.out.println("fill factor " + factor);

            LpidGenerator[] gens = new LpidGenerator[] { new ZipfLpidGenerator() };
            long totalPages = Simulator.TOTAL_PAGES * 10;
            int[] intervals = { (int) 1e4, (int) 1e5, (int) 1e6, (int) 1e7 };
            GCBlockSelector[] selectors = new GCBlockSelector[intervals.length + 2];
            selectors[0] = new RoundRobinSelector();
            for (int i = 0; i < intervals.length; i++) {
                selectors[i + 1] = new AgeBlockSelector(intervals[i]);
            }
            selectors[intervals.length + 1] = new AdaptiveBlockSelector();

            ScoreComputer[] computers = new ScoreComputer[] { new RandomScore(), new Oldest() };
            ScoreComputer[] lineComputers = new ScoreComputer[] { new MaxAvail(), new MinDecline() };

            for (LpidGenerator gen : gens) {
                Simulator.GC_LINES = 1;
                for (ScoreComputer computer : computers) {
                    Simulator sim = new Simulator(gen, new RoundRobinSelector(), computer);
                    sim.run(totalPages);
                    System.out.println(gen.name() + "\t" + computer.name() + "\t" + sim.formatMovedPages());
                }

                for (ScoreComputer computer : lineComputers) {
                    List<String> movedPages = new ArrayList<>();
                    StringBuilder header = new StringBuilder();
                    header.append("lines\t");
                    for (GCBlockSelector selector : selectors) {
                        header.append(selector.name());
                        header.append("\t");
                    }
                    for (int lines = 1; lines <= 5; lines++) {

                        StringBuilder sbPages = new StringBuilder();
                        sbPages.append(lines);
                        sbPages.append("\t");
                        Simulator.GC_LINES = lines;

                        for (GCBlockSelector selector : selectors) {
                            Simulator sim = new Simulator(gen, selector, computer);
                            sim.run(totalPages);
                            sbPages.append(sim.formatMovedPages() + "\t");
                        }
                        movedPages.add(sbPages.toString());
                    }
                    System.out.println(header.toString());
                    for (String str : movedPages) {
                        System.out.println(str);
                    }

                }

            }
        }
    }

    public static void mainLine(String[] args) {
        Simulator.FILL_FACTOR = 0.8;

        long[] totalPages = { 5, 10, 15, 20, 25 };

        int[] intervals = { (int) 1e4, (int) 1e5, (int) 1e6, (int) 1e7 };
        GCBlockSelector[] selectors = new GCBlockSelector[intervals.length + 2];
        selectors[0] = new RoundRobinSelector();
        for (int i = 0; i < intervals.length; i++) {
            selectors[i + 1] = new AgeBlockSelector(intervals[i]);
        }
        selectors[intervals.length + 1] = new AdaptiveBlockSelector();

        for (long totalPage : totalPages) {
            System.out.println("totalPages\t" + totalPage * Simulator.TOTAL_PAGES);
            ScoreComputer computer = new MinDecline();
            List<String> movedPages = new ArrayList<>();
            StringBuilder header = new StringBuilder();
            header.append("lines\t");
            for (GCBlockSelector selector : selectors) {
                header.append(selector.name());
                header.append("\t");
            }
            for (int lines = 1; lines <= 5; lines++) {

                StringBuilder sbPages = new StringBuilder();
                sbPages.append(lines);
                sbPages.append("\t");
                Simulator.GC_LINES = lines;

                for (GCBlockSelector selector : selectors) {
                    Simulator sim = new Simulator(new ZipfLpidGenerator(), selector, computer);
                    sim.run(totalPage * Simulator.TOTAL_PAGES);
                    sbPages.append(sim.formatMovedPages() + "\t");
                }
                movedPages.add(sbPages.toString());
            }
            System.out.println(header.toString());
            for (String str : movedPages) {
                System.out.println(str);
            }
        }

    }

}
