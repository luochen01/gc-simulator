package simulator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.FastMath;
import simulator.Block.State;
import simulator.HotColdLpidGenerator.HotColdLpidGeneratorFactory;
import simulator.ZipfLpidGenerator.ZipfLpidGeneratorFactory;

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
    Used, Open, Free
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
    assert (this.updateFreqSum >= 0);
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

}

interface LpidGenerator {
  public int generate();

  public double getProb(int lpid);

  public int maxLpid();

  public String name();
}

@FunctionalInterface
interface LpidGeneratorFactory {
  LpidGenerator create(int maxLpid);
}

class UniformLpidGenerator implements LpidGenerator {
  private final Random rand = new Random();
  private final int maxLpid;
  private final double prob;

  public UniformLpidGenerator(int maxLpid) {
    this.maxLpid = maxLpid;
    this.prob = 1.0 / (maxLpid);
  }

  @Override
  public int generate() {
    return rand.nextInt(maxLpid) + 1;
  }

  @Override
  public double getProb(int lpid) {
    return prob;
  }

  @Override
  public String name() {
    return "uniform";
  }

  @Override
  public int maxLpid() {
    return maxLpid;
  }

}

class ZipfLpidGenerator implements LpidGenerator {

  public static final double DEFAULT_EXP = 0.99;

  public static class ZipfLpidGeneratorFactory implements LpidGeneratorFactory {

    private final double exp;

    public ZipfLpidGeneratorFactory(double exp) {
      this.exp = exp;
    }

    @Override
    public LpidGenerator create(int maxLpid) {
      return new ZipfLpidGenerator(maxLpid, exp);
    }
  }

  private final ZipfDistribution rand;

  private final double[] probs;
  private final double harmonic;
  private final int maxLpid;

  public ZipfLpidGenerator(int maxLpid, double exp) {
    this.maxLpid = maxLpid;
    this.rand = new ZipfDistribution(maxLpid, exp);
    double h = 0;
    h = 0;
    for (int k = maxLpid; k > 0; --k) {
      h += 1.0 / FastMath.pow(k, rand.getExponent());
    }
    harmonic = h;
    probs = new double[maxLpid + 1];
    for (int i = 1; i <= maxLpid; i++) {
      probs[i] = (1.0 / FastMath.pow(i, rand.getExponent())) / harmonic;
    }
  }

  @Override
  public double getProb(int lpid) {
    return probs[lpid];
  }

  @Override
  public String name() {
    return "zipf";
  }

  @Override
  public int generate() {
    return rand.sample();
  }

  @Override
  public int maxLpid() {
    return maxLpid;
  }

}

class HotColdLpidGenerator implements LpidGenerator {
  private final Random rand = new Random();
  private final int hotSkew;
  private final int numCold;
  private final int numHot;
  private final double coldProb;
  private final double hotProb;

  private final int maxLpid;

  public static class HotColdLpidGeneratorFactory implements LpidGeneratorFactory {
    private final int hotSkew;

    public HotColdLpidGeneratorFactory(int hotSkew) {
      this.hotSkew = hotSkew;
    }

    @Override
    public LpidGenerator create(int maxLpid) {
      return new HotColdLpidGenerator(maxLpid, hotSkew);
    }

    @Override
    public String toString() {
      return "hot-cold:" + hotSkew + "-" + (100 - hotSkew);
    }

  }

  public HotColdLpidGenerator(int maxLpid, int hotSkew) {
    this.maxLpid = maxLpid;
    this.hotSkew = hotSkew;
    this.numHot = maxLpid / 100 * hotSkew;
    this.numCold = maxLpid / 100 * (100 - hotSkew);
    // assert (numCold + numHot == maxLpid);

    this.coldProb = hotSkew / 100.0 / numCold;
    this.hotProb = (100 - hotSkew) / 100.0 / numHot;
  }

  @Override
  public double getProb(int lpid) {
    return lpid <= numHot ? hotProb : coldProb;
  }

  @Override
  public int maxLpid() {
    return maxLpid;
  }

  @Override
  public int generate() {
    int sample = rand.nextInt(100);
    if (sample < hotSkew) {
      // generate a cold data
      return rand.nextInt(numCold) + numHot + 1;
    } else {
      // generate a hot data
      return rand.nextInt(numHot) + 1;
    }
  }

  @Override
  public String name() {
    return "hot-cold";
  }

}

interface BlockSelector {
  default public void init(Simulator sim) {

  }

  public int selectUser(Simulator sim, int lpid, Block[] outputBlocks);

  public int selectGC(Simulator sim, Block block, Block[] outputBlocks);

  public String name();
}

class NoBlockSelector implements BlockSelector {

  @Override
  public String name() {
    return "none";
  }

  @Override
  public int selectUser(Simulator sim, int lpid, Block[] outputBlocks) {
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
  public int selectUser(Simulator sim, int lpid, Block[] outputBlocks) {
    return 0;
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
  public int selectUser(Simulator simulator, int lpid, Block[] outputBlocks) {
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

class AgeBlockSelector implements BlockSelector {

  private final int interval;

  public AgeBlockSelector(int interval) {
    this.interval = interval;
  }

  @Override
  public int selectGC(Simulator simulator, Block block, Block[] outputBlocks) {
    for (int i = 0; i < outputBlocks.length; i++) {
      if (block.aggTsSum >= simulator.currentTs - i * interval) {
        return i;
      }
    }
    return outputBlocks.length - 1;
  }

  @Override
  public int selectUser(Simulator sim, int lpid, Block[] outputBlocks) {
    return 0;
  }

  @Override
  public String name() {
    return "age-" + interval;
  }
}

class RoundRobinSelector implements BlockSelector {
  private int gcIndex = 0;
  private int userIndex = 0;

  @Override
  public int selectGC(Simulator simulator, Block block, Block[] outputBlocks) {
    gcIndex = (gcIndex + 1) % outputBlocks.length;
    return gcIndex;
  }

  @Override
  public int selectUser(Simulator sim, int lpid, Block[] outputBlocks) {
    userIndex = (userIndex + 1) % outputBlocks.length;
    return userIndex;
  }

  @Override
  public String name() {
    return "round-robin";
  }

}

interface ScoreComputer {
  public static final double MAX_SCORE = Double.MAX_VALUE;

  public double compute(Simulator sim, Block block);

  public String name();
}

class MinDecline implements ScoreComputer {

  @Override
  public double compute(Simulator sim, Block block) {
    double E = (double) block.avail / Simulator.BLOCK_SIZE;
    double age = (sim.currentTs - block.aggTsSum);
    return (1 - E) / (E * E) / age;
  }

  @Override
  public String name() {
    return "min-decline";
  }
}

class MinDeclinePriorTs implements ScoreComputer {

  @Override
  public double compute(Simulator sim, Block block) {
    double E = (double) block.avail / Simulator.BLOCK_SIZE;
    double age = (sim.currentTs - block.priorTsSum / Simulator.BLOCK_SIZE) / 2;
    return (1 - E) / (E * E) / age;
  }

  @Override
  public String name() {
    return "min-decline-prior-ts";
  }
}

class MinDeclineWeightedAvg implements ScoreComputer {

  @Override
  public double compute(Simulator sim, Block block) {
    double E = (double) block.avail / Simulator.BLOCK_SIZE;

    double age = block.avail
        * (block.updateTsSum / block.avail - block.priorTsSum / Simulator.BLOCK_SIZE) / 2
        + (block.count - block.avail) * (sim.currentTs - block.priorTsSum / Simulator.BLOCK_SIZE)
            / 2;
    return (1 - E) / (E * E) / age;
  }

  @Override
  public String name() {
    return "min-decline-weighted-avg";
  }
}

class MinDeclineOptUpdate implements ScoreComputer {
  @Override
  public double compute(Simulator sim, Block block) {
    double E = (double) block.avail / Simulator.BLOCK_SIZE;
    double updateFreq = block.updateFreqSum / (block.count - block.avail);
    return updateFreq * (1 - E) / (E * E);
  }

  @Override
  public String name() {
    return "min-decline-opt-update-freq";
  }
}

class Berkeley implements ScoreComputer {

  public static final double FULL_LEVEL = 0.95;

  @Override
  public double compute(Simulator sim, Block block) {
    int active = Simulator.BLOCK_SIZE - block.avail;
    if ((double) active / Simulator.BLOCK_SIZE >= FULL_LEVEL) {
      return Double.MAX_VALUE;
    }
    double age = (sim.currentTs - block.newestTs);
    return (double) (Simulator.BLOCK_SIZE + active) / (Simulator.BLOCK_SIZE - active) / age;
  }

  @Override
  public String name() {
    return "berkeley";
  }
}

class MaxAvail implements ScoreComputer {

  @Override
  public double compute(Simulator sim, Block block) {
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
  public double compute(Simulator sim, Block block) {
    double age = Math.max(sim.currentTs - block.closedTs, 1.0) / 1000 / 1000;
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
  public double compute(Simulator sim, Block block) {
    return rand.nextDouble();
  }

  @Override
  public String name() {
    return "random";
  }
}

class Param {
  final BlockSelector blockSelector;
  final ScoreComputer scoreComputer;
  final LpidGeneratorFactory genFactory;
  final int userLines;
  final int gcLines;

  final Comparator<Block> sorter;

  public Param(LpidGeneratorFactory genFactory, BlockSelector blockSelector,
      ScoreComputer scoreComputer, int userLines, int gcLines, Comparator<Block> sorter) {
    this.genFactory = genFactory;
    this.blockSelector = blockSelector;
    this.scoreComputer = scoreComputer;
    this.userLines = userLines;
    this.gcLines = gcLines;
    this.sorter = sorter;
  }

  @Override
  public String toString() {
    return genFactory + "/" + blockSelector.name() + "/" + scoreComputer.name() + "/" + userLines
        + "/" + gcLines;
  }

}

public class Simulator {

  public static final int TOTAL_BLOCKS = 1280;
  // public static final int TOTAL_BLOCKS = 1024;

  public static final int BLOCK_SIZE = 2048; // 8MB

  // public static final int BLOCK_SIZE = 256; // 2MB

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

  public final Block[] userBlocks;
  public final Block[] gcBlocks;

  public final Param param;

  public final LpidGenerator gen;

  private final PriorityQueue<Block> queue =
      new PriorityQueue<>((b1, b2) -> Double.compare(b1.score, b2.score));
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
    userBlocks = new Block[param.userLines];
    for (int i = 0; i < param.userLines; i++) {
      userBlocks[i] = getFreeBlock(false);
    }
    gcBlocks = new Block[param.gcLines];
    for (int i = 0; i < param.gcLines; i++) {
      gcBlocks[i] = getFreeBlock(true);
    }
    this.gen = param.genFactory.create(maxLpid);
    param.blockSelector.init(this);
  }

  public void load(int[] lpids) {
    for (int i = 0; i < lpids.length; i++) {
      write(lpids[i]);
    }
  }

  public void run(long totalPages) {
    // load the dataset

    long progress = totalPages / 10;
    for (long i = 0; i < totalPages; i++) {
      int lpid = gen.generate();
      write(lpid);
      if (i % progress == 0) {
        System.out.println("completed " + i + "/" + totalPages + " E " + formatE() + " write cost "
            + formatWriteCost() + " moved pages " + formatMovedPages());
      }
    }
  }

  private void write(int lpid) {
    int index = param.blockSelector.selectUser(this, lpid, userBlocks);

    if (userBlocks[index].count == BLOCK_SIZE) {
      userBlocks[index].state = State.Used;
      userBlocks[index].closedTs = currentTs;
      userBlocks[index] = getFreeBlock(false);
    }

    long addr = mappingTable[lpid];
    if (addr != -1) {
      int blockIndex = getBlockIndex(addr);
      assert (blocks[blockIndex].state != State.Free);
      blocks[blockIndex].invalidate(currentTs, getPageIndex(addr), gen.getProb(lpid));
      userBlocks[index].add(lpid, currentTs, blocks[blockIndex].aggTsSum / blocks[blockIndex].count,
        gen.getProb(lpid), currentTs);
    } else {
      userBlocks[index].add(lpid, currentTs, 0, gen.getProb(lpid), currentTs);
    }
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
        double priorTs = minBlock.priorTsSum / BLOCK_SIZE;
        double aggTs = minBlock.aggTsSum / BLOCK_SIZE;
        if (lpid >= 0) {
          movedPages++;
          int index = param.blockSelector.selectGC(this, minBlock, gcBlocks);
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

  public String formatMovedPages() {
    return String.format("%.2f", (double) movedPages / 1000 / 1000);
  }

  public String formatE() {
    return String.format("%.3f", 1 - (double) movedPages / BLOCK_SIZE / movedBlocks);
  }

  public String formatWriteCost() {
    double e = 1 - (double) movedPages / BLOCK_SIZE / movedBlocks;
    return String.format("%.3f", 2 / e);
  }

  public static void main(String[] args) throws Exception {
    runHotCold();
    // runZipf();
    // runHotColdOpt();
  }

  public static void runHotColdOpt() throws IOException {
    double[] factors = new double[] { 0.8 };

    int[] hots = new int[] { 20 };

    Comparator<Block> noSorter = (b1, b2) -> 0;

    BlockSelector optSelector = new OptBlockSelector();
    int lines = 2;
    for (int hot : hots) {
      LpidGeneratorFactory gen = new HotColdLpidGeneratorFactory(hot);
      Param[] params =
          new Param[] { new Param(gen, optSelector, new MaxAvail(), lines, lines, noSorter),
              new Param(gen, optSelector, new MinDeclinePriorTs(), lines, lines, noSorter),
              new Param(gen, optSelector, new MinDeclineOptUpdate(), lines, lines, noSorter),
              new Param(gen, optSelector, new Berkeley(), lines, lines, noSorter) };
      run(params, factors, "test-" + hot + "-opt.log", 100);
    }

  }

  public static void runHotCold() throws IOException, Exception {
    double[] factors = new double[] { 0.6, 0.7, 0.8, 0.9 };

    int[] hots = new int[] { 10, 20, 40 };
    // int[] hots = new int[] { 40 };

    Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
    Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);
    Comparator<Block> closedTsSorter = (b1, b2) -> Long.compare(b1.closedTs, b2.closedTs);
    Comparator<Block> updateFrepSorter =
        (b1, b2) -> Double.compare(b1.updateFreq(), b2.updateFreq());

    BlockSelector noSelector = new NoBlockSelector();
    BlockSelector adaptSelector = new AdaptiveBlockSelector();
    Thread[] threads = new Thread[hots.length];

    for (int i = 0; i < hots.length; i++) {
      final int hot = hots[i];
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          LpidGeneratorFactory gen = new HotColdLpidGeneratorFactory(hot);
          Param[] params =
              new Param[] { new Param(gen, adaptSelector, new MaxAvail(), 1, 3, closedTsSorter),
                  new Param(gen, adaptSelector, new MinDeclinePriorTs(), 1, 3, closedTsSorter),
                  new Param(gen, adaptSelector, new MinDeclineOptUpdate(), 1, 3, closedTsSorter),
                  new Param(gen, noSelector, new Berkeley(), 1, 1, newestSorter) };
          try {
            Simulator.run(params, factors, "test-" + hot + ".log", 100);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      threads[i].start();
    }

    for (int i = 0; i < hots.length; i++) {
      threads[i].join();
    }
  }

  public static void runZipf() throws IOException, InterruptedException {
    double[] factors = new double[] { 0.6, 0.7, 0.8, 0.9 };
    double[] skews = new double[] { 0.1, 0.5, 0.99, 1.35 };

    Comparator<Block> newestSorter = (b1, b2) -> Long.compare(b1.newestTs, b2.newestTs);
    Comparator<Block> priorTsSorter = (b1, b2) -> Double.compare(b1.priorTsSum, b2.priorTsSum);
    Comparator<Block> closedTsSorter = (b1, b2) -> Long.compare(b1.closedTs, b2.closedTs);
    Comparator<Block> updateFrepSorter =
        (b1, b2) -> Double.compare(b1.updateFreq(), b2.updateFreq());

    BlockSelector noSelector = new NoBlockSelector();
    BlockSelector adaptiveSelector = new AdaptiveBlockSelector();

    Thread[] threads = new Thread[skews.length];
    for (int i = 0; i < skews.length; i++) {
      final double skew = skews[i];
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          LpidGeneratorFactory gen = new ZipfLpidGeneratorFactory(skew);
          Param[] params = new Param[] {
              new Param(gen, new NoBlockSelector(), new MaxAvail(), 1, 1, closedTsSorter),
              new Param(gen, noSelector, new MinDeclinePriorTs(), 1, 1, closedTsSorter),
              new Param(gen, noSelector, new MinDeclineOptUpdate(), 1, 1, closedTsSorter),
              new Param(gen, new NoBlockSelector(), new Berkeley(), 1, 1, newestSorter) };
          try {
            Simulator.run(params, factors, "test-" + skew + ".log", 100);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

  }

  public static void run(Param[] params, double[] fills, String output, int scaleFactor)
      throws IOException {
    List<String> movedPages = new ArrayList<>();
    List<String> Es = new ArrayList<>();
    List<String> writeCosts = new ArrayList<>();

    StringBuilder header = new StringBuilder();
    header.append("utilization\t");
    for (Param param : params) {
      header.append(param.scoreComputer.name());
      header.append("\t");
    }

    PrintWriter writer = new PrintWriter(output);
    for (double fill : fills) {
      int numKeys = (int) (fill * Simulator.TOTAL_PAGES);
      int[] lpids = new int[numKeys];
      for (int i = 0; i < numKeys; i++) {
        lpids[i] = i + 1;
      }
      shuffleArray(lpids);

      long totalPages = (long) numKeys * scaleFactor;

      StringBuilder movedPageBuilder = new StringBuilder();
      StringBuilder EBuilder = new StringBuilder();
      StringBuilder writeCostBuider = new StringBuilder();
      movedPageBuilder.append(fill);
      EBuilder.append(fill);
      writeCostBuider.append(fill);

      for (Param param : params) {
        Simulator sim = new Simulator(param, numKeys);
        System.out.println("start " + param + " with fill factor " + fill);
        sim.load(lpids);
        sim.run(totalPages);

        movedPageBuilder.append("\t");
        movedPageBuilder.append(sim.formatMovedPages());

        EBuilder.append("\t");
        EBuilder.append(sim.formatE());

        writeCostBuider.append("\t");
        writeCostBuider.append(sim.formatWriteCost());
      }
      movedPages.add(movedPageBuilder.toString());
      Es.add(EBuilder.toString());
      writeCosts.add(writeCostBuider.toString());
    }

    writer.println("moved pages");
    writer.println(header.toString());
    for (String str : movedPages) {
      writer.println(str);
    }
    writer.println();

    writer.println("E (avail space)");
    writer.println(header.toString());
    for (String str : Es) {
      writer.println(str);
    }
    writer.println();

    writer.println("write cost (2/E)");
    writer.println(header.toString());
    for (String str : writeCosts) {
      writer.println(str);
    }
    writer.println();
    writer.close();
  }

  public static void shuffleArray(int[] a) {
    int n = a.length;
    Random random = new Random();
    random.nextInt();
    for (int i = 0; i < n; i++) {
      int change = i + random.nextInt(n - i);
      swap(a, i, change);
    }
  }

  private static void swap(int[] a, int i, int change) {
    int helper = a[i];
    a[i] = a[change];
    a[change] = helper;
  }

}
