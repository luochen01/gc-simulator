package simulator;

import java.util.Arrays;
import java.util.Random;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.FastMath;

interface LpidGenerator {
    public int generate();

    public double getProb(int lpid);

    public int maxLpid();

    public String name();

    public double getMinProb();

    public double getMaxProb();
}

@FunctionalInterface
interface LpidGeneratorFactory {
    LpidGenerator create(int maxLpid);
}

class UniformLpidGenerator implements LpidGenerator {
    private final Random rand = new Random();
    private final int maxLpid;
    private final double prob;

    public static class UniformLpidGeneratorFactory implements LpidGeneratorFactory {

        public UniformLpidGeneratorFactory() {
        }

        @Override
        public LpidGenerator create(int maxLpid) {
            return new UniformLpidGenerator(maxLpid);
        }
    }

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
    public double getMinProb() {
        return prob;
    }

    @Override
    public double getMaxProb() {
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

class TPCCLpidGenerator implements LpidGenerator {
    public static class TPCCLpidGeneratorFactory implements LpidGeneratorFactory {
        @Override
        public LpidGenerator create(int maxLpid) {
            return new TPCCLpidGenerator(GCSimulator.TOTAL_PAGES);
        }
    }

    private long count;
    private final long[] freqs;
    private final double[] probs;
    private double minProb = 1;
    private double maxProb = 0;

    public TPCCLpidGenerator(int maxLpid) {
        this.freqs = new long[maxLpid];
        this.probs = new double[maxLpid];
        Arrays.fill(probs, -1);
    }

    public void add(int lpid) {
        count++;
        freqs[lpid]++;
    }

    public void compute() {
        for (int i = 1; i < freqs.length; i++) {
            if (freqs[i] > 0) {
                probs[i] = (double) freqs[i] / count;
                minProb = Math.min(minProb, probs[i]);
                maxProb = Math.max(maxProb, probs[i]);
            }
        }

    }

    @Override
    public int generate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getProb(int lpid) {
        return probs[lpid];
    }

    @Override
    public double getMinProb() {
        return minProb;
    }

    @Override
    public double getMaxProb() {
        return maxProb;
    }

    @Override
    public String name() {
        return "tpcc";
    }

    @Override
    public int maxLpid() {
        return freqs.length - 1;
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

    private final IntegerDistribution rand;

    private final double[] probs;
    private final double exp;
    private final int maxLpid;

    public ZipfLpidGenerator(int maxLpid, double exp) {
        this.exp = exp;
        this.maxLpid = maxLpid;
        if (exp == 0) {
            this.rand = new UniformIntegerDistribution(1, maxLpid);
            probs = new double[maxLpid + 1];
            for (int i = 1; i <= maxLpid; i++) {
                probs[i] = 1.0 / maxLpid;
            }
        } else {
            this.rand = new ZipfDistribution(maxLpid, exp);
            double h = 0;
            for (int k = maxLpid; k > 0; --k) {
                h += 1.0 / FastMath.pow(k, exp);
            }
            double harmonic = h;
            probs = new double[maxLpid + 1];
            for (int i = 1; i <= maxLpid; i++) {
                probs[i] = (1.0 / FastMath.pow(i, exp)) / harmonic;
            }
        }
    }

    @Override
    public double getProb(int lpid) {
        return probs[lpid];
    }

    @Override
    public String name() {
        return "zipf-" + exp;
    }

    @Override
    public int generate() {
        return rand.sample();
    }

    @Override
    public double getMinProb() {
        return probs[maxLpid];
    }

    @Override
    public double getMaxProb() {
        return probs[1];
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
        this.coldProb = hotSkew / 100.0 / numCold;
        this.hotProb = (100 - hotSkew) / 100.0 / numHot;
    }

    @Override
    public double getMaxProb() {
        return hotProb;
    }

    @Override
    public double getMinProb() {
        return coldProb;
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
        return "hot-cold-" + hotSkew;
    }
}