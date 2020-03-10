package simulator;

import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.FastMath;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntLists;

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

    private final IntArrayList lpidMapping;
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
        lpidMapping = new IntArrayList(maxLpid);

        for (int i = 0; i < maxLpid; i++) {
            lpidMapping.add(i);
        }
        IntLists.shuffle(lpidMapping, new Random(0));
    }

    @Override
    public double getProb(int lpid) {
        return probs[lpid];
    }

    @Override
    public String name() {
        return "zipf-" + rand.getExponent();
    }

    @Override
    public int generate() {
        return lpidMapping.getInt(rand.sample() - 1) + 1;
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