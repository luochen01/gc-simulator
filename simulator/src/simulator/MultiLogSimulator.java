package simulator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import simulator.Block.State;

public class MultiLogSimulator extends GCSimulator {

    private static final Logger LOGGER = LogManager.getLogger(MultiLogSimulator.class);

    private static final int LINE_MIN_BLOCKS = 64;

    private static final double DELTA = 1;

    public MultiLogSimulator(Param param, int maxLpid) {
        super(param, maxLpid);
    }

    @Override
    protected void checkGC(int line) {
        if (freeBlocks.size() <= GC_TRIGGER_BLOCKS) {
            runGC(line);
        }
    }

    @Override
    protected int runGC(int line) {
        double dLine = computeDerivative(lines.get(line));

        double dPrev = Integer.MIN_VALUE;
        if (line > 0) {
            // check prev
            dPrev = computeDerivative(lines.get(line - 1));
        }
        double dNext = Integer.MIN_VALUE;
        if (line < lines.size() - 1) {
            dNext = computeDerivative(lines.get(line + 1));
        }

        // check min
        Line target = null;
        if (dLine >= dPrev && dLine >= dNext) {
            target = lines.get(line);
        } else if (dPrev >= dLine && dPrev >= dNext) {
            target = lines.get(line - 1);
        } else if (dNext >= dLine && dNext >= dPrev) {
            target = lines.get(line + 1);
        } else {
            LOGGER.error("Invalid line {}, dPrev: {}, dLine: {}, dNext: {}, skip GC ", line, dPrev, dLine, dNext);
            return -1;
        }

        Block block = target.poll();
        if (block != null) {
            assert block != null && block.state == State.Used;
            gcBlock(new IntArrayList(), block);
        }
        return target.lineIndex;
    }

    private double computeDerivative(Line line) {
        if (line.numBlocks() <= LINE_MIN_BLOCKS) {
            return Integer.MIN_VALUE;
        }
        return computeDerivativeNumerical(line);
    }

    private double computeDerivativeAnalytical(Line line) {
        double s = line.sizeRatio(this);
        double z = 1 + line.getBeta(this) / s;
        double W = -Math.pow(Math.E, -0.9 * (z - 1));
        MultiLogBlockSelector selector = (MultiLogBlockSelector) blockSelector;

        double freq = 1.0 / selector.getInterval(line.lineIndex);

        double d = freq * W / (s * (W + 1) * (W + z));
        return d;
    }

    private double computeDerivativeNumerical(Line line) {
        double a1 = line.getAlpha();
        if (a1 <= 0.000001) {
            // shouldn't select this line
            return Integer.MIN_VALUE;
        }
        double cost1 = cleanCost(line, a1);
        double a2 = line.getAlpha(this, DELTA);
        double cost2 = cleanCost(line, a2);
        double d = (cost2 - cost1) / DELTA;
        return d;
    }

    private double cleanCost(Line line, double a) {
        double pgc = Math.min(Math.pow(Math.E, -0.9 * a) / (1 + a), 0.99999);
        double updateFrequency = blockSelector.updateFreq(line.lineIndex);
        return updateFrequency * pgc / (1 - pgc);

    }

    @Override
    protected void closeBlock(Block block) {
        super.closeBlock(block);
        lines.get(block.line).add(block);
    }

}
