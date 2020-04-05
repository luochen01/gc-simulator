package simulator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import simulator.Block.State;

public class MultiLogSimulator extends Simulator {

    private static final int LINE_MIN_BLOCKS = 64;

    private static final double DELTA = 0.0001;

    public MultiLogSimulator(Param param, int maxLpid) {
        super(param, maxLpid);
    }

    @Override
    protected void runGC(int line) {
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
            throw new IllegalStateException();
        }

        Block block = target.poll();
        assert block != null && block.state == State.Used;
        int nextLine = block.line;
        gcBlock(new IntArrayList(), block);
        if (freeBlocks.size() <= GC_TRIGGER_BLOCKS) {
            runGC(nextLine + 1);
        }
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
        double cost1 = cleanCost(a1);
        double a2 = line.getAlpha(this, DELTA);
        double cost2 = cleanCost(a2);
        double d = (cost2 - cost1) / DELTA;
        return d;
    }

    private double cleanCost(double a) {
        return Math.pow(Math.E, -0.9 * a) / (1 + a);
    }

    @Override
    protected void closeBlock(Block block) {
        super.closeBlock(block);
        lines.get(block.line).add(block);
    }

}
