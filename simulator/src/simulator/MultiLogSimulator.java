package simulator;

public class MultiLogSimulator extends Simulator {

    public MultiLogSimulator(Param param, int maxLpid) {
        super(param, maxLpid);
    }

    @Override
    protected void runGC(Block userBlock) {
        Line line = lines.get(userBlock.line);



    }

}
