package simulator;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

class TraceOperation {
    byte op;
    int file;
    int page;
    int length;
}

public class TraceReader {

    public static final byte WRITE = 1;
    public static final byte DELETE = 2;

    private final BufferedInputStream stream;
    private final VarLenDataInput input;

    public TraceReader(String file) throws Exception {
        stream = new BufferedInputStream(new FileInputStream(file));
        input = new VarLenDataInput(new DataInputStream(stream));
    }

    public boolean read(TraceOperation operation) throws IOException {
        try {
            operation.op = input.readByte();
            if (operation.op == WRITE) {
                operation.file = input.readInt();
                operation.page = input.readInt();
            } else if (operation.op == DELETE) {
                operation.file = input.readInt();
            } else {
                throw new IllegalStateException("Unknown operation " + operation.op);
            }
            return true;
        } catch (EOFException e) {
            stream.close();
            return false;
        }
    }

    public void close() throws IOException {
        stream.close();
    }

    public static void main(String[] args) {
    }

}
