/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package simulator;

import java.io.DataInput;
import java.io.IOException;

public class VarLenDataInput implements DataInput {

    private final DataInput input;

    public VarLenDataInput(DataInput input) {
        this.input = input;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readFully(b);

    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        byte b = readByte();
        if (b >= 0)
            return b;
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0)
            return i;
        b = readByte();
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0)
            return i;
        throw new IOException("Invalid vInt detected (too many bits)");
    }

    @Override
    public long readLong() throws IOException {
        byte b = readByte();
        if (b >= 0)
            return b;
        long i = b & 0x7FL;
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 56;
        if (b >= 0)
            return i;
        throw new IOException("Invalid vLong detected (negative values disallowed)");
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

}