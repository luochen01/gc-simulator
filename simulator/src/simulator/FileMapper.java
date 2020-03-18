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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.FastEntrySet;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class FileMapper {

    private final IntArrayFIFOQueue lpidQueue;
    private final int numLpids;

    private final Simulator sim;

    // file id -> (page id -> lpid)
    private final Int2ObjectMap<Int2IntMap> fileMap = new Int2ObjectOpenHashMap<>();

    public FileMapper(int numLpids, Simulator sim) {
        this.sim = sim;
        this.numLpids = numLpids;
        this.lpidQueue = new IntArrayFIFOQueue(numLpids);
        for (int i = 1; i < numLpids; i++) {
            this.lpidQueue.enqueue(i);
        }
    }

    public int write(int file, int page) {
        Int2IntMap pageMap = fileMap.computeIfAbsent(file, k -> new Int2IntOpenHashMap());
        int lpid = pageMap.getOrDefault(page, -1);
        if (lpid == -1) {
            lpid = lpidQueue.dequeueInt();
            pageMap.put(page, lpid);
        }
        return lpid;
    }

    public void delete(int file) {
        Int2IntMap pageMap = fileMap.remove(file);
        if (pageMap == null) {
            throw new IllegalStateException("Cannot find file " + file);
        }

        ObjectIterator<Int2IntMap.Entry> it = ((FastEntrySet) pageMap.int2IntEntrySet()).fastIterator();
        while (it.hasNext()) {
            int lpid = it.next().getIntValue();
            lpidQueue.enqueue(lpid);
            sim.delete(lpid);
        }
    }

    public int getFiles() {
        return fileMap.size();
    }

    public int getUsedLpids() {
        return numLpids - lpidQueue.size();
    }

}