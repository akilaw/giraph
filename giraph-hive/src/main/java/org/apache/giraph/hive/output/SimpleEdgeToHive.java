/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.hive.output;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveWritableRecord;

import java.io.IOException;

/**
 * Simple implementation of {@link EdgeToHive} when each {@link Edge} is
 * stored to one row in the output.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public abstract class SimpleEdgeToHive<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    AbstractEdgeToHive<I, V, E> {

  /**
   * Fill the HiveRecord from the Vertex given.
   *
   * @param sourceId    The vertex ID from which the edge originates
   * @param sourceValue The vertex value; the vertex is the one from which
   *                    the edge originates
   * @param edge        Edge which we want to save.
   * @param record      HiveRecord to write to.
   */
  public abstract void fillRecord(I sourceId, V sourceValue, Edge<I, E> edge,
                                  HiveWritableRecord record);

  @Override
  public final void saveEdge(I sourceId, V sourceValue, Edge<I, E> edge,
      HiveWritableRecord reusableRecord,
      HiveRecordSaver recordSaver) throws IOException, InterruptedException {
    fillRecord(sourceId, sourceValue, edge, reusableRecord);
    recordSaver.save(reusableRecord);
  }
}
