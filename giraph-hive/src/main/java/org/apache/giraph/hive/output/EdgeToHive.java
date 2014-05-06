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

import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import java.io.IOException;

/**
 * Interface for writing edges to Hive.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public interface EdgeToHive<I extends WritableComparable, V extends Writable,
    E extends Writable> {
  /**
   * User initialization before any saveEdge() calls but after Configuration
   * and HiveTableSchema are guaranteed to be set.
   */
  void initialize();

  /**
   * Check the output is valid. This method provides information to the user as
   * early as possible so that they may validate they are using the correct
   * input and fail the job early rather than getting into it and waiting a long
   * time only to find out something was misconfigured.
   *
   * @param outputDesc  HiveOutputDescription
   * @param schema      Hive table schema
   * @param emptyRecord Example Hive record that you can write to as if you were
   *                    writing a Vertex. This record will check column types.
   */
  void checkOutput(HiveOutputDescription outputDesc, HiveTableSchema schema,
                   HiveWritableRecord emptyRecord);

  /**
   * Save edge to the output. One edge can be stored to multiple rows in
   * the output.
   * <p/>
   * Record you get here is reusable, and the protocol to follow is:
   * - fill the reusableRecord with your data
   * - call recordSaver.save(reusableRecord)
   * - repeat
   * If you don't call save() at all then there won't be any output for this
   * edge.
   *
   * @param sourceId       The vertex ID from which the edge originates
   * @param sourceValue    The vertex value; the vertex is the one from which
   *                       the edge originates
   * @param edge           Edge which we want to save.
   * @param reusableRecord Record to use for writing data to it.
   * @param recordSaver    Saver of records
   */
  void saveEdge(I sourceId, V sourceValue, Edge<I, E> edge,
                HiveWritableRecord reusableRecord, HiveRecordSaver recordSaver)
    throws IOException, InterruptedException;
}
