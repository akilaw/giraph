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
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.io.EdgeWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.facebook.hiveio.record.HiveRecordFactory;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import java.io.IOException;

/**
 * Vertex writer using Hive.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class HiveEdgeWriter<I extends WritableComparable, V extends Writable,
    E extends Writable>
    extends EdgeWriter<I, V, E> implements HiveRecordSaver {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveEdgeWriter.class);
  /** Underlying Hive RecordWriter used */
  private RecordWriter<WritableComparable, HiveWritableRecord> hiveRecordWriter;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;
  /** Reusable {@link HiveRecord} */
  private HiveWritableRecord reusableRecord;
  /** User class to write vertices from a HiveRecord */
  private EdgeToHive<I, V, E> edgeToHive;

  /**
   * Get underlying Hive record writer used.
   *
   * @return RecordWriter for Hive.
   */
  public RecordWriter<WritableComparable, HiveWritableRecord> getBaseWriter() {
    return hiveRecordWriter;
  }

  /**
   * Set underlying Hive record writer used.
   *
   * @param hiveRecordWriter RecordWriter to write to Hive.
   */
  public void setBaseWriter(
      RecordWriter<WritableComparable, HiveWritableRecord> hiveRecordWriter) {
    this.hiveRecordWriter = hiveRecordWriter;
  }

  /**
   * Get Hive table schema for table being read from.
   *
   * @return Hive table schema for table
   */
  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  /**
   * Set Hive schema for table being read from.
   *
   * @param tableSchema Hive table schema
   */
  public void setTableSchema(HiveTableSchema tableSchema) {
    this.tableSchema = tableSchema;
    reusableRecord = HiveRecordFactory.newWritableRecord(tableSchema);
  }

  @Override
  public void initialize(TaskAttemptContext context)
    throws IOException, InterruptedException {
    edgeToHive = HiveUtils.newEdgeToHive(getConf(), tableSchema);
    edgeToHive.initialize();
  }

  @Override
  public void writeEdge(I sourceId, V sourceValue, Edge<I, E> edge)
    throws IOException, InterruptedException {
    edgeToHive.saveEdge(sourceId, sourceValue, edge, reusableRecord, this);
  }

  @Override
  public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
    hiveRecordWriter.close(context);
  }

  @Override
  public void save(HiveWritableRecord record) throws IOException,
      InterruptedException {
    hiveRecordWriter.write(NullWritable.get(), record);
  }
}
