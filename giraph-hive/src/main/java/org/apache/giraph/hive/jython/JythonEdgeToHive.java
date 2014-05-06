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
package org.apache.giraph.hive.jython;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.output.SimpleEdgeToHive;
import org.apache.giraph.hive.values.HiveValueWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * A {@link org.apache.giraph.hive.output.EdgeToHive} that writes each part
 * (source vertex ID, target vertex ID, edge value) using separate
 * writers.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class JythonEdgeToHive<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends SimpleEdgeToHive<I, V, E> {

  /** Source ID column name in Hive */
  public static final StrConfOption EDGE_SOURCE_ID_COLUMN =
      new StrConfOption("hive.output.edge.source.id.column", null,
          "Source Vertex ID column");
  /** Target ID column name in Hive */
  public static final StrConfOption EDGE_TARGET_ID_COLUMN =
      new StrConfOption("hive.output.edge.target.id.column", null,
          "Target Vertex ID column");
  /** Edge Value column name in Hive */
  public static final StrConfOption EDGE_VALUE_COLUMN =
      new StrConfOption("hive.output.edge.value.column", null,
          "Edge Value column");

  /** Source ID writer */
  private HiveValueWriter<I> sourceIdWriter;
  /** Target ID writer */
  private HiveValueWriter<I> targetIdWriter;
  /** Edge Value writer */
  private HiveValueWriter<E> edgeValueWriter;

  @Override
  public void checkOutput(HiveOutputDescription outputDesc,
                          HiveTableSchema schema,
                          HiveWritableRecord emptyRecord) { }

  @Override
  public void initialize() {
    HiveTableSchema schema = getTableSchema();
    ImmutableClassesGiraphConfiguration conf = getConf();

    sourceIdWriter = HiveJythonUtils.newValueWriter(schema,
        EDGE_SOURCE_ID_COLUMN, conf, GraphType.VERTEX_ID,
        GiraphHiveConstants.VERTEX_ID_WRITER_JYTHON_NAME);
    targetIdWriter = HiveJythonUtils.newValueWriter(schema,
        EDGE_TARGET_ID_COLUMN, conf, GraphType.VERTEX_ID,
        GiraphHiveConstants.VERTEX_ID_WRITER_JYTHON_NAME);
    edgeValueWriter = HiveJythonUtils.newValueWriter(schema,
        EDGE_VALUE_COLUMN, conf, GraphType.EDGE_VALUE,
        GiraphHiveConstants.EDGE_VALUE_WRITER_JYTHON_NAME);
  }

  @Override
  public void fillRecord(I sourceId, V sourceValue, Edge<I, E> edge,
                         HiveWritableRecord record) {
    sourceIdWriter.write(sourceId, record);
    targetIdWriter.write(edge.getTargetVertexId(), record);
    edgeValueWriter.write(edge.getValue(), record);
  }
}
