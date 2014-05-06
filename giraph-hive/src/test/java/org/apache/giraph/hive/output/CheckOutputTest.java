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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.output.examples.HiveOutputIntNullEdge;
import org.apache.giraph.hive.output.examples.HiveOutputIntIntVertex;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.TestSchema;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;

import static com.facebook.hiveio.record.HiveRecordFactory.newWritableRecord;
import static org.junit.Assert.assertNull;

public class CheckOutputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testCheckEdge() throws Exception {
    EdgeToHive edgeToHive = new HiveOutputIntNullEdge();
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    HiveTableSchema schema = TestSchema.builder()
        .addColumn("foo", HiveType.LONG)
        .addColumn("bar", HiveType.LONG)
        .addColumn("baz", HiveType.DOUBLE)
        .build();
    edgeToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));

    schema = TestSchema.builder()
        .addColumn("foo", HiveType.INT)
        .addColumn("bar", HiveType.LONG)
        .addColumn("baz", HiveType.DOUBLE)
        .build();
    checkEdgeThrows(edgeToHive, outputDesc, schema);
  }

  private void checkEdgeThrows(EdgeToHive edgeToHive,
                                 HiveOutputDescription outputDesc, HiveTableSchema schema) {
    try {
      edgeToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }


  @Test
  public void testCheckVertex() throws Exception {
    VertexToHive vertexToHive = new HiveOutputIntIntVertex();
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    HiveTableSchema schema = TestSchema.builder()
        .addColumn("foo", HiveType.LONG)
        .addColumn("bar", HiveType.LONG)
        .build();
    vertexToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));

    schema = TestSchema.builder()
            .addColumn("foo", HiveType.INT)
            .addColumn("bar", HiveType.LONG)
            .build();
    checkVertexThrows(vertexToHive, outputDesc, schema);
  }

  private void checkVertexThrows(VertexToHive vertexToHive,
      HiveOutputDescription outputDesc, HiveTableSchema schema) {
    try {
      vertexToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testCheckFailsJob() throws Exception {
    String vertexTableName = "test1Vertex";
    hiveServer.createTable("CREATE TABLE " + vertexTableName +
       " (i1 INT, i2 BIGINT) ");

    String edgeTableName = "test1Edge";
    hiveServer.createTable("CREATE TABLE " + edgeTableName +
        " (i1 BIGINT, i2 BIGINT, d3 DOUBLE) ");

    GiraphConfiguration conf = new GiraphConfiguration();
    String[] edges = new String[] {
        "1 2",
        "2 3",
        "2 4",
        "4 1"
    };

    GiraphHiveConstants.HIVE_EDGE_OUTPUT_TABLE.set(conf, edgeTableName);
    GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE.set(conf, vertexTableName);
    GiraphHiveConstants.EDGE_TO_HIVE_CLASS.set(conf,HiveOutputIntNullEdge.class);
    GiraphHiveConstants.VERTEX_TO_HIVE_CLASS.set(conf, HiveOutputIntIntVertex.class);

    conf.setComputationClass(ComputationCountEdges.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(HiveVertexOutputFormat.class);
    conf.setEdgeOutputFormatClass(HiveEdgeOutputFormat.class);
    try {
      Iterable<String> result = InternalVertexRunner.run(conf, null, edges);
      assertNull(result);
    } catch (IllegalArgumentException e) { }
  }
}
