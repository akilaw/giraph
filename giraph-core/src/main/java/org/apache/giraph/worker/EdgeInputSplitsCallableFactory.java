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

package org.apache.giraph.worker;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Factory for {@link EdgeInputSplitsCallable}s.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class EdgeInputSplitsCallableFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements CallableFactory<VertexEdgeCount> {
  /** Edge input format */
  private final EdgeInputFormat<I, E> edgeInputFormat;
  /** Mapper context. */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Configuration. */
  private final ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** {@link BspServiceWorker} we're running on. */
  private final BspServiceWorker<I, V, E> bspServiceWorker;
  /** Handler for input splits */
  private final InputSplitsHandler splitsHandler;
  /** {@link ZooKeeperExt} for this worker. */
  private final ZooKeeperExt zooKeeperExt;

  /**
   * Constructor.
   *
   * @param edgeInputFormat Edge input format
   * @param context Mapper context
   * @param configuration Configuration
   * @param bspServiceWorker Calling {@link BspServiceWorker}
   * @param splitsHandler Handler for input splits
   * @param zooKeeperExt {@link ZooKeeperExt} for this worker
   */
  public EdgeInputSplitsCallableFactory(
      EdgeInputFormat<I, E> edgeInputFormat,
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E> configuration,
      BspServiceWorker<I, V, E> bspServiceWorker,
      InputSplitsHandler splitsHandler,
      ZooKeeperExt zooKeeperExt) {
    this.edgeInputFormat = edgeInputFormat;
    this.context = context;
    this.configuration = configuration;
    this.bspServiceWorker = bspServiceWorker;
    this.zooKeeperExt = zooKeeperExt;
    this.splitsHandler = splitsHandler;
  }

  @Override
  public InputSplitsCallable<I, V, E> newCallable(int threadId) {
    return new EdgeInputSplitsCallable<I, V, E>(
        edgeInputFormat,
        context,
        configuration,
        bspServiceWorker,
        splitsHandler,
        zooKeeperExt);
  }
}
