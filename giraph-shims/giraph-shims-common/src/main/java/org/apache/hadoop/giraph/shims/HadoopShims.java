/**
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
package org.apache.hadoop.giraph.shims;


import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * In order to be compatible with multiple versions of Hadoop, all parts
 * of the Hadoop interface that are not cross-version compatible are
 * encapsulated in an implementation of this class. Users should use
 * the ShimLoader class as a factory to obtain an implementation of
 * HadoopShims corresponding to the version of Hadoop currently on the
 * classpath.
 */
public interface HadoopShims {

  static final Logger LOG = Logger.getLogger(HadoopShims.class);

  /**
   * Get configuration from JobContext
   */
  public Configuration getConfiguration(JobContext context);

  public OutputCommitter getNullOutputCommitter();

  /**
   * Create a TaskAttemptContext, supporting many Hadoops.
   *
   * @param conf Configuration
   * @param taskAttemptID TaskAttemptID to use
   * @return TaskAttemptContext
   */
  public TaskAttemptContext makeTaskAttemptContext(Configuration conf,
                                                   TaskAttemptID taskAttemptID);

  /**
   * Create a JobContext, supporting many Hadoops.
   *
   * @param conf Configuration
   * @param jobID JobID to use
   * @return JobContext
   */
  public JobContext makeJobContext(Configuration conf, JobID jobID);

  /**
   * Get tokens for all the required FileSystems
   * @param job The job
   * @param dirs list of vertex/edge input paths
   */
  public void obtainTokensForNamenodes(JobContext job, Path[] dirs)
      throws IOException;

  /**
   * Return true if the hadoop configuration has security enabled
   * @return
   */
  public boolean isSecurityEnabled();
}
