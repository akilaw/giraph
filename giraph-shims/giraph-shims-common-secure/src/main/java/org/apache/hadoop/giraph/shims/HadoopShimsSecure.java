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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.log4j.Logger;


/**
 * Base implementation for shims against secure Hadoop 0.20.3/0.23.
 */
public abstract class HadoopShimsSecure implements HadoopShims {

  static final Logger LOG = Logger.getLogger(HadoopShimsSecure.class);

  public static class NullOutputCommitter extends OutputCommitter {

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
        throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
    }
  }

  @Override
  public OutputCommitter getNullOutputCommitter() {
    return new NullOutputCommitter();
  }

  @Override
  public void obtainTokensForNamenodes(
      org.apache.hadoop.mapreduce.JobContext job, Path[] dirs)
      throws IOException {
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
        job.getConfiguration());
  }

  @Override
  public boolean isSecurityEnabled() { return true;}
}
