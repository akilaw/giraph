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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;


/**
 * Implemention of shims against Hadoop 0.20 with Security.
 */
public class Hadoop20SShims extends HadoopShimsSecure {

  @Override
  public Configuration getConfiguration(org.apache.hadoop.mapreduce.JobContext context) {
    return context.getConfiguration();
  }

  @Override
  public TaskAttemptContext makeTaskAttemptContext(Configuration conf,
                                                  TaskAttemptID taskAttemptID) {
    TaskAttemptContext context;
    context = new TaskAttemptContext(conf, taskAttemptID);
    return context;
  }

  @Override
  public JobContext makeJobContext(Configuration conf, JobID jobID) {
    JobContext context;
    context = new JobContext(conf, jobID);
    return context;
  }
}
