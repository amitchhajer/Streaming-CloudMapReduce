/*
* Copyright 2009 Accenture. All rights reserved.
*
* Accenture licenses this file to you under the Apache License, 
* Version 2.0 (the "License"); you may not use this file except in 
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* @author Huan Liu (huanliu AT cs.stanford.edu)
*/ 
package com.acnlabs.CloudMapReduce.mapreduce;

import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;

/** 
 * Maps input key/value pairs to a set of intermediate key/value pairs.  
*/

public interface Mapper {
  
  /** 
   * Maps a single input key/value pair into an intermediate key/value pair.
   * 
   * <p>Output pairs need not be of the same types as input pairs.  A given 
   * input pair may map to zero or many output pairs.  Output pairs are 
   * collected with calls to 
   * {@link OutputCollector#collect(Object,Object)}.</p>
   *
   * @param key the input key.
   * @param value the input value.
   * @param output collects mapped keys and values.
   * @param perf performance tracker to keep track of statistics
   */
  void map(String key, String value, OutputCollector output, PerformanceTracker perf)
  throws Exception;
}
