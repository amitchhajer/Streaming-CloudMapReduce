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
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 */
public interface Reducer <T> {
  
  /** 
   * Called before processing a key. Used to initialize state
   * @param key the key.
   * @param output in case need to produce key/value pair
   */
  T start(String key, OutputCollector output)
  	throws Exception;
	
  /** 
   * @param key the key.
   * @param value next value int the list of values to reduce.
   * @param state the current state for the reducer.
   * @param output to collect keys and combined values.
   * @param perf performance tracker to keep track of statistics
   */
  void next(String key, String value, T state, OutputCollector output, PerformanceTracker perf)
    throws Exception;

  /** 
   * Called after processing a key. Used to do necessary clean up, if any.
   * @param key the key.
   * @param state the current state variable
   * @param output in case need to produce key/value pair
   */
  void complete(String key, T state, OutputCollector output)
  	throws Exception;
  
  /** 
   * For convenience of querying
   * @param state the current state variable
   */
  long getSize(T state)
  	throws Exception;
}
