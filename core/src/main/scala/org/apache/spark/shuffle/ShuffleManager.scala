/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 *
 * shuffle的可插拔接口。shuffleManager在driver上SparkEnv中和每个executor中创建的，基于spark.shuffle.manager
 * 的配置。driver通过它注册shuffles，executors（或是本地模式运行task的driver）可以要求读取和写入数据。
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * 向管理器注册shuffle，并获取一个句柄传递给任务。
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  // 获取相应partition的writer。由map tasks在executors中调用
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   * 获取一系列reduce分区的reader（startPartition到endPartition-1）。
   * 通过reduce任务调用执行器。
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
    * 从shuffle manager中删除shuffle的元数据
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   * 返回一个能够基于块坐标检索shuffle块数据的解析器
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  // 关闭shuffle manager
  def stop(): Unit
}
