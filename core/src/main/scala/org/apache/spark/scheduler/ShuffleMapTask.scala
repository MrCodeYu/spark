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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
  * 一个ShuffleMapTask会将RDD的元素，切分为多个buckets（基于ShuffleDependency中指定的Partitioner）
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param metrics a [[TaskMetrics]] that is created at driver side and sent to executor side.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    metrics: TaskMetrics,
    localProperties: Properties)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null, new Properties)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  /**
    * 非常重要的一点：ShuffleMapTask的runTask()有MapStatus这个返回值
    */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    // 对于task要处理的数据进行反序列化，
    // 因为多个task可能是并发运行在多个Executor上的，可能都不在同一台机器上，
    // 但是，一个stage的task其实要处理的RDD是一样的，
    // 这些数据是task通过广播变量(broadcast variable)拿到这些数据
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取 ShuffleManager，从shuffleManager中获取shuffle writer
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 首先调用了rdd的iterator(并且传入了当前task要处理哪个partititon)，所以，核心逻辑就在rdd的iterator()中，
      // 在这里就实现了针对RDD的某个partition执行我们自己定义的算子，或者是函数。
      // 执行完了我们定义的算子或函数，相当于是针对rdd的partition进行了处理，处理之后的结果数据都是通过ShuffleWriter，
      // 经过HashPartitioner分区之后，写入自己对应的分区bucket
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // 最后返回结果
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
