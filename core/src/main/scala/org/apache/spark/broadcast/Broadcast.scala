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

package org.apache.spark.broadcast

import java.io.Serializable

import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
 * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
 * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
 * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
 * communication cost.
 *
 * 广播变量:广播变量允许程序员保存每个机器上缓存的只读变量，而不是发送task的副本。Spark还尝试使用高效的广播算法
 * 来分发广播变量以降低通信成本。
 *
 * Broadcast variables are created from a variable `v` by calling
 * [[org.apache.spark.SparkContext#broadcast]].
 * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
 * `value` method. The interpreter session below shows this:
 *
 * 通过调用[[org.apache.spark.SparkContext＃broadcast]]从变量`v`创建广播变量。 广播变量是`v`的包装，
 * 它的值可以通过调用`value`方法来访问。 下面的解释器会话显示：
 *
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 * After the broadcast variable is created, it should be used instead of the value `v` in any
 * functions run on the cluster so that `v` is not shipped to the nodes more than once.
 * In addition, the object `v` should not be modified after it is broadcast in order to ensure
 * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
 * to a new node later).
 *
 * 创建广播变量后，应该在群集上运行的任何函数中使用它而不是值“v”，以使'v'不会多次发送到节点。
 * 此外，对象 'v' 不应该在广播之后被修改，以便确保所有节点获得相同的广播变量值（例如，如果变量以后被发送到一个新节点）。
 *
 * @param id A unique identifier for the broadcast variable.
 * @tparam T Type of the data contained in the broadcast variable.
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * Flag signifying whether the broadcast variable is valid
   * (that is, not already destroyed) or not.
   */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** Get the broadcasted value. */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   * Asynchronously delete cached copies of this broadcast on the executors.
   * If the broadcast is used after this is called, it will need to be re-sent to each executor.
   * 异步删除执行器上的此广播的缓存副本。
   * 如果在调用该广播之后使用广播，则需要将其重新发送给每个执行器。就是说这个变量其实还在Driver上有存储，只是executor中删除
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   * Delete cached copies of this broadcast on the executors. If the broadcast is used after
   * this is called, it will need to be re-sent to each executor.
   * 不是异步
   * @param blocking Whether to block until unpersisting has completed
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * This method blocks until destroy has completed
   * 销毁与此广播变量相关的所有数据和元数据。 请谨慎使用;
   * 一旦广播变量已被销毁，则不能再次使用。
   * 此方法阻塞，直到销毁完成
   */
  def destroy() {
    destroy(blocking = true)
  }

  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * @param blocking Whether to block until destroy has completed
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    doDestroy(blocking)
  }

  /**
   * Whether this Broadcast is actually usable. This should be false once persisted state is
   * removed from the driver.
   * 此广播是否实际可用。 一旦持久状态从Driver中删除，这应该是false。
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * Actually get the broadcasted value. Concrete implementations of Broadcast class must
   * define their own way to get the value.
   * 实际上得到广播的值。 Broadcast类的具体实现必须定义自己的方式来获取值。
   */
  protected def getValue(): T

  /**
   * Actually unpersist the broadcasted value on the executors. Concrete implementations of
   * Broadcast class must define their own logic to unpersist their own data.
   * 删除executors上broadcast的值。 Broadcast类的具体实现必须定义他们自己的逻辑来解开自己的数据。
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * Actually destroy all data and metadata related to this broadcast variable.
   * Implementation of Broadcast class must define their own logic to destroy their own
   * state.
   * 实际上会销毁与此广播变量相关的所有数据和元数据。
   *实现Broadcast类必须定义自己的逻辑来销毁自己的状态。
   */
  protected def doDestroy(blocking: Boolean)

  /** Check if this broadcast is valid. If not valid, exception is thrown. */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  override def toString: String = "Broadcast(" + id + ")"
}
