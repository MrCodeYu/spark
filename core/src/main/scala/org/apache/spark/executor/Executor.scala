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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{AccumulableInfo, DirectTaskResult, IndirectTaskResult, Task}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  // Start worker thread pool
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")
  private val executorSource = new ExecutorSource(threadPool, executorId)

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  startDriverHeartbeater()

  def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    // 对于每一个task，创建一个TaskRunner对象；每一个线程运行一个task
    // TaskRunner继承的是Java Runnable接口
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    // 将taskRunner放到 runningTasks 内存缓存
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean) : Unit = {
    // kill all the running tasks
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner != null) {
        taskRunner.kill(interruptThread)
      }
    }
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  /**
    * 从TaskRunner开始就是Task的运行原理
    */
  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskId: Long,
      val attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer)
    extends Runnable {

    /** Whether this task has been killed. */
    @volatile private var killed = false

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread)
          }
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
    }

    override def run(): Unit = {
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        // 对序列化的task进行反序列化
        val (taskFiles, taskJars, taskProps, taskBytes) =
          Task.deserializeWithDependencies(serializedTask)

        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskProps)
        // 然后通过网络通信，将需要的文件，资源，jar都拷贝过来
        updateDependencies(taskFiles, taskJars)
        // 最后通过反序列化操作，将task的数据反序列化出来
        // 为什么要用类加载器？因为类加载器在这里可以实现很多功能，例如通过反射来加载一个类，然后创建这个类对象
        // 或者可以指定上下文的相关资源，进行加载和读取
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskProps
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        // 运行实际任务并测量其运行时间
        // taskStart：task开始的时间
        taskStart = System.currentTimeMillis()
        var threwException = true
        // 这里是最关键的地方，用的是task的run方法
        //
        val value = try {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }
        }
        // taskFinish：task结束时间
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        // 对MapStatus进行了序列化和封装，之后要将其发送给Driver（通过网络）
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        // 在这里计算了task的merics，就是统计信息，
        // 包括了：task运行了多长时间，反序列化用了多长时间，Java GC用了多长时间，结果反序列化用了多长时间
        // 这些会在spark ui上显示。
        // 真正在企业中运行长时间运行的spark程序时，可以在4040去看情况。
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        // 在这里调用了Executor所在的CoarseGrainedExecutorBackend的statusUpdate方法，通知
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskEndReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException | _: InterruptedException if task.killed =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskEndReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // Collect latest accumulator values to report back to the driver
          val accums: Seq[AccumulatorV2[_, _]] =
            if (task != null) {
              task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
              task.collectAccumulatorUpdates(taskFailed = true)
            } else {
              Seq.empty
            }

          val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
            }
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    // 这里用了synchronized保证多线程下的线程安全问题，
    // 因为task是以线程的方式运行在CoarseGrainedExecutorBackend这个进程中并发执行的，
    // 当他们访问一些共享的数据时，可能会出现线程安全的问题，
    // spark选择使用线程同步 synchronized

    // 为什么在这里要进行synchronized？
    // 因为在这里访问了currentFiles等，这类共享的资源
    synchronized {
      // Fetch missing dependencies
      // 通过网络从网络拉取需要的依赖文件
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      // 遍历要拉取的jar
      for ((name, timestamp) <- newJars) {
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
