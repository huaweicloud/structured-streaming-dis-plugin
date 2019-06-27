package org.apache.spark.sql.dis.source

import java.io._
import java.nio.charset.StandardCharsets
import java.{util => ju}

import com.huaweicloud.dis.adapter.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import com.huaweicloud.dis.adapter.kafka.clients.consumer.{Consumer, DISKafkaConsumer}
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArrayDeserializer
import com.huaweicloud.dis.exception.DISSequenceNumberOutOfRangeException
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.dis.source.DISKafkaSource._
import org.apache.spark.sql.dis._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.UninterruptibleThread

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * A [[Source]] that uses Kafka's own [[DISKafkaConsumer]] API to reads data from Kafka. The design
  * for this source is as follows.
  *
  * - The [[DISKafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
  *   a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
  *   example if the last record in a Kafka topic "t", partition 2 is offset 5, then
  *   DISKafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
  *   with the semantics of `KafkaConsumer.position()`.
  *
  * - The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
  *   by this source. These strategies directly correspond to the different consumption options
  *   in . This class is designed to return a configured [[DISKafkaConsumer]] that is used by the
  *   [[DISKafkaSource]] to query for the offsets. See the docs on
  *   [[org.apache.spark.sql.dis.source.DISKafkaSource.ConsumerStrategy]] for more details.
  *
  * - The [[DISKafkaSource]] written to do the following.
  *
  *  - As soon as the source is created, the pre-configured KafkaConsumer returned by the
  *    [[ConsumerStrategy]] is used to query the initial offsets that this source should
  *    start reading from. This used to create the first batch.
  *
  *   - `getOffset()` uses the KafkaConsumer to query the latest available offsets, which are
  *     returned as a [[DISKafkaSourceOffset]].
  *
  *   - `getBatch()` returns a DF that reads from the 'start offset' until the 'end offset' in
  *     for each partition. The end offset is excluded to be consistent with the semantics of
  *     [[DISKafkaSourceOffset]] and `KafkaConsumer.position()`.
  *
  *   - The DF returned is based on [[DISKafkaSourceRDD]] which is constructed such that the
  *     data from Kafka topic + partition is consistently read by the same executors across
  *     batches, and cached KafkaConsumers in the executors can be reused efficiently. See the
  *     docs on [[DISKafkaSourceRDD]] for more details.
  *
  * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
  * must make sure all messages in a topic have been processed when deleting a topic.
  *
  * There is a known issue caused by KAFKA-1894: the query using DISKafkaSource maybe cannot be stopped.
  * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
  * and not use wrong broker addresses.
  */
private[source] case class DISKafkaSource(
                                          sqlContext: SQLContext,
                                          consumerStrategy: ConsumerStrategy,
                                          executorKafkaParams: ju.Map[String, Object],
                                          sourceOptions: Map[String, String],
                                          metadataPath: String,
                                          startingOffsets: StartingOffsets,
                                          failOnDataLoss: Boolean)
  extends Source with MyLogging {

  private val sc = sqlContext.sparkContext

  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    sc.conf.getTimeAsMs("spark.network.timeout", "120s").toString
  ).toLong

  private val maxOffsetFetchAttempts =
    sourceOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private val offsetFetchAttemptIntervalMs =
    sourceOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  private val maxOffsetsPerTrigger =
    sourceOptions.get("maxOffsetsPerTrigger").map(_.toLong)

  /**
    * A KafkaConsumer used in the driver to query the latest Kafka offsets. This only queries the
    * offsets and never commits them.
    */
  private val consumer = consumerStrategy.createConsumer()

  /**
    * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
    * called in StreamExecutionThread. Otherwise, interrupting a thread while running
    * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
    */
  private lazy val initialPartitionOffsets = {
    myLogInfo(s"init offset metadataPath: $metadataPath")
    val metadataLog =
      new HDFSMetadataLog[DISKafkaSourceOffset](sqlContext.sparkSession, metadataPath) {
        override def serialize(metadata: DISKafkaSourceOffset, out: OutputStream): Unit = {
          val bytes = metadata.json.getBytes(StandardCharsets.UTF_8)
          out.write(bytes.length)
          out.write(bytes)
        }

        override def deserialize(in: InputStream): DISKafkaSourceOffset = {
          val length = in.read()
          val bytes = new Array[Byte](length)
          in.read(bytes)
          DISKafkaSourceOffset(SerializedOffset(new String(bytes, StandardCharsets.UTF_8)))
        }
      }

    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsets => DISKafkaSourceOffset(fetchEarliestOffsets())
        case LatestOffsets => DISKafkaSourceOffset(fetchLatestOffsets())
        case SpecificOffsets(p) => DISKafkaSourceOffset(fetchSpecificStartingOffsets(p))
      }
      metadataLog.add(0, offsets)
      myLogInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  private var currentPartitionOffsets: Option[Map[TopicPartition, Long]] = None

  override def schema: StructType = DISKafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    val startMS = System.currentTimeMillis()
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val latest = fetchLatestOffsets()
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest
      case Some(limit) if currentPartitionOffsets.isEmpty =>
        rateLimit(limit, initialPartitionOffsets, latest)
      case Some(limit) =>
        rateLimit(limit, currentPartitionOffsets.get, latest)
    }

    currentPartitionOffsets = Some(offsets)
    myLogDebug(s"GetOffset: ${offsets.toSeq.map(_.toString).sorted}, cost ${System.currentTimeMillis() - startMS}ms")
    Some(DISKafkaSourceOffset(offsets))
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
                         limit: Long,
                         from: Map[TopicPartition, Long],
                         until: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val fromNew = fetchNewPartitionEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          myLogInfo(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.get(tp).getOrElse(fromNew(tp))
            val prorate = limit * (size / total)
            myLogInfo(s"rateLimit $tp prorated amount is $prorate")
            // Don't completely starve small topicpartitions
            val off = begin + (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            myLogInfo(s"rateLimit $tp new offset is $off")
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  override def commit(end: Offset): Unit = {
  //    myLogInfo(s"Commit ${end}")
  //    val h = DISKafkaSourceOffset.getPartitionOffsets(end).map { case (k, v) => (k, new OffsetAndMetadata(v)) }
  //    myLogInfo(h.asJava.toString)
  //    consumer.commitSync(new ju.HashMap[TopicPartition, OffsetAndMetadata](h.asJava))
  }
  /**
    * Returns the data that is between the offsets
    * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
    * exclusive.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    myLogDebug(s"getBatch ${start} ~ ${end}")
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    myLogInfo(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = DISKafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        DISKafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionOffsets = fetchNewPartitionEarliestOffsets(newPartitions.toSeq)
    if (newPartitionOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    myLogInfo(s"Partitions added: $newPartitionOffsets")
    newPartitionOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    // !!!!!!!!!!!!!
    myLogInfo("TopicPartitions: " + topicPartitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    myLogInfo("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = topicPartitions.map { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        newPartitionOffsets.getOrElse(tp, {
          // This should not happen since newPartitionOffsets contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset")
        })
      }
      val untilOffset = untilPartitionOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        // This allows cached KafkaConsumers in the executors to be re-used to read the same
        // partition in every batch.
        Some(sortedExecutors(floorMod(tp.hashCode, numExecutors)))
      } else None
      DISKafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        reportDataLoss(s"Partition ${range.topicPartition}'s offset was changed from " +
          s"${range.fromOffset} to ${range.untilOffset}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new DISKafkaSourceRDD(
      sc, executorKafkaParams, offsetRanges, pollTimeoutMs, failOnDataLoss).map { cr =>
      InternalRow(
        cr.key,
        cr.value,
        UTF8String.fromString(cr.topic),
        cr.partition,
        cr.offset,
        DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(cr.timestamp)),
        cr.timestampType.id)
    }

    myLogInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = Some(untilPartitionOffsets)
    }

    val ru = scala.reflect.runtime.universe
    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    val sqlContextClass = classMirror.reflect(sqlContext)

    val methods = ru.typeOf[SQLContext]
    val internalCreateDataFrameMethod = methods.decl(ru.TermName(s"internalCreateDataFrame")).asMethod

    val result = {
      if (org.apache.spark.SPARK_VERSION.compareTo("2.3") >= 0) {
        sqlContextClass.reflectMethod(internalCreateDataFrameMethod)(rdd, schema, true)
      }
      else {
        sqlContextClass.reflectMethod(internalCreateDataFrameMethod)(rdd, schema)
      }
    }
    result.asInstanceOf[org.apache.spark.sql.DataFrame]
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    consumer.close()
  }

  override def toString(): String = s"DISKafkaSource[$consumerStrategy]"

  /**
    * Set consumer position to specified offsets, making sure all assignments are set.
    */
  private def fetchSpecificStartingOffsets(
                                            partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val result = withRetriesWithoutInterrupt {
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      assert(partitions.asScala == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions.asScala}")
      myLogInfo(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")

      partitionOffsets.foreach {
        case (tp, -1) => consumer.seekToEnd(ju.Arrays.asList(tp))
        case (tp, -2) => consumer.seekToBeginning(ju.Arrays.asList(tp))
        case (tp, off) => consumer.seek(tp, off)
      }
      partitionOffsets.map {
        case (tp, _) => tp -> consumer.position(tp)
      }
    }
    partitionOffsets.foreach {
      case (tp, off) if off != -1 && off != -2 =>
        if (result(tp) != off) {
          reportDataLoss(
            s"startingOffsets for $tp was $off but consumer reset to ${result(tp)}")
        }
      case _ =>
      // no real way to check that beginning or end is reasonable
    }
    result
  }

  /**
    * Fetch the earliest offsets of partitions.
    */
  private def fetchEarliestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    myLogDebug(s"Partitions assigned to consumer: $partitions. Seeking to the beginning")

    consumer.seekToBeginning(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    myLogDebug(s"Got earliest offsets for partition : $partitionOffsets")
    partitionOffsets
  }
  
  /**
    * Fetch the latest offset of partitions.
    */
  private def fetchLatestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    myLogDebug(s"Partitions assigned to consumer: $partitions. Seeking to the end.")

    consumer.seekToEnd(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    myLogDebug(s"Got latest offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  /**
    * Fetch the earliest offsets for newly discovered partitions. The return result may not contain
    * some partitions if they are deleted.
    */
  private def fetchNewPartitionEarliestOffsets(
                                                newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] =
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      withRetriesWithoutInterrupt {
        // Poll to get the latest assigned partitions
        consumer.poll(0)
        val partitions = consumer.assignment()
        consumer.pause(partitions)
        myLogDebug(s"\tPartitions assigned to consumer: $partitions")

        // Get the earliest offset of each partition
        consumer.seekToBeginning(partitions)
        val partitionOffsets = newPartitions.filter { p =>
          // When deleting topics happen at the same time, some partitions may not be in
          // `partitions`. So we need to ignore them
          partitions.contains(p)
        }.map(p => p -> consumer.position(p)).toMap
        myLogInfo(s"Got earliest offsets for new partitions: $partitionOffsets")
        partitionOffsets
      }
    }

  /**
    * Helper function that does multiple retries on the a body of code that returns offsets.
    * Retries are needed to handle transient failures. For e.g. race conditions between getting
    * assignment and getting position while topics/partitions are deleted can cause NPEs.
    *
    * This method also makes sure `body` won't be interrupted to workaround a potential issue in
    * `KafkaConsumer.poll`. (KAFKA-1894)
    */
  private def withRetriesWithoutInterrupt(
                                           body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // Make sure `KafkaConsumer.poll` won't be interrupted (KAFKA-1894)
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        Thread.currentThread match {
          case ut: UninterruptibleThread =>
            // "KafkaConsumer.poll" may hang forever if the thread is interrupted (E.g., the query
            // is stopped)(KAFKA-1894). Hence, we just make sure we don't interrupt it.
            //
            // If the broker addresses are wrong, or Kafka cluster is down, "KafkaConsumer.poll" may
            // hang forever as well. This cannot be resolved in DISKafkaSource until Kafka fixes the
            // issue.
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case x: DISSequenceNumberOutOfRangeException =>
                  reportDataLoss(x.getMessage)
                case NonFatal(e) =>
                  lastException = e
                  myLogWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
              }
            }
          case _ =>
            throw new IllegalStateException(
              "Kafka APIs must be executed on a o.a.spark.util.UninterruptibleThread")
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException()
      }
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }

  /**
    * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
    * Otherwise, just log a warning.
    */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      myLogWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }
}


/** Companion object for the [[DISKafkaSource]]. */
private[source] object DISKafkaSource {

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
    """.stripMargin

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))

  sealed trait ConsumerStrategy {
    def createConsumer(): Consumer[Array[Byte], Array[Byte]]
  }

  case class AssignStrategy(partitions: Array[TopicPartition], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new DISKafkaConsumer[Array[Byte], Array[Byte]](
        DISKafkaSourceProvider.getDISConfig(kafkaParams),
        classOf[ByteArrayDeserializer].newInstance,
        classOf[ByteArrayDeserializer].newInstance)
      consumer.assign(ju.Arrays.asList(partitions: _*))
      consumer
    }

    override def toString: String = s"Assign[${partitions.mkString(", ")}]"
  }

  case class SubscribeStrategy(topics: Seq[String], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new DISKafkaConsumer[Array[Byte], Array[Byte]](
        DISKafkaSourceProvider.getDISConfig(kafkaParams),
        classOf[ByteArrayDeserializer].newInstance,
        classOf[ByteArrayDeserializer].newInstance)
      consumer.subscribe(topics.asJava)
      consumer
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }

  case class SubscribePatternStrategy(
                                       topicPattern: String, kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy {
    override def createConsumer(): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new DISKafkaConsumer[Array[Byte], Array[Byte]](
        DISKafkaSourceProvider.getDISConfig(kafkaParams),
        classOf[ByteArrayDeserializer].newInstance,
        classOf[ByteArrayDeserializer].newInstance)
      consumer.subscribe(
        ju.regex.Pattern.compile(topicPattern),
        new NoOpConsumerRebalanceListener())
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }

  private def getSortedExecutorList(sc: SparkContext): Array[String] = {
    val bm = sc.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
  }

  private def floorMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b
}