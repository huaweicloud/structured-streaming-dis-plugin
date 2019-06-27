package org.apache.spark.sql.dis.source

import java.util.concurrent.TimeoutException
import java.{util => ju}

import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig
import com.huaweicloud.dis.adapter.kafka.clients.consumer.{ConsumerRecord, DISKafkaConsumer}
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArrayDeserializer
import com.huaweicloud.dis.exception.DISSequenceNumberOutOfRangeException
import org.apache.spark.sql.dis.source.DISKafkaSource._
import org.apache.spark.sql.dis.{BackOffExecution, ExponentialBackOff, MyLogging}
import org.apache.spark.{SparkEnv, SparkException, TaskContext}

import scala.collection.JavaConverters._


/**
  * Consumer of single topicpartition, intended for cached reuse.
  * Underlying consumer is not threadsafe, so neither is this,
  * but processing the same topicpartition and group id in multiple threads is usually bad anyway.
  */
private[source] case class CachedDISKafkaConsumer private(
                                                           topicPartition: TopicPartition,
                                                           kafkaParams: ju.Map[String, Object]) extends MyLogging {

  import CachedDISKafkaConsumer._

  private val groupId = kafkaParams.get(DisConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

  private var consumer = createConsumer

  private val initialInterval = 100

  private val maxInterval = 5 * 1000L

  private val maxElapsedTime = Long.MaxValue

  private val multiplier = 1.5

  private var backOff: ExponentialBackOff = {
    val b = new ExponentialBackOff(initialInterval, multiplier)
    b.setMaxInterval(maxInterval)
    b.setMaxElapsedTime(maxElapsedTime)
    b
  }

  /** Iterator to the already fetch data */
  private var fetchedData = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
  private var nextOffsetInFetchedData = UNKNOWN_OFFSET

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer: DISKafkaConsumer[Array[Byte], Array[Byte]] = {
    val c = new DISKafkaConsumer[Array[Byte], Array[Byte]](
      DISKafkaSourceProvider.getDISConfig(kafkaParams),
      classOf[ByteArrayDeserializer].newInstance,
      classOf[ByteArrayDeserializer].newInstance)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  /**
    * Get the record for the given offset if available. Otherwise it will either throw error
    * (if failOnDataLoss = true), or return the next available offset within [offset, untilOffset),
    * or null.
    *
    * @param offset         the offset to fetch.
    * @param untilOffset    the max offset to fetch. Exclusive.
    * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
    * @param failOnDataLoss When `failOnDataLoss` is `true`, this method will either return record at
    *                       offset if available, or throw exception.when `failOnDataLoss` is `false`,
    *                       this method will either return record at offset if available, or return
    *                       the next earliest available record less than untilOffset, or null. It
    *                       will not throw any exception.
    */
  def get(
           offset: Long,
           untilOffset: Long,
           pollTimeoutMs: Long,
           failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    require(offset < untilOffset,
      s"offset must always be less than untilOffset [offset: $offset, untilOffset: $untilOffset]")
    myLogDebug(s"Get $groupId $topicPartition nextOffset $nextOffsetInFetchedData requested $offset")
    // The following loop is basically for `failOnDataLoss = false`. When `failOnDataLoss` is
    // `false`, first, we will try to fetch the record at `offset`. If no such record exists, then
    // we will move to the next available offset within `[offset, untilOffset)` and retry.
    // If `failOnDataLoss` is `true`, the loop body will be executed only once.
    var toFetchOffset = offset
    while (toFetchOffset != UNKNOWN_OFFSET) {
      try {
        return fetchData(toFetchOffset, untilOffset, pollTimeoutMs, failOnDataLoss)
      } catch {
        case e: DISSequenceNumberOutOfRangeException =>
          // When there is some error thrown, it's better to use a new consumer to drop all cached
          // states in the old consumer. We don't need to worry about the performance because this
          // is not a common path.
          resetConsumer()
          reportDataLoss(failOnDataLoss, s"Cannot fetch offset $toFetchOffset", e)
          toFetchOffset = getEarliestAvailableOffsetBetween(toFetchOffset, untilOffset)
      }
    }
    resetFetchedData()
    null
  }

  /**
    * Return the next earliest available offset in [offset, untilOffset). If all offsets in
    * [offset, untilOffset) are invalid (e.g., the topic is deleted and recreated), it will return
    * `UNKNOWN_OFFSET`.
    */
  private def getEarliestAvailableOffsetBetween(offset: Long, untilOffset: Long): Long = {
    val (earliestOffset, latestOffset) = getAvailableOffsetRange()
    myLogWarning(s"Some data may be lost. Recovering from the earliest offset: $earliestOffset")
    if (offset >= latestOffset || earliestOffset >= untilOffset) {
      // [offset, untilOffset) and [earliestOffset, latestOffset) have no overlap,
      // either
      // --------------------------------------------------------
      //         ^                 ^         ^         ^
      //         |                 |         |         |
      //   earliestOffset   latestOffset   offset   untilOffset
      //
      // or
      // --------------------------------------------------------
      //      ^          ^              ^                ^
      //      |          |              |                |
      //   offset   untilOffset   earliestOffset   latestOffset
      val warningMessage =
      s"""
         |The current available offset range is [$earliestOffset, $latestOffset).
         | Offset ${offset} is out of range, and records in [$offset, $untilOffset) will be
         | skipped ${additionalMessage(failOnDataLoss = false)}
        """.stripMargin
      myLogWarning(warningMessage)
      UNKNOWN_OFFSET
    } else if (offset >= earliestOffset) {
      // -----------------------------------------------------------------------------
      //         ^            ^                  ^                                 ^
      //         |            |                  |                                 |
      //   earliestOffset   offset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      //
      // This will happen when a topic is deleted and recreated, and new data are pushed very fast,
      // then we will see `offset` disappears first then appears again. Although the parameters
      // are same, the state in Kafka cluster is changed, so the outer loop won't be endless.
      myLogWarning(s"Found a disappeared offset $offset. " +
        s"Some data may be lost ${additionalMessage(failOnDataLoss = false)}")
      offset
    } else {
      // ------------------------------------------------------------------------------
      //      ^           ^                       ^                                 ^
      //      |           |                       |                                 |
      //   offset   earliestOffset   min(untilOffset,latestOffset)   max(untilOffset, latestOffset)
      val warningMessage =
      s"""
         |The current available offset range is [$earliestOffset, $latestOffset).
         | Offset ${offset} is out of range, and records in [$offset, $earliestOffset) will be
         | skipped ${additionalMessage(failOnDataLoss = false)}
        """.stripMargin
      myLogWarning(warningMessage)
      earliestOffset
    }
  }

  /**
    * Get the record for the given offset if available. Otherwise it will either throw error
    * (if failOnDataLoss = true), or return the next available offset within [offset, untilOffset),
    * or null.
    *
    * @throws DISSequenceNumberOutOfRangeException if `offset` is out of range
    * @throws TimeoutException          if cannot fetch the record in `pollTimeoutMs` milliseconds.
    */
  private def fetchData(
                         offset: Long,
                         untilOffset: Long,
                         pollTimeoutMs: Long,
                         failOnDataLoss: Boolean): ConsumerRecord[Array[Byte], Array[Byte]] = {
    if (offset != nextOffsetInFetchedData || !fetchedData.hasNext()) {
      // This is the first fetch, or the last pre-fetched data has been drained.
      // Seek to the offset because we may call seekToBeginning or seekToEnd before this.
      if(offset != nextOffsetInFetchedData){
        seek(offset)
      }
      poll(pollTimeoutMs)
    }

    if (!fetchedData.hasNext()) {
      // We cannot fetch anything after `poll`. Two possible cases:
      // - `offset` is out of range so that Kafka returns nothing. Just throw
      // `OffsetOutOfRangeException` to let the caller handle it.
      // - Cannot fetch any data before timeout. TimeoutException will be thrown.
      val (earliestOffset, latestOffset) = getAvailableOffsetRange()
      if (offset < earliestOffset || offset >= latestOffset) {
        throw new DISSequenceNumberOutOfRangeException(
          Map(topicPartition -> java.lang.Long.valueOf(offset)).asJava.toString)
      } else {
        throw new TimeoutException(
          s"Cannot fetch record for offset $offset in $pollTimeoutMs milliseconds")
      }
    } else {
      val record = fetchedData.next()
      nextOffsetInFetchedData = record.offset + 1
      // In general, Kafka uses the specified offset as the start point, and tries to fetch the next
      // available offset. Hence we need to handle offset mismatch.
      if (record.offset > offset) {
        // This may happen when some records aged out but their offsets already got verified
        if (failOnDataLoss) {
          reportDataLoss(true, s"Cannot fetch records in [$offset, ${record.offset})")
          // Never happen as "reportDataLoss" will throw an exception
          null
        } else {
          if (record.offset >= untilOffset) {
            reportDataLoss(false, s"Skip missing records in [$offset, $untilOffset)")
            null
          } else {
            reportDataLoss(false, s"Skip missing records in [$offset, ${record.offset})")
            record
          }
        }
      } else if (record.offset < offset) {
        // This should not happen. If it does happen, then we probably misunderstand Kafka internal
        // mechanism.
        throw new IllegalStateException(
          s"Tried to fetch $offset but the returned record offset was ${record.offset}")
      } else {
        record
      }
    }
  }

  /** Create a new consumer and reset cached states */
  private def resetConsumer(): Unit = {
    consumer.close()
    consumer = createConsumer
    resetFetchedData()
  }

  /** Reset the internal pre-fetched data. */
  private def resetFetchedData(): Unit = {
    nextOffsetInFetchedData = UNKNOWN_OFFSET
    fetchedData = ju.Collections.emptyIterator[ConsumerRecord[Array[Byte], Array[Byte]]]
  }

  /**
    * Return an addition message including useful message and instruction.
    */
  private def additionalMessage(failOnDataLoss: Boolean): String = {
    if (failOnDataLoss) {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE"
    } else {
      s"(GroupId: $groupId, TopicPartition: $topicPartition). " +
        s"$INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE"
    }
  }

  /**
    * Throw an exception or log a warning as per `failOnDataLoss`.
    */
  private def reportDataLoss(
                              failOnDataLoss: Boolean,
                              message: String,
                              cause: Throwable = null): Unit = {
    val finalMessage = s"$message ${additionalMessage(failOnDataLoss)}"
    if (failOnDataLoss) {
      if (cause != null) {
        throw new IllegalStateException(finalMessage)
      } else {
        throw new IllegalStateException(finalMessage, cause)
      }
    } else {
      if (cause != null) {
        myLogWarning(finalMessage)
      } else {
        myLogWarning(finalMessage, cause)
      }
    }
  }

  private def close(): Unit = consumer.close()

  private def seek(offset: Long): Unit = {
    myLogInfo(s"Seeking to $groupId $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(pollTimeoutMs: Long): Unit = {
    val startTime = System.currentTimeMillis()
    var totalCostTime: Long = 0
    var recordList: ju.List[ConsumerRecord[Array[Byte], Array[Byte]]] = null
    var execution: BackOffExecution = null
    var retryCount = 0
    var callStartTime: Long = 0
    var callEndTime: Long = 0
    var isGetData: Boolean = false
    while (!isGetData && totalCostTime < pollTimeoutMs) {
      callStartTime = System.currentTimeMillis()
      val p = consumer.poll(pollTimeoutMs)
      callEndTime = System.currentTimeMillis()
      totalCostTime = callEndTime - startTime
      recordList = p.records(topicPartition)
      if (recordList.size() != 0) {
        // Traffic control or network error, should be retry
        isGetData = true
      } else {
        if (execution == null) {
          execution = backOff.start
        }
        val sleepTime = execution.nextBackOff
        myLogWarning(s"Polled $topicPartition 0 records cost ${callEndTime - callStartTime}ms" +
          s", will retry after ${sleepTime}ms, total retry count $retryCount, total cost ${totalCostTime}ms")
        Thread.sleep(sleepTime)
        retryCount = retryCount + 1
      }
    }

    var log = s"Polled $topicPartition ${recordList.size} records cost ${callEndTime - callStartTime}ms"

    if (recordList.size() > 0) {
      log += s", lastOffset ${recordList.get(recordList.size() - 1).offset()}"
    }

    if (retryCount > 0) {
      log += s", total retry count $retryCount, total cost ${totalCostTime}ms."
    }
    myLogInfo(log)
    fetchedData = recordList.iterator
  }

  /**
    * Return the available offset range of the current partition. It's a pair of the earliest offset
    * and the latest offset.
    */
  private def getAvailableOffsetRange(): (Long, Long) = {
    consumer.seekToBeginning(Set(topicPartition).asJava)
    val earliestOffset = consumer.position(topicPartition)
    consumer.seekToEnd(Set(topicPartition).asJava)
    val latestOffset = consumer.position(topicPartition)
    (earliestOffset, latestOffset)
  }
}

private[source] object CachedDISKafkaConsumer extends MyLogging {

  private val UNKNOWN_OFFSET = -2L

  private case class CacheKey(groupId: String, topicPartition: TopicPartition)

  private lazy val cache = {
    val conf = SparkEnv.get.conf
    val capacity = conf.getInt("spark.sql.kafkaConsumerCache.capacity", 64)
    new ju.LinkedHashMap[CacheKey, CachedDISKafkaConsumer](capacity, 0.75f, true) {
      override def removeEldestEntry(
                                      entry: ju.Map.Entry[CacheKey, CachedDISKafkaConsumer]): Boolean = {
        if (this.size > capacity) {
          myLogWarning(s"KafkaConsumer cache hitting max capacity of $capacity, " +
            s"removing consumer for ${entry.getKey}")
          try {
            entry.getValue.close()
          } catch {
            case e: SparkException =>
              myLogError(s"Error closing earliest Kafka consumer for ${entry.getKey}", e)
          }
          true
        } else {
          false
        }
      }
    }
  }

  /**
    * Get a cached consumer for groupId, assigned to topic and partition.
    * If matching consumer doesn't already exist, will be created using kafkaParams.
    */
  def getOrCreate(
                   topic: String,
                   partition: Int,
                   kafkaParams: ju.Map[String, Object]): CachedDISKafkaConsumer = synchronized {
    val groupId = kafkaParams.get(DisConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    val topicPartition = new TopicPartition(topic, partition)
    val key = CacheKey(groupId, topicPartition)

    // If this is reattempt at running the task, then invalidate cache and start with
    // a new consumer
    if (TaskContext.get != null && TaskContext.get.attemptNumber > 1) {
      val removedConsumer = cache.remove(key)
      if (removedConsumer != null) {
        removedConsumer.close()
      }
      new CachedDISKafkaConsumer(topicPartition, kafkaParams)
    } else {
      if (!cache.containsKey(key)) {
        cache.put(key, new CachedDISKafkaConsumer(topicPartition, kafkaParams))
      }
      cache.get(key)
    }
  }
}