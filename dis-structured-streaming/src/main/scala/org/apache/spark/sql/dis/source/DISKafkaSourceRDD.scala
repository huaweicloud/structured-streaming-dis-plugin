package org.apache.spark.sql.dis.source

import java.{util => ju}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer


/** Offset range that one partition of the DISKafkaSourceRDD has to read */
private[source] case class DISKafkaSourceRDDOffsetRange(
                                                        topicPartition: TopicPartition,
                                                        fromOffset: Long,
                                                        untilOffset: Long,
                                                        preferredLoc: Option[String]) {
  def topic: String = topicPartition.topic

  def partition: Int = topicPartition.partition

  def size: Long = untilOffset - fromOffset
}


/** Partition of the DISKafkaSourceRDD */
private[source] case class DISKafkaSourceRDDPartition(
                                                      index: Int, offsetRange: DISKafkaSourceRDDOffsetRange) extends Partition


/**
  * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
  * Additionally, it allows preferred locations to be set for each topic + partition, so that
  * the [[DISKafkaSource]] can ensure the same executor always reads the same topic + partition
  * and cached KafkaConsuemrs (see [[CachedDISKafkaConsumer]] can be used read data efficiently.
  *
  * @param sc                  the [[SparkContext]]
  * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
  * @param offsetRanges        Offset ranges that define the Kafka data belonging to this RDD
  */
private[source] class DISKafkaSourceRDD(
                                        sc: SparkContext,
                                        executorKafkaParams: ju.Map[String, Object],
                                        offsetRanges: Seq[DISKafkaSourceRDDOffsetRange],
                                        pollTimeoutMs: Long,
                                        failOnDataLoss: Boolean)
  extends RDD[ConsumerRecord[Array[Byte], Array[Byte]]](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new DISKafkaSourceRDDPartition(i, o) }.toArray
  }

  override def count(): Long = offsetRanges.map(_.size).sum

  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] = {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[DISKafkaSourceRDDPartition]).filter(_.offsetRange.size > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return new Array[ConsumerRecord[Array[Byte], Array[Byte]]](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.offsetRange.size)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]) =>
        it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
    )
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[DISKafkaSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
                        thePart: Partition,
                        context: TaskContext): Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val range = thePart.asInstanceOf[DISKafkaSourceRDDPartition].offsetRange
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      new NextIterator[ConsumerRecord[Array[Byte], Array[Byte]]]() {
        logInfo(s"Start New NextIterator ${range.partition}, ${range.fromOffset} ~ ${range.untilOffset}")
        val consumer = CachedDISKafkaConsumer.getOrCreate(
          range.topic, range.partition, executorKafkaParams)
        var requestOffset = range.fromOffset

        override def getNext(): ConsumerRecord[Array[Byte], Array[Byte]] = {
          if (requestOffset >= range.untilOffset) {
            // Processed all offsets in this partition.
            finished = true
            null
          } else {
            val r = consumer.get(requestOffset, range.untilOffset, pollTimeoutMs, failOnDataLoss)
            if (r == null) {
              // Losing some data. Skip the rest offsets in this partition.
              finished = true
              null
            } else {
              requestOffset = r.offset + 1
              r
            }
          }
        }

        override protected def close(): Unit = {}
      }
    }
  }
}