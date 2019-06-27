package org.apache.spark.sql.dis.source

import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import org.apache.spark.sql.dis.JsonUtils
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
  * An [[Offset]] for the [[DISKafkaSource]]. This one tracks all partitions of subscribed topics and
  * their offsets.
  */
private[source]
case class DISKafkaSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {

  override val json: String = JsonUtils.partitionOffsets(partitionToOffsets)
}

/** Companion object of the [[DISKafkaSourceOffset]] */
private[source] object DISKafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: DISKafkaSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => DISKafkaSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to DISKafkaSourceOffset")
    }
  }

  /**
    * Returns [[DISKafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
    * tuples.
    */
  def apply(offsetTuples: (String, Int, Long)*): DISKafkaSourceOffset = {
    DISKafkaSourceOffset(offsetTuples.map { case(t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }

  /**
    * Returns [[DISKafkaSourceOffset]] from a JSON [[SerializedOffset]]
    */
  def apply(offset: SerializedOffset): DISKafkaSourceOffset =
    DISKafkaSourceOffset(JsonUtils.partitionOffsets(offset.json))
}