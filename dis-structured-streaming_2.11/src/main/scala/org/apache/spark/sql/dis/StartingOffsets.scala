package org.apache.spark.sql.dis

import com.huaweicloud.dis.adapter.kafka.common.TopicPartition


/*
 * Values that can be specified for config startingOffsets
 */
sealed trait StartingOffsets

case object EarliestOffsets extends StartingOffsets

case object LatestOffsets extends StartingOffsets

case class SpecificOffsets(partitionOffsets: Map[TopicPartition, Long]) extends StartingOffsets