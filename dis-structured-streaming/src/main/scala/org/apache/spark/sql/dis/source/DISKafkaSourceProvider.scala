package org.apache.spark.sql.dis.source

import java.util.UUID
import java.{util => ju}

import com.huaweicloud.dis.adapter.kafka.consumer.Fetcher
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest
import com.huaweicloud.dis.{DISClient, DISConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.dis.source.DISKafkaSource._
import org.apache.spark.sql.dis.{EarliestOffsets, JsonUtils, LatestOffsets, SpecificOffsets}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
  * The provider class for the [[DISKafkaSource]]. This provider is designed such that it throws
  * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
  * missing options even before the query is started.
  */
private[source] class DISKafkaSourceProvider extends StreamSourceProvider
  with DataSourceRegister with Logging {

  import DISKafkaSourceProvider._

  /**
    * Returns the name and schema of the source. In addition, it also verifies whether the options
    * are correct and sufficient to create the [[DISKafkaSource]] when the query is started.
    */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    validateOptions(parameters)
    ("kafka", DISKafkaSource.kafkaSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    //parameters.keySet.foreach{ k => logInfo("!!!!parameters " + k + "|" + parameters(k))}
    
    
    validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
//    val specifiedKafkaParams = parameters
//            .keySet
//            .filter(_.toLowerCase.startsWith("kafka."))
//            .map { k => k.drop(6).toString -> parameters(k) }
//            .toMap

    val specifiedKafkaParams = parameters
            .keySet
            .map { k => k.toString -> parameters(k) }
            .toMap
    
    val deserClassName = classOf[ByteArrayDeserializer].getName
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-dis-source-${UUID.randomUUID}-${metadataPath.hashCode}"

    val kafkaParamsForStrategy =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

        // So that consumers in Kafka source do not mess with any existing group id
        .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-driver")

        // Set to "earliest" to avoid exceptions. However, DISKafkaSource will fetch the initial
        // offsets by itself instead of counting on KafkaConsumer.
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        // So that consumers in the driver does not commit offsets unnecessarily
        .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        // So that the driver does not pull too much data
        .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, new java.lang.Integer(1))

        // If buffer config is not set, set it to reasonable value to work around
        // buffer issues (see KAFKA-3135)
        .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
        .build()
    
    val kafkaParamsForExecutors =
      ConfigUpdater("executor", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

        // Make sure executors do only what the driver tells them.
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

        // So that consumers in executors do not mess with any existing group id
        .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-executor")

        // So that consumers in executors does not commit offsets unnecessarily
        .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        // If buffer config is not set, set it to reasonable value to work around
        // buffer issues (see KAFKA-3135)
        .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
        .build()
    
    var streamName:String = null
    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("streamname", value) =>
        streamName = value
        AssignStrategy(
          getTopicPartitions(value, kafkaParamsForStrategy).toArray,
          kafkaParamsForStrategy)
      /*
      case ("subscribe", value) =>
        SubscribeStrategy(
          value.split(",").map(_.trim()).filter(_.nonEmpty),
          kafkaParamsForStrategy)
      case ("subscribepattern", value) =>
        SubscribePatternStrategy(
          value.trim(),
          kafkaParamsForStrategy)
      **/
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    val startingOffsets =
      caseInsensitiveParams.get(PROPERTY_STARTING_OFFSETS) match {
        case Some("latest") => LatestOffsets
        case Some("earliest") => EarliestOffsets
        case Some(json) =>
          if (streamName == null)
            SpecificOffsets(JsonUtils.partitionOffsets(json))
          else
            SpecificOffsets(JsonUtils.partitionOffsetsByStreamName(streamName, json))
        case None => LatestOffsets
      }
    
    val failOnDataLoss =
      caseInsensitiveParams.getOrElse(FAIL_ON_DATA_LOSS_OPTION_KEY, "true").toBoolean

    new DISKafkaSource(
      sqlContext,
      strategy,
      kafkaParamsForExecutors,
      parameters,
      metadataPath,
      startingOffsets,
      failOnDataLoss)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {

    // Validate source options

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedStrategies =
      caseInsensitiveParams.filter { case (k, _) => STRATEGY_OPTION_KEYS.contains(k) }.toSeq
    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    }

//    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
//      case ("streamname", value) =>
//        if (!value.trim.startsWith("{")) {
//          throw new IllegalArgumentException(
//            "No topic partitions to assign as specified value for option " +
//              s"'streamName' is '$value'")
//        }
//
//      case ("subscribe", value) =>
//        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
//        if (topics.isEmpty) {
//          throw new IllegalArgumentException(
//            "No topics to subscribe to as specified value for option " +
//              s"'subscribe' is '$value'")
//        }
//      case ("subscribepattern", value) =>
//        val pattern = caseInsensitiveParams("subscribepattern").trim()
//        if (pattern.isEmpty) {
//          throw new IllegalArgumentException(
//            "Pattern to subscribe is empty as specified value for option " +
//              s"'subscribePattern' is '$value'")
//        }
//      case _ =>
//        // Should never reach here as we are already matching on
//        // matched strategy names
//        throw new IllegalArgumentException("Unknown option")
//    }

    // Validate user-specified Kafka options

    if (caseInsensitiveParams.contains(s"${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        s"DIS option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as " +
          s"user-specified consumer groups is not used to track offsets.")
    }

    if (caseInsensitiveParams.contains(s"${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |DIS option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$PROPERTY_STARTING_OFFSETS' to 'earliest' or 'latest'
           |to specify where to start. Structured Streaming manages which offsets are consumed
           |internally, rather than relying on the DISKafkaConsumer to do it. This will ensure that no
           |data is missed when when new topics/partitions are dynamically subscribed. Note that
           |'$PROPERTY_STARTING_OFFSETS' only applies when a new Streaming query is started, and
           |that resuming will always pick up from where the query left off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"DIS option '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations "
          + "to explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"DIS option '${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
          + "operations to explicitly deserialize the values.")
    }

    val otherUnsupportedConfigs = Seq(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, // committing correctly requires new APIs in Source
      ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG) // interceptors can modify payload, so not safe

    otherUnsupportedConfigs.foreach { c =>
      if (caseInsensitiveParams.contains(s"kafka.$c")) {
        throw new IllegalArgumentException(s"Kafka option '$c' is not supported")
      }
    }

    if (!caseInsensitiveParams.contains(s"endpoint")) {
      throw new IllegalArgumentException(
        s"Option '${PROPERTY_ENDPOINT}' must be specified for configuring DIS consumer")
    }

    if (!caseInsensitiveParams.contains(s"ak")) {
      throw new IllegalArgumentException(
        s"Option '${PROPERTY_AK}' must be specified for configuring DIS consumer")
    }

    if (!caseInsensitiveParams.contains(s"sk")) {
      throw new IllegalArgumentException(
        s"Option '${PROPERTY_SK}' must be specified for configuring DIS consumer")
    }

    if (!caseInsensitiveParams.contains(s"region")) {
      throw new IllegalArgumentException(
        s"Option '${PROPERTY_REGION_ID}' must be specified for configuring DIS consumer")
    }

    if (!caseInsensitiveParams.contains(s"projectid")) {
      throw new IllegalArgumentException(
        s"Option '${PROPERTY_PROJECT_ID}' must be specified for configuring DIS consumer")
    }
  }

  override def shortName(): String = "dis"

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      logInfo(s"$module: Set $key to $value, earlier value: ${kafkaParams.get(key).getOrElse("")}")
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
        logInfo(s"$module: Set $key to $value")
      }
      this
    }

    def build(): ju.Map[String, Object] = map
  }

  def getTopicPartitions(streamName: String, disParams: ju.Map[String, Object]): Iterable[TopicPartition] = {
    val disClient = new DISClient(getDISConfig(disParams))
    var topicPartitions = List[TopicPartition]()
    
    val describeStreamRequest = new DescribeStreamRequest
    describeStreamRequest.setStreamName(streamName)
    describeStreamRequest.setLimitPartitions(1)
    val describeStreamResult = disClient.describeStream(describeStreamRequest)
    for (index <- 0 until describeStreamResult.getReadablePartitionCount) {
      topicPartitions = topicPartitions :+ new TopicPartition(streamName, index)
    }

    logInfo(s"DIS Stream [$streamName] has ${topicPartitions.size} partitions.")
    topicPartitions
  }
}

object DISKafkaSourceProvider {
//  private val STRATEGY_OPTION_KEYS = Set("subscribe", "subscribepattern", "assign")
  val PROPERTY_STREAM_NAME = "streamname"
  val PROPERTY_STARTING_OFFSETS = "startingoffsets"
  val PROPERTY_REGION_ID: String = DISConfig.PROPERTY_REGION_ID.toLowerCase
  val PROPERTY_ENDPOINT: String = DISConfig.PROPERTY_ENDPOINT.toLowerCase
  val PROPERTY_PROJECT_ID: String = DISConfig.PROPERTY_PROJECT_ID.toLowerCase
  val PROPERTY_AK: String = DISConfig.PROPERTY_AK.toLowerCase
  val PROPERTY_SK: String = DISConfig.PROPERTY_SK.toLowerCase
  val PROPERTY_CONNECTION_TIMEOUT: String = DISConfig.PROPERTY_CONNECTION_TIMEOUT.toLowerCase
  val PROPERTY_SOCKET_TIMEOUT: String = DISConfig.PROPERTY_SOCKET_TIMEOUT.toLowerCase
  val PROPERTY_MAX_PER_ROUTE: String = DISConfig.PROPERTY_MAX_PER_ROUTE.toLowerCase
  val PROPERTY_MAX_TOTAL: String = DISConfig.PROPERTY_MAX_TOTAL.toLowerCase
  val PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED: String = DISConfig.PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED.toLowerCase
  val PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED: String = DISConfig.PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED.toLowerCase
  val PROPERTY_BODY_SERIALIZE_TYPE: String = DISConfig.PROPERTY_BODY_SERIALIZE_TYPE.toLowerCase
  val PROPERTY_CONFIG_PROVIDER_CLASS: String = DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS.toLowerCase
  val PROPERTY_DATA_PASSWORD: String = DISConfig.PROPERTY_DATA_PASSWORD.toLowerCase
  val PROPERTY_AUTO_OFFSET_RESET: String = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG.toLowerCase
  val PROPERTY_MAX_PARTITION_FETCH_RECORDS: String = Fetcher.KEY_MAX_PARTITION_FETCH_RECORDS.toLowerCase
  val PROPERTY_MAX_FETCH_THREADS: String = Fetcher.KEY_MAX_FETCH_THREADS.toLowerCase
  val PROPERTY_KEY_DESERIALIZER_CLASS: String = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG.toLowerCase
  val PROPERTY_VALUE_DESERIALIZER_CLASS: String = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG.toLowerCase
  
  val STRATEGY_OPTION_KEYS = Set(PROPERTY_STREAM_NAME)
  val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"

  private var _disConfig: Option[DISConfig] = None

  def getDISConfig(disParams: ju.Map[String, Object]): DISConfig = {
    _disConfig.getOrElse{
      _disConfig = Some{
        val disConfig = new DISConfig
        disConfig.putAll(disParams)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_REGION_ID, disParams.get(PROPERTY_REGION_ID), true)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_AK, disParams.get(PROPERTY_AK), true)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_SK, disParams.get(PROPERTY_SK), true)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_PROJECT_ID, disParams.get(PROPERTY_PROJECT_ID), true)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_ENDPOINT, disParams.get(PROPERTY_ENDPOINT), false)
        updateDisConfigParam(disConfig, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false", false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED,
          disParams.getOrDefault(PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED, "false"), false)
        updateDisConfigParam(disConfig, Fetcher.KEY_MAX_PARTITION_FETCH_RECORDS,
          disParams.getOrDefault(PROPERTY_MAX_PARTITION_FETCH_RECORDS, "500"), false)
        updateDisConfigParam(disConfig, Fetcher.KEY_MAX_FETCH_THREADS,
          disParams.getOrDefault(PROPERTY_MAX_FETCH_THREADS, "1"), false)
        updateDisConfigParam(disConfig, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          disParams.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "LATEST").toString.toUpperCase, false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_CONNECTION_TIMEOUT,
          disParams.get(PROPERTY_CONNECTION_TIMEOUT), false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_SOCKET_TIMEOUT,
          disParams.get(PROPERTY_SOCKET_TIMEOUT), false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_BODY_SERIALIZE_TYPE,
          disParams.get(PROPERTY_BODY_SERIALIZE_TYPE), false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED,
          disParams.get(PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED), false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_DATA_PASSWORD,
          disParams.get(PROPERTY_DATA_PASSWORD), false)
        updateDisConfigParam(disConfig, DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS,
          disParams.get(PROPERTY_CONFIG_PROVIDER_CLASS), false)
        disConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        disConfig.remove(ConsumerConfig.GROUP_ID_CONFIG)

        disConfig
      }
      _disConfig.get
    }
  }

  private def updateDisConfigParam(disConfig: DISConfig, param: String, value: Any, isRequired: Boolean): Any = {
    if (value == null) {
      if (isRequired) throw new IllegalArgumentException("param " + param + " is null.")
      return
    }
    disConfig.set(param, value.toString)
  }
}