package com.ustundag.stream.processing

import com.ustundag.stream.processing.constant.KafkaConfigConstants.BOOTSTRAP_SERVERS
import com.ustundag.stream.processing.constant.MediaEventConstants.MEDIA_UPLOAD_TOPIC
import com.ustundag.stream.processing.model.MediaUploadCommand
import com.ustundag.stream.processing.service.stream.serdes.MediaDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

fun createConsumer(): KafkaConsumer<String, MediaUploadCommand> {
    val props = Properties()
    props[BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    props[GROUP_ID_CONFIG] = "kafka-streams-consumer-group-test"
    props[KEY_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer()::class.java
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = MediaDeserializer::class.java
    props[ENABLE_AUTO_COMMIT_CONFIG] = true
    props[AUTO_OFFSET_RESET_CONFIG] = "earliest"
    val consumer = KafkaConsumer<String, MediaUploadCommand>(props)
    consumer.subscribe(listOf(MEDIA_UPLOAD_TOPIC))
    return consumer
}

fun main() {
    val consumer = createConsumer()
    consumer.use {
        while (true) {
            val consumerRecords = consumer.poll(Duration.ofSeconds(1))
            if (!consumerRecords.isEmpty) {
                consumerRecords.forEach {
                    val date = LocalDateTime.ofInstant(Instant.ofEpochMilli(it.timestamp()), ZoneId.systemDefault())
                    val dayHour = "${date.hour}:${date.minute}:${date.second}:"
                    println("Date: $dayHour <> Key: ${it.key()} <> Event: ${it.value()}")
                }
            }
        }
    }

//    val metricName = "records-lag"
//    val metricGroup = "consumer-fetch-manager-metrics"
//    val metricDescription = "The latest lag of the partition"
//    val metricTags = mutableMapOf<String, String>()
//    metricTags["client-id"] = "kafka-streams-consumer-group-test"
//    metricTags["topic"] = "media-upload-topic"
//    metricTags["partition"] = "0"
//    val metric = MetricName(metricName, metricGroup, metricDescription, metricTags)
//
//    val asd = consumer.metrics()[metric]
//
//    consumer.metrics().forEach {
//        println("********* key: ${it.key} -- value: ${it.value.metricValue()}")
//    }
//    if (consumerRecords.isEmpty) {
//        println("no message found!")
//    }
//    consumerRecords.forEach {
//        println(it.headers().headers("records-lag-max"))
//        println("Key: ${it.key()} <> Event: ${it.value()}")
//    }
//    consumer.close()
}
