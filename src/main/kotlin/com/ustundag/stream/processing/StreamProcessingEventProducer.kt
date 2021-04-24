package com.ustundag.stream.processing

import com.ustundag.stream.processing.constant.KafkaConfigConstants.BOOTSTRAP_SERVERS
import com.ustundag.stream.processing.constant.MediaEventConstants
import com.ustundag.stream.processing.model.MediaUploadCommand
import com.ustundag.stream.processing.service.stream.serdes.MediaSerde
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.*

fun main() {
    val fileName = "media_url_source.csv"
    val keys: MutableList<String> = mutableListOf()
    val inputStream = StreamProcessingApi::class.java.classLoader.getResourceAsStream(fileName)!!
    val streamReader = InputStreamReader(inputStream, StandardCharsets.UTF_8)
    val bufferedReader = BufferedReader(streamReader)
    bufferedReader.use { reader ->
        var line = reader.readLine()
        while (line != null) {
            keys.add(line)
            line = reader.readLine()
        }
    }

    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    val mediaProducer = KafkaProducer(props, Serdes.String().serializer(), MediaSerde().serializer())

    keys.forEach { key ->
        val command = MediaUploadCommand(url = "https://www.$key.com/images/${(0..1000).random()}.jpg")
        mediaProducer.send(ProducerRecord(MediaEventConstants.MEDIA_UPLOAD_TOPIC, key, command))
    }
    mediaProducer.close()
}
