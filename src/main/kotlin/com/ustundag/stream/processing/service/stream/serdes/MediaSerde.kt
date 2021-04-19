package com.ustundag.stream.processing.service.stream.serdes

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.ustundag.stream.processing.model.MediaUploadCommand
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.io.IOException

class MediaSerde : Serde<MediaUploadCommand> {

    private val objectMapper = ObjectMapper().registerKotlinModule()
    private val serializer = serializer()
    private val deserializer = deserializer()

    private inner class MediaSerializer : Serializer<MediaUploadCommand> {

        override fun configure(config: Map<String, *>, isKey: Boolean) {}

        override fun serialize(topic: String, data: MediaUploadCommand): ByteArray {
            return try {
                objectMapper.writeValueAsBytes(data)
            } catch (e: JsonProcessingException) {
                throw SerializationException("Error serializing JSON message", e)
            }
        }

        override fun close() {}
    }

    private inner class MediaDeserializer : Deserializer<MediaUploadCommand> {

        override fun configure(config: Map<String, *>, isKey: Boolean) {}

        override fun deserialize(s: String, bytes: ByteArray): MediaUploadCommand? {
            return try {
                objectMapper.readValue(bytes, MediaUploadCommand::class.java)
            } catch (e: IOException) {
                e.printStackTrace()
                null
            }
        }

        override fun close() {}
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {
        serializer.configure(configs, isKey)
        deserializer.configure(configs, isKey)
    }

    override fun close() {
        serializer.close()
        deserializer.close()
    }

    override fun serializer(): Serializer<MediaUploadCommand> {
        return MediaSerializer()
    }

    override fun deserializer(): Deserializer<MediaUploadCommand> {
        return MediaDeserializer()
    }
}