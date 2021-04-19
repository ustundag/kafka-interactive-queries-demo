package com.ustundag.stream.processing.controller

import com.ustundag.stream.processing.constant.MediaEventConstants.ALL_MEDIAS_STORE_NAME
import com.ustundag.stream.processing.constant.MediaEventConstants.COUNT_MEDIAS_STORE_NAME
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_STORE_NAME
import com.ustundag.stream.processing.model.MediaUploadCommand
import com.ustundag.stream.processing.model.MediaUploadCommandResponse
import com.ustundag.stream.processing.service.stream.MediaStreamService
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyQueryMetadata
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.web.bind.annotation.*
import org.springframework.web.client.RestTemplate
import java.time.Instant

@RestController
@CrossOrigin(origins = ["*"])
@RequestMapping("/media")
class MediaEventController(private val streamService: MediaStreamService) {

    private val restTemplate = RestTemplate()

    @GetMapping("/print")
    fun printUnavailableKeys() {
        val mediaStream = streamService.getStream()
        listOf("key_domain_A", "key_domain_B", "key_domain_C", "key_domain_D",
            "key_domain_E", "key_domain_F", "key_domain_G", "key_domain_H")
            .forEach { key ->
                val keyQueryMetadata =
                    mediaStream.queryMetadataForKey(ALL_MEDIAS_STORE_NAME, key, Serdes.String().serializer())
                if (!isThisHost(keyQueryMetadata)) {
                    val path = "media/all/$key"
                    val url = "http://${keyQueryMetadata.activeHost.host()}:${keyQueryMetadata.activeHost.port()}/$path"
                    println(url)
                }
            }
    }

    @GetMapping("/all/{key}")
    fun getMediaByKey(@PathVariable key: String): MediaUploadCommandResponse {
        val mediaStream = streamService.getStream()
        val keyQueryMetadata =
            mediaStream.queryMetadataForKey(ALL_MEDIAS_STORE_NAME, key, Serdes.String().serializer())
        if (!isThisHost(keyQueryMetadata)) {
            val path = "media/all/$key"
            return fetchEventFromRemote(keyQueryMetadata, path)
        }

        val mediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            ALL_MEDIAS_STORE_NAME,
            QueryableStoreTypes.keyValueStore<String, MediaUploadCommand>()))
        val event = mediaStore[key] ?: throw Exception("Media event with key: $key not found!")
        return MediaUploadCommandResponse(event, keyQueryMetadata.partition)
    }

    @GetMapping("/count/{key}")
    fun getMediaByKeyCount(@PathVariable key: String): Long {
        val mediaStream = streamService.getStream()
        val keyQueryMetadata =
            mediaStream.queryMetadataForKey(COUNT_MEDIAS_STORE_NAME, key, Serdes.String().serializer())
        if (!isThisHost(keyQueryMetadata)) {
            val path = "media/count/$key"
            return fetchCountFromRemote(keyQueryMetadata, path)
        }
        val mediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            COUNT_MEDIAS_STORE_NAME,
            QueryableStoreTypes.keyValueStore<String, Long>()))
        return mediaStore[key] ?: throw Exception("Media count with key: $key not found!")
    }

    // todo not working :(
    @GetMapping("/window/{key}")
    fun getMediaByKeyWindowed(@PathVariable key: String): List<MediaUploadCommandResponse> {
        val events = mutableListOf<KeyValue<String, MediaUploadCommand>>()
        val mediaStream = streamService.getStream()
        val windowedMediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            WINDOWED_MEDIAS_STORE_NAME,
            QueryableStoreTypes.windowStore<String, MediaUploadCommand>()))

        val to = Instant.now()
        val from = Instant.now().minusSeconds(5 * 60) // 5 minute before
        val eventIterator = windowedMediaStore.fetch(key, from, to)
        if (!eventIterator.hasNext()) {
            return emptyList()
        }
        eventIterator.forEach {
            println(it)
        }
//        val keyQueryMetadata = mediaStream.queryMetadataForKey(WINDOWED_MEDIAS_STORE_NAME, key, StringSerializer())
//        return MediaUploadCommandResponse(event., keyQueryMetadata.partition)
        return emptyList()
    }

    private fun fetchEventFromRemote(host: KeyQueryMetadata, path: String): MediaUploadCommandResponse {
        println("Event not found in the local store! Fetching from remote store...")
        val url = "http://${host.activeHost.host()}:${host.activeHost.port()}/$path"
        return restTemplate.getForEntity(url, MediaUploadCommandResponse::class.java).body
            ?: throw Exception("Media count not found!")
    }

    private fun fetchCountFromRemote(host: KeyQueryMetadata, path: String): Long {
        println("Count not found in the local store! Fetching from remote store...")
        val url = "http://${host.activeHost.host()}:${host.activeHost.port()}/$path"
        return restTemplate.getForEntity(url, Long::class.java).body
            ?: throw Exception("Media count not found!")
    }

    private fun isThisHost(metadata: KeyQueryMetadata): Boolean {
        val myHostInfo = streamService.getHostInfo()
        return metadata.activeHost.host() == myHostInfo.host() &&
                metadata.activeHost.port() == myHostInfo.port()
    }
}
