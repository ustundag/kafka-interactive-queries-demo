package com.ustundag.stream.processing.controller

import com.ustundag.stream.processing.constant.MediaEventConstants.ALL_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.COUNT_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS
import com.ustundag.stream.processing.model.MediaUploadCommand
import com.ustundag.stream.processing.model.MediaUploadCommandResponse
import com.ustundag.stream.processing.service.stream.MediaStreamService
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyQueryMetadata
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.kstream.Windowed
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
        val myHost = streamService.getHostInfo()
        listOf("key_domain_A", "key_domain_B", "key_domain_C", "key_domain_D",
            "key_domain_E", "key_domain_F", "key_domain_G", "key_domain_H")
            .forEach { key ->
                val keyQueryMetadata =
                    mediaStream.queryMetadataForKey(WINDOWED_MEDIAS_STORE, key, Serdes.String().serializer())
                var url = "http://${myHost.host()}:${myHost.port()}"
                if (!isThisHost(keyQueryMetadata)) {
                    url = "http://${keyQueryMetadata.activeHost.host()}:${keyQueryMetadata.activeHost.port()}"
                }
                println(url)
            }
    }

    @GetMapping("/all/{key}")
    fun getMediaByKey(@PathVariable key: String): MediaUploadCommandResponse {
        val mediaStream = streamService.getStream()
        val keyQueryMetadata =
            mediaStream.queryMetadataForKey(ALL_MEDIAS_STORE, key, Serdes.String().serializer())
        if (!isThisHost(keyQueryMetadata)) {
            val path = "media/all/$key"
            return fetchEventFromRemote(keyQueryMetadata, path)
        }

        val mediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            ALL_MEDIAS_STORE,
            QueryableStoreTypes.keyValueStore<String, MediaUploadCommand>()))
        val event = mediaStore[key] ?: throw Exception("Media event with key: $key not found!")
        return MediaUploadCommandResponse(event, keyQueryMetadata.partition)
    }

    @GetMapping("/count/{key}")
    fun getMediaByKeyCount(@PathVariable key: String): Long {
        val mediaStream = streamService.getStream()
        val keyQueryMetadata =
            mediaStream.queryMetadataForKey(COUNT_MEDIAS_STORE, key, Serdes.String().serializer())
        if (!isThisHost(keyQueryMetadata)) {
            val path = "media/count/$key"
            return fetchCountFromRemote(keyQueryMetadata, path)
        }
        val mediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            COUNT_MEDIAS_STORE,
            QueryableStoreTypes.keyValueStore<String, Long>()))
        return mediaStore[key] ?: throw Exception("Media count with key: $key not found!")
    }

    @GetMapping("/window/all")
    fun getAllMediasWindowed(): List<Triple<String, String, MediaUploadCommand>> {
        val eventsWindowed = mutableListOf<KeyValue<Windowed<String>, MediaUploadCommand>>()
        val mediaStream = streamService.getStream()
        val windowedMediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            WINDOWED_MEDIAS_STORE,
            QueryableStoreTypes.windowStore<String, MediaUploadCommand>()))
        windowedMediaStore.all().forEachRemaining { eventsWindowed.add(it) }
        return eventsWindowed.map {
            Triple("started: ${it.key.window().startTime()} <> ended: ${it.key.window().endTime()}",
                it.key.key().toString(),
                it.value
            )
        }
//            .distinctBy { it.second }
    }

    @GetMapping("/window/{key}/parts")
    fun getMediaByKeyWindowedAllParts(@PathVariable key: String): List<MediaUploadCommand> {
        val eventsWindowed = mutableListOf<MediaUploadCommand>()
        val mediaStream = streamService.getStream()
        val windowedMediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            WINDOWED_MEDIAS_STORE,
            QueryableStoreTypes.windowStore<String, MediaUploadCommand>()))
        val to = Instant.now()
        val from = Instant.now().minusSeconds(WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS)
        val eventIterator = windowedMediaStore.fetch(key, from, to)
        if (!eventIterator.hasNext()) {
            return emptyList()
        }
        eventIterator.forEach {
            eventsWindowed.add(it.value)
        }
        return eventsWindowed
    }

    @GetMapping("/window/{key}")
    fun getMediaByKeyWindowed(@PathVariable key: String): MediaUploadCommandResponse {
        val mediaStream = streamService.getStream()
        val keyQueryMetadata =
            mediaStream.queryMetadataForKey(WINDOWED_MEDIAS_STORE, key, Serdes.String().serializer())
                ?: throw Exception("Windowed event not found! Key: $key")
        if (!isThisHost(keyQueryMetadata)) {
            val path = "media/window/{key}"
            return fetchEventFromRemote(keyQueryMetadata, path)
        }

        val windowedMediaStore = mediaStream.store(StoreQueryParameters.fromNameAndType(
            WINDOWED_MEDIAS_STORE,
            QueryableStoreTypes.windowStore<String, MediaUploadCommand>()))
        val to = Instant.now()
        val from = Instant.now().minusSeconds(WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS)
        val eventIterator = windowedMediaStore.fetch(key, from, to)
        if (!eventIterator.hasNext()) {
            throw Exception("Windowed event not found! Key: $key")
        }
        return MediaUploadCommandResponse(eventIterator.next().value, keyQueryMetadata.partition)
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
