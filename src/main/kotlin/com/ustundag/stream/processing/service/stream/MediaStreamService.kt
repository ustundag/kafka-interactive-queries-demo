package com.ustundag.stream.processing.service.stream

import com.ustundag.stream.processing.constant.KafkaConfigConstants
import com.ustundag.stream.processing.constant.KafkaConfigConstants.APPLICATION_ID
import com.ustundag.stream.processing.constant.KafkaConfigConstants.BOOTSTRAP_SERVERS
import com.ustundag.stream.processing.constant.KafkaConfigConstants.IS_STREAM_AVAILABLE_PROPERTY
import com.ustundag.stream.processing.constant.KafkaConfigConstants.REST_HOSTNAME
import com.ustundag.stream.processing.constant.KafkaConfigConstants.STATE_DIRECTORY
import com.ustundag.stream.processing.constant.MediaEventConstants.ALL_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.COUNT_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.MEDIA_UPLOAD_TOPIC
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_GRACE_PERIOD_SECONDS
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_HOPPING_DURATION_SECONDS
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_STORE
import com.ustundag.stream.processing.constant.MediaEventConstants.WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS
import com.ustundag.stream.processing.model.MediaUploadCommand
import com.ustundag.stream.processing.service.stream.serdes.MediaSerde
import com.ustundag.stream.processing.util.getUrlPrefixByHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.WindowStore
import org.springframework.boot.CommandLineRunner
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.*

@Component
class MediaStreamService(private val environment: Environment) : CommandLineRunner {

    private lateinit var stream: KafkaStreams
    private lateinit var host: HostInfo
    private val port = environment.getProperty("server.port")?.toInt() ?: KafkaConfigConstants.DEFAULT_REST_PORT

    override fun run(vararg args: String?) {
        host = HostInfo(REST_HOSTNAME, port)
        val topology = createTopologyWithAllMedias()
        println(topology.describe())
        val streams = KafkaStreams(topology, getStreamsConfig())
        streams.cleanUp()
        streams.setStateListener { currentState, previousState ->
            System.setProperty(IS_STREAM_AVAILABLE_PROPERTY, currentState.isRunningOrRebalancing.toString())
        }
        streams.start()
        stream = streams
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(Thread {
            try {
                streams.close()
                println("Media Stream has been stopped!")
            } catch (e: Exception) {
                println(e)
            }
        })
    }

    fun getStream() = stream
    fun getHostInfo() = host

    private fun getStreamsConfig(): Properties {
        val streamsConfiguration = Properties()
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = APPLICATION_ID
        // Where to find Kafka broker(s).
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        // Provide the details of our embedded http service that we'll use to connect to this streams
        // instance and discover locations of stores.
        streamsConfiguration[StreamsConfig.APPLICATION_SERVER_CONFIG] = "$REST_HOSTNAME:$port"
        streamsConfiguration[StreamsConfig.STATE_DIR_CONFIG] = "${STATE_DIRECTORY}_$port"
        // Set to earliest so we don't miss any data that arrived in the topics before the process
        // started
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        // Set the commit interval to 500ms so that any changes are flushed frequently and the top five
        // charts are updated with low latency.
        streamsConfiguration[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 500
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = MediaSerde::class.java
        // Allow the user to fine-tune the `metadata.max.age.ms` via Java system properties from the CLI.
        // Lowering this parameter from its default of 5 minutes to a few seconds is helpful in
        // situations where the input topic was not pre-created before running the application because
        // the application will discover a newly created topic faster.  In production, you would
        // typically not change this parameter from its default.
        val metadataMaxAgeMs = System.getProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG)
        if (metadataMaxAgeMs != null) {
            try {
                val value = metadataMaxAgeMs.toInt()
                streamsConfiguration[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = value
                println("Set consumer configuration " + ConsumerConfig.METADATA_MAX_AGE_CONFIG + " to " + value)
            } catch (ignored: NumberFormatException) {
            }
        }
        return streamsConfiguration
    }

    private fun createTopologyWithAllMedias(): Topology {
        val builder = StreamsBuilder()
        // get table and create a state store to hold all the event info
        val mediaTable: KTable<String, MediaUploadCommand> = builder.table(MEDIA_UPLOAD_TOPIC,
            Materialized.`as`<String, MediaUploadCommand, KeyValueStore<Bytes, ByteArray>>(ALL_MEDIAS_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(MediaSerde()))
        return builder.build()
    }

    private fun createTopologyWithAllMediasEditNullKeys(): Topology {
        val builder = StreamsBuilder()
        // get table and create a state store to hold all the event info
        val mediaTable: KTable<String, MediaUploadCommand> =
            builder.stream<String, MediaUploadCommand>(MEDIA_UPLOAD_TOPIC)
                .selectKey { key, event ->
                    when (key) {
                        null -> event.url.getUrlPrefixByHost()
                        else -> key
                    }
                }
                .toTable(Named.`as`(MEDIA_UPLOAD_TOPIC),
                    Materialized.`as`<String, MediaUploadCommand, KeyValueStore<Bytes, ByteArray>>(ALL_MEDIAS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MediaSerde()))
        return builder.build()
    }

    private fun createTopologyWithAllMediasCount(): Topology {
        val builder = StreamsBuilder()
        val windowedMediaTable = builder.stream<String, MediaUploadCommand>(MEDIA_UPLOAD_TOPIC)
            .groupBy({ k, _ -> k },
                Grouped.with(Serdes.String(), MediaSerde())
            )
            .count(Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(COUNT_MEDIAS_STORE)
                .withValueSerde(Serdes.Long()))
        return builder.build()
    }

    private fun createTopologyWithAllMediasWindowed(): Topology {
        val builder = StreamsBuilder()
        val windowSize = Duration.ofSeconds(WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS)
        val windowHopingSize = Duration.ofSeconds(WINDOWED_MEDIAS_HOPPING_DURATION_SECONDS)
        val windowGracePeriod = Duration.ofSeconds(WINDOWED_MEDIAS_GRACE_PERIOD_SECONDS)

        val inMemoryWindowStore = Stores.inMemoryWindowStore(
            WINDOWED_MEDIAS_STORE,
            windowSize + windowGracePeriod,
            windowSize,
            false
        )

        val windowedMediaTable = builder.stream<String, MediaUploadCommand>(MEDIA_UPLOAD_TOPIC)
            .selectKey { key, event ->
                when (key) {
                    null -> event.url.getUrlPrefixByHost()
                    else -> key
                }
            }
            .groupByKey(Grouped.with(Serdes.String(), MediaSerde()))

//            .windowedBy(TimeWindows.of(windowSize))
//            .windowedBy(TimeWindows.of(windowSize).advanceBy(windowHopingSize))
//            .windowedBy(TimeWindows.of(windowSize).grace(windowGracePeriod))
            .windowedBy(TimeWindows.of(windowSize).grace(windowGracePeriod).advanceBy(windowHopingSize))

//            .reduce(
//                { _, v2 -> v2 },
//                Materialized.`as`(inMemoryWindowStore)
//            )

            // take last value for each grouped key - value pairs
            .reduce(
                { _, v2 -> v2 },
                Materialized.`as`<String, MediaUploadCommand, WindowStore<Bytes, ByteArray>>(WINDOWED_MEDIAS_STORE)
                    .withValueSerde(MediaSerde())
                    .withRetention(windowSize + windowGracePeriod)
            )
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .peek { wKey, event ->
                println("key: ${wKey.key()} <> windowId: ${wKey.window().hashCode()} <> " +
                        "started: ${wKey.window().startTime()} <> ended: ${wKey.window().endTime()}")
            }

//        windowedMediaTable.toStream().print(Printed.toSysOut())
//        windowedMediaTable.toStream().foreach { wKey, event ->
//            println("key: ${wKey.key()} <> windowId: ${wKey.window().hashCode()} <> " +
//                    "started: ${wKey.window().startTime()} <> ended: ${wKey.window().endTime()} <> " +
//                    "event: $event")
//        }
        return builder.build()
    }

    private fun createTopologyWithAllMediasWindowedRetention(): Topology {
        val builder = StreamsBuilder()
        val windowSize = Duration.ofSeconds(WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS)
        val retention = windowSize.plus(Duration.ofSeconds(WINDOWED_MEDIAS_GRACE_PERIOD_SECONDS))

        val materialized = Materialized.`as`<String, MediaUploadCommand>(Stores
            .inMemoryWindowStore(WINDOWED_MEDIAS_STORE, retention, windowSize, false))
            .withKeySerde(Serdes.String())
            .withValueSerde(MediaSerde())

        val windowedMediaTable = builder.stream<String, MediaUploadCommand>(MEDIA_UPLOAD_TOPIC)
            .selectKey { key, event ->
                when (key) {
                    null -> event.url.getUrlPrefixByHost()
                    else -> key
                }
            }
            .groupByKey(Grouped.with(Serdes.String(), MediaSerde()))
            .windowedBy(TimeWindows.of(windowSize).grace(Duration.ZERO))
            .reduce(
                { _, v2 -> v2 },
                materialized
            )
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        return builder.build()
    }
}
