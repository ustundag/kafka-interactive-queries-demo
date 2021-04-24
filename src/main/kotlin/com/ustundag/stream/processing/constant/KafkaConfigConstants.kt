package com.ustundag.stream.processing.constant

object KafkaConfigConstants {

    const val APPLICATION_ID = "quakka_streams_processing_api"
    const val BOOTSTRAP_SERVERS = "localhost:9092"
    const val REST_HOSTNAME = "localhost"
    const val DEFAULT_REST_PORT = 8080
    const val STATE_DIRECTORY = "/tmp/kafka-streams"
    const val IS_STREAM_AVAILABLE_PROPERTY = "isStreamAvailable"
}