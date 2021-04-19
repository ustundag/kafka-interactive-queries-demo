package com.ustundag.stream.processing.service

import com.ustundag.stream.processing.constant.KafkaConfigConstants.DEFAULT_REST_PORT
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service

@Service
class PropertiesService(private val environment: Environment) {

    var isStreamAvailable: Boolean = false
    val port: Int = environment.getProperty("server.port")?.toInt() ?: DEFAULT_REST_PORT
}
