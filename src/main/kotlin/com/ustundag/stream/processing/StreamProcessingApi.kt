package com.ustundag.stream.processing

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class StreamProcessingApi

fun main(args: Array<String>) {
    runApplication<StreamProcessingApi>(*args)
}
