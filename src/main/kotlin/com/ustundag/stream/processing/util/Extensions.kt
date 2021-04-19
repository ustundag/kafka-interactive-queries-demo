package com.ustundag.stream.processing.util

import com.ustundag.stream.processing.constant.MediaEventConstants
import org.apache.kafka.common.utils.Utils
import java.net.URL

fun String.getUrlPrefixByHost(): String {
    return try {
        URL(this).host.replace(".com", "")
    } catch (exception: Exception) {
        if (this.length > 17) this.substring(0, 17)
        else this
    }
}

fun String.getPartition(): Int {
//    return Utils.toPositive(Utils.murmur2(this.toByteArray())) % MediaEventConstants.TOPIC_PARTITION_COUNT
    return Utils.toPositive(Utils.murmur2(this.toByteArray())) % 2
}

fun main() {
    val key1 = "101"
    val key2 = "202"
//    val key1 = "i.hizliresim"
//    val key2 = "cdn.shopify"
    println(key1.getPartition())
    println(key2.getPartition())
}