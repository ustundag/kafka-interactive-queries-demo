package com.ustundag.stream.processing.constant

object MediaEventConstants {

    const val TOPIC_PARTITION_COUNT = 3
    const val MEDIA_UPLOAD_TOPIC = "media-upload-topic"
    const val ALL_MEDIAS_STORE = "all-medias-store"
    const val COUNT_MEDIAS_STORE = "count-medias-store"
    const val WINDOWED_MEDIAS_STORE = "windowed-medias-store"
    const val WINDOWED_MEDIAS_WINDOW_DURATION_SECONDS = 60L
    const val WINDOWED_MEDIAS_HOPPING_DURATION_SECONDS = 10L
    const val WINDOWED_MEDIAS_GRACE_PERIOD_SECONDS = 5L
}
