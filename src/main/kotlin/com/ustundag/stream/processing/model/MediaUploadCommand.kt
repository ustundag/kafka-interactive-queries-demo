package com.ustundag.stream.processing.model

data class MediaUploadCommand(val otherFields: String = "dummy", var url: String)

data class MediaUploadCommandResponse(val event: MediaUploadCommand, val partition: Int)
