package com.ustundag.stream.processing.config

import com.ustundag.stream.processing.constant.KafkaConfigConstants.IS_STREAM_AVAILABLE_PROPERTY
import org.springframework.web.servlet.HandlerInterceptor
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE

class RestInterceptor : HandlerInterceptor {

    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        val isStreamAvailable = System.getProperty(IS_STREAM_AVAILABLE_PROPERTY).toBoolean()
        if (isStreamAvailable) {
            return super.preHandle(request, response, handler)
        }
        response.writer.write("Stream is not available!")
        response.status = SC_SERVICE_UNAVAILABLE
        return false
    }
}