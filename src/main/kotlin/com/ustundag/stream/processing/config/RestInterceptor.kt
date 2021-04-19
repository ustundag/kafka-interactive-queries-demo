package com.ustundag.stream.processing.config

import com.ustundag.stream.processing.service.PropertiesService
import org.springframework.stereotype.Component
import org.springframework.web.servlet.HandlerInterceptor
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE

@Component
class RestInterceptor(private val propertiesService: PropertiesService) : HandlerInterceptor {

    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        if (!propertiesService.isStreamAvailable) {
            response.writer.write("Stream is not available!")
            response.status = SC_SERVICE_UNAVAILABLE
            return false
        }
        return super.preHandle(request, response, handler)
    }
}
