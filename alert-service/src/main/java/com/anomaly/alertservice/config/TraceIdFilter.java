package com.anomaly.alertservice.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Gives every request a trace id, in the log lines and in the error response.
 *
 * <p>This is what turns a screenshot of a failed request into the log line that
 * explains it. Without it, an operator reporting "it said something went wrong"
 * leaves nothing to search on.
 *
 * <p>An inbound {@code X-Trace-Id} is honoured so a trace started elsewhere is
 * not broken here; otherwise one is generated.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Trace-Id";
    private static final String MDC_KEY = "traceId";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String inbound = request.getHeader(HEADER);
        String traceId =
                inbound == null || inbound.isBlank()
                        ? UUID.randomUUID().toString().replace("-", "").substring(0, 16)
                        : inbound.trim();
        MDC.put(MDC_KEY, traceId);
        response.setHeader(HEADER, traceId);
        try {
            chain.doFilter(request, response);
        } finally {
            // Servlet threads are pooled and reused, so a value left behind
            // would reappear on an unrelated request and mislabel its logs.
            MDC.remove(MDC_KEY);
        }
    }
}
