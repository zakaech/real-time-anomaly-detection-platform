package com.anomaly.alertservice.exception;

import jakarta.validation.ConstraintViolationException;
import java.net.URI;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

/**
 * Every error leaves through here, as a {@code ProblemDetail} (RFC 9457).
 *
 * <p>No controller builds an error response of its own. If they did, the format
 * would drift from one endpoint to the next and a client would have to handle
 * several shapes of failure.
 *
 * <p>Three rules the responses follow:
 *
 * <ol>
 *   <li>Every error carries a {@code traceId} correlated with the logs, so a
 *       screenshot from a user leads to the right log line.
 *   <li>A 409 returns the current state, so the client can resynchronise without
 *       a second request.
 *   <li>A 500 never exposes a stack trace. It reveals internal structure, and
 *       the only useful audience for it is the server log.
 * </ol>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String BASE = "https://api.anomaly-platform.local/problems/";

    @ExceptionHandler(AlertNotFoundException.class)
    ProblemDetail onNotFound(AlertNotFoundException e) {
        ProblemDetail problem = problem(HttpStatus.NOT_FOUND, "Alert not found", e.getMessage(), "alert-not-found");
        problem.setProperty("alertId", e.getAlertId().toString());
        return problem;
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    ProblemDetail onInvalidTransition(InvalidStateTransitionException e) {
        ProblemDetail problem =
                problem(
                        HttpStatus.CONFLICT,
                        "Invalid alert state transition",
                        e.getMessage(),
                        "invalid-state-transition");
        problem.setProperty("alertId", e.getAlertId().toString());
        problem.setProperty("currentStatus", e.getCurrentStatus().name());
        problem.setProperty("currentVersion", e.getCurrentVersion());
        return problem;
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    ProblemDetail onConcurrentModification(ObjectOptimisticLockingFailureException e) {
        return problem(
                HttpStatus.CONFLICT,
                "Concurrent modification",
                "The alert was modified by someone else; reload it and retry",
                "concurrent-modification");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail onBodyValidation(MethodArgumentNotValidException e) {
        String detail =
                e.getBindingResult().getFieldErrors().stream()
                        .map(error -> error.getField() + " " + error.getDefaultMessage())
                        .sorted()
                        .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "Validation failed", detail, "validation-failed");
    }

    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail onParameterValidation(ConstraintViolationException e) {
        String detail =
                e.getConstraintViolations().stream()
                        .map(v -> v.getPropertyPath() + " " + v.getMessage())
                        .sorted()
                        .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "Validation failed", detail, "validation-failed");
    }

    @ExceptionHandler({
        HttpMessageNotReadableException.class,
        MethodArgumentTypeMismatchException.class,
        MissingServletRequestParameterException.class
    })
    ProblemDetail onMalformedRequest(Exception e) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "Malformed request",
                e.getMessage(),
                "malformed-request");
    }

    @ExceptionHandler(ResponseStatusException.class)
    ProblemDetail onResponseStatus(ResponseStatusException e) {
        HttpStatus status = HttpStatus.valueOf(e.getStatusCode().value());
        return problem(
                status,
                status.getReasonPhrase(),
                e.getReason() == null ? status.getReasonPhrase() : e.getReason(),
                status == HttpStatus.BAD_REQUEST ? "validation-failed" : "request-failed");
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail onUnexpected(Exception e) {
        // Logged in full, returned as nothing: the trace belongs in the logs.
        log.error("unhandled_exception", e);
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal error",
                "An unexpected error occurred; the trace id identifies it in the server logs",
                "internal-error");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail, String type) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create(BASE + type));
        String traceId = MDC.get("traceId");
        problem.setProperty("traceId", traceId == null ? "unavailable" : traceId);
        return problem;
    }
}
