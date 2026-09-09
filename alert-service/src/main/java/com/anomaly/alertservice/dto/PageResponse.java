package com.anomaly.alertservice.dto;

import java.util.List;
import org.springframework.data.domain.Page;

/**
 * A page of results.
 *
 * <p>Spring's Page is not returned directly: its JSON shape is an
 * implementation detail of Spring Data and has changed between versions, which
 * would make it an accidental part of our API contract.
 */
public record PageResponse<T>(
        List<T> content, int page, int size, long totalElements, int totalPages, boolean last) {

    public static <T> PageResponse<T> of(Page<T> page) {
        return new PageResponse<>(
                page.getContent(),
                page.getNumber(),
                page.getSize(),
                page.getTotalElements(),
                page.getTotalPages(),
                page.isLast());
    }
}
