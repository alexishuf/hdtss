package com.github.lapesd.hdtss.controller.exceptions;

import com.github.lapesd.hdtss.sparql.FeatureNotSupportedException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

@Produces(MediaType.TEXT_PLAIN)
@Singleton
public class FeatureNotSupportedExceptionHandler
        implements ExceptionHandler<FeatureNotSupportedException, HttpResponse<String>> {
    @Override
    public HttpResponse<String> handle(HttpRequest request, FeatureNotSupportedException ex) {
        return HttpResponse.badRequest(ex.reason());
    }
}
