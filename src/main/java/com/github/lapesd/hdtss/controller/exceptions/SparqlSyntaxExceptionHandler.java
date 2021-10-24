package com.github.lapesd.hdtss.controller.exceptions;

import com.github.lapesd.hdtss.sparql.SparqlSyntaxException;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

@Produces({MediaType.TEXT_PLAIN, SparqlMediaTypes.RESULTS_TSV})
@Singleton
public class SparqlSyntaxExceptionHandler
        implements ExceptionHandler<SparqlSyntaxException, HttpResponse<String>> {
    @Override
    public HttpResponse<String> handle(HttpRequest request, SparqlSyntaxException ex) {
        return HttpResponse.badRequest("Syntax error in query: "+ex.reason());
    }
}
