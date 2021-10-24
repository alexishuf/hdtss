package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.sparql.FeatureNotSupportedException;
import com.github.lapesd.hdtss.sparql.SparqlSyntaxException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static io.micronaut.http.MediaType.TEXT_PLAIN_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class SparqlErrorHandler {

    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull SparqlSyntaxException ex) {
        return HttpResponse.badRequest("Syntax error in query: "+ex.reason())
                           .contentType(TEXT_PLAIN_TYPE);
    }

    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull FeatureNotSupportedException ex) {
        return HttpResponse.badRequest(ex.reason()).contentType(TEXT_PLAIN_TYPE);
    }


    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull RuntimeException e) {
        if (e instanceof FeatureNotSupportedException f)
            return handle(request, f);
        else if (e instanceof SparqlSyntaxException s)
            return handle(request, s);
        var msgBuilder = new StringBuilder().append(request.getMethod())
                .append(' ').append(request.getUri());
        request.getContentType().ifPresent(t -> msgBuilder.append("Content-Type: ").append(t));
        request.getBody()
                .ifPresent(b -> msgBuilder.append("Body:\n")
                        .append(String.join("\n  ", b.toString().split("\n")))
                        .append('\n'));
        request.getParameters().get("query", String.class)
                .ifPresent(q -> msgBuilder.append("Query:\n")
                        .append(String.join("\n  ", q.split("\n")))
                        .append('\n'));
        msgBuilder.append("Failed due to ").append(e);
        ByteArrayOutputStream bOs = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(bOs, true, UTF_8)) {
            e.printStackTrace(ps);
        }
        msgBuilder.append("Stack trace:\n")
                .append(String.join("\n  ", bOs.toString(UTF_8).split("\n")));
        return HttpResponse.serverError(msgBuilder.toString()).contentType(TEXT_PLAIN_TYPE);
    }
}
