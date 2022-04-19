package com.github.lapesd.hdtss.controller.websocket;

import io.micronaut.context.annotation.Requires;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.*;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@ServerWebSocket("/sparql")
@Requires(property = "sparql.websocket", pattern = "(?i)t(rue)?|1|on", defaultValue = "true")
@RequiredArgsConstructor
@Slf4j
public class WebSocketSparqlController {
    private final @NonNull SparqlSessionContext context;
    private final Map<String, SparqlSession> sessionMap = new ConcurrentHashMap<>();

    @OnOpen public void onOpen(@NonNull WebSocketSession session) {
        sessionMap.put(session.getId(), new SparqlSession(context, session));
    }

    @OnMessage public void onMessage(@Nullable String msg, @NonNull WebSocketSession session) {
        if (msg != null && !msg.isEmpty())
            sessionMap.get(session.getId()).receive(msg);
    }

    @OnError public void onError(@NonNull WebSocketSession session) { onClose(session); }

    @OnClose public void onClose(@NonNull WebSocketSession session) {
        sessionMap.get(session.getId()).close();
    }
}
