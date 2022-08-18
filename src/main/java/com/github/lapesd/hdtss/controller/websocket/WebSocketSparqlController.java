package com.github.lapesd.hdtss.controller.websocket;

import io.micronaut.context.annotation.Requires;
import io.micronaut.websocket.CloseReason;
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
    private final boolean debug = log.isDebugEnabled();

    @OnOpen public void onOpen(@NonNull WebSocketSession session) {
        log.trace("onOpen({})", session.getId());
        sessionMap.put(session.getId(), new SparqlSession(context, session));
    }

    @OnMessage public void onMessage(@Nullable String msg, @NonNull WebSocketSession session) {
        if (msg == null || msg.isEmpty())
            return;
        SparqlSession ss = sessionMap.get(session.getId());
        if (ss == null) {
            log.warn("onMessage(): ignoring unknown session {}: {}", session.getId(), msg);
            assert false : "unknown session";
        } else {
            ss.receive(msg);
        }
    }

    @OnError public void onError(@NonNull WebSocketSession session, @Nullable Throwable err) {
        SparqlSession ss = sessionMap.get(session.getId());
        Object id = ss == null ? session.getId() : ss;
        if (debug)
            log.info("onError({}, {})", id, err, err);
        else
            log.info("onError({}, {})", id, err == null ? null : err.getClass().getName());
        if (ss != null)
            close(ss);
        else if (session.isOpen())
            session.close();
    }

    @OnClose public void onClose(@NonNull WebSocketSession session,
                                 @Nullable CloseReason reason) {
        SparqlSession ss = sessionMap.get(session.getId());
        if (ss == null) {
            log.debug("onClose({}, {}): untracked session", session.getId(), reason);
        } else {
            log.trace("onClose({}, {})", ss, reason);
            close(ss);
        }
    }

    private void close(SparqlSession ss) {
        //noinspection resource
        sessionMap.remove(ss.id());
        try {
            ss.close();
        } catch (Throwable t) {
            log.warn("Error closing {}: {}", ss, t.toString());
        }
    }

}
