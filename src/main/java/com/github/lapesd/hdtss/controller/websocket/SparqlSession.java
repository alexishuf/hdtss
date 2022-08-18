package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.controller.websocket.task.AbstractQueryTask;
import com.github.lapesd.hdtss.controller.websocket.task.BindTask;
import com.github.lapesd.hdtss.controller.websocket.task.QueryTask;
import com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener;
import com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener.Cause;
import com.github.lapesd.hdtss.model.Term;
import io.micronaut.websocket.WebSocketSession;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.List;

import static io.micronaut.http.MediaType.TEXT_PLAIN_TYPE;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j @Accessors(fluent = true)
public class SparqlSession implements AutoCloseable {
    private final @NonNull MessageParser messageParser = new MessageParser() {
        @Override protected void onAction(@NonNull Action action) {
            if (action instanceof Action.Cancel) {
                if      (!queue.isEmpty()) queue.removeLast();
                else if (task != null)     task.cancel();
            } else {
                queue.add(action);
            }
        }
        @Override protected void onRow(@Nullable Term @NonNull[]  row) throws ProtocolException {
            if (!(task instanceof BindTask b))
                throw new ProtocolException("No !bind action active");
            b.receiveBinding(row);
        }
        @Override protected void onVars(@NonNull List<String> vars) throws ProtocolException {
            if (!(task instanceof BindTask b))
                throw new ProtocolException("No !bind action active");
            b.receiveVarNames(vars);
        }
        @Override protected void onEndRows() throws ProtocolException {
            if (!(task instanceof BindTask b))
                throw new ProtocolException("No !bind action active");
            b.receiveBindingsEnd();
        }
        @Override protected void onError(String reason) {
            try {
                if (session.isOpen())
                    send("!error "+reason.replace("\n", " "));
            } catch (Throwable e) {
                log.error("Failed to send !error {} message", reason);
            } finally {
                if (session.isOpen())
                    session.close();
            }
        }
        @Override protected void onPing() {
            session.sendAsync("!ping-ack\n");
        }
        @Override protected void onPingAck() {
            nextPing = nanoTime() + context.pingIntervalNanos();
        }
    };
    @Getter private final @NonNull SparqlSessionContext context;
    private final @NonNull WebSocketSession session;
    private final @NonNull ArrayDeque<Action> queue;
    private final @NonNull TaskTerminationListener onTermination;
    private @Nullable AbstractQueryTask task = null;
    private volatile long nextPing;
    private boolean closed;

    public SparqlSession(@NonNull SparqlSessionContext ctx,
                         @NonNull WebSocketSession session) {
        this.session = session;
        this.context = ctx;
        this.queue = new ArrayDeque<>(ctx.actionQueueCapacity());
        this.nextPing = nanoTime() + ctx.pingIntervalNanos();
        checkPing(); // will not ping but will schedule future check
        this.onTermination = cause -> {
            synchronized (this) {
                task = null;
                if      (closed)                session.close();
                else if (cause == Cause.FAILED) close();
                else                            handleNext();
            }
        };
    }

    /** Get the WebSocket session id */
    public String id() { return session.getId(); }

    /** Process a WebSocket message sent by a client. */
    public synchronized void receive(@NonNull String msg) {
        if (context.tracing())
            log.trace("{} >>> {}", session.getId(), msg.replace("\n", "\\n"));
        nextPing = nanoTime() + context.pingIntervalNanos();
        messageParser.parse(msg);
        handleNext();
    }

    /** Synchronously send a message through the WebSocket session */
    public void send(@NonNull CharSequence msg) {
        if (context.tracing())
            log.trace("{} <<< {}", session.getId(), msg.toString().replace("\n", "\\n"));
        nextPing = nanoTime() + context.pingIntervalNanos();
        session.sendSync(msg, TEXT_PLAIN_TYPE);
    }

    /** Cancel all queued requests, cancel the current task and close the session. */
    @Override public synchronized void close() {
        if (closed)
            return;
        closed = true;
        queue.clear();
        if (task != null)
            task.cancel(); // will call session.close() from onTermination
        else
            session.close();
    }

    @Override public String toString() {
        return "SparqlSession["+session.getId()+"]{"
                + (closed ? "" : "!") + "closed, #queue=" + queue.size() + ", task="+task+'}';
    }

    /** Send a !ping if needed, else schedule a future check from nextPing current value. */
    private void checkPing() {
        if (closed)
            return;
        if (nextPing < nanoTime()) {
            if (context.tracing())
                log.trace("{} <<< {}", session.getId(), "!ping\\n");
            nextPing = nanoTime() + context.pingIntervalNanos();
            session.sendAsync("!ping\n", TEXT_PLAIN_TYPE);
        }
        long delay = nextPing - nanoTime();
        context.executor().scheduledExecutor().schedule(this::checkPing, delay, NANOSECONDS);
    }

    /** If there is no active task, dequeue and start execution. */
    private void handleNext() {
        while (task == null) {
            Action ac = queue.poll();
            if (ac == null) {
                break;
            } else if (ac instanceof Action.QueueCap) {
                String msg = "!action-queue-cap=" + context.actionQueueCapacity() + "\n";
                try {
                    send(msg);
                } catch (Throwable err) {
                    log.warn("Failed to send {}: {}", msg, err.toString());
                    close();
                }
            } else if (ac instanceof Action.Query q) {
                task = new QueryTask(this, onTermination, q.sparql());
                task.start();
            } else if (ac instanceof Action.Bind b) {
                task = new BindTask(this, onTermination, b.sparql());
                task.start();
            } else if (ac instanceof Action.Cancel) {
                log.error("Cancel action should not have been queued");
                assert false : "Action.Cancel in queue";
            } else {
                throw new IllegalArgumentException("Unexpected action: "+ac);
            }
        }
    }
}
