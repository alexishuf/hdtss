package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.controller.websocket.task.AbstractQueryTask;
import com.github.lapesd.hdtss.controller.websocket.task.BindTask;
import com.github.lapesd.hdtss.controller.websocket.task.QueryTask;
import com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener;
import com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener.Cause;
import com.github.lapesd.hdtss.model.Term;
import io.micronaut.websocket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.List;

@Slf4j
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
                    ctx.sendSync(session, "!error "+reason.replace("\n", " "));
            } catch (Throwable e) {
                log.error("Failed to send !error {} message", reason);
            } finally {
                if (session.isOpen())
                    session.close();
            }
        }
    };
    private final @NonNull SparqlSessionContext ctx;
    private final @NonNull WebSocketSession session;
    private final @NonNull ArrayDeque<Action> queue;
    private final @NonNull TaskTerminationListener onTermination;
    private @Nullable AbstractQueryTask task = null;
    private boolean closed;

    public SparqlSession(@NonNull SparqlSessionContext ctx,
                         @NonNull WebSocketSession session) {
        this.session = session;
        this.ctx = ctx;
        this.queue = new ArrayDeque<>(ctx.actionQueueCapacity());
        this.onTermination = cause -> {
            synchronized (this) {
                task = null;
                if      (closed)                session.close();
                else if (cause == Cause.FAILED) close();
                else                            handleNext();
            }
        };
    }

    /** Process a WebSocket message sent by a client. */
    public synchronized void receive(@NonNull String msg) {
        if (ctx.tracing())
            log.trace("{} >>> {}", session.getId(), msg.replace("\n", "\\n"));
        messageParser.parse(msg);
        handleNext();
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

    /** If there is no active task, dequeue and start execution. */
    private void handleNext() {
        while (task == null) {
            Action ac = queue.poll();
            if (ac == null) {
                break;
            } else if (ac instanceof Action.QueueCap) {
                String msg = "!action-queue-cap=" + ctx.actionQueueCapacity() + "\n";
                try {
                    ctx.sendSync(session, msg);
                } catch (Throwable err) {
                    log.warn("Failed to send {}: {}", msg, err.toString());
                    close();
                }
            } else if (ac instanceof Action.Query q) {
                task = new QueryTask(ctx, session, onTermination, q.sparql());
                task.start();
            } else if (ac instanceof Action.Bind b) {
                task = new BindTask(ctx, session, onTermination, b.sparql());
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
