package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.TSVCodec;
import io.micronaut.websocket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener.Cause.*;

@Slf4j
public abstract class AbstractQueryTask {
    protected final SparqlSessionContext context;
    protected final String sessionId;
    protected final MessageSerializer serializer;
    protected final StringBuilder buf = new StringBuilder(4096-256);

    /* --- --- --- mutable state --- --- --- */
    private long rows = 0;
    protected QueryInfo.@Nullable Builder info;
    private volatile boolean normalCompletion = false;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean cancelled = new AtomicBoolean();

    public AbstractQueryTask(@NonNull SparqlSessionContext ctx, @NonNull WebSocketSession session,
                             @NonNull TaskTerminationListener onTermination) {
        this.context = ctx;
        this.sessionId = session.getId();
        this.serializer = new MessageSerializer(ctx.windowNanos(), ctx.windowRows(), session) {
            @Override public void onCompletion(@Nullable Throwable e) {
                if (e != null)
                    log.debug("{}.serializer.onCompletion({})", AbstractQueryTask.this, e.toString());
                if (info != null) {
                    QueryInfo queryInfo = info.cancelled(cancelled.get() && !normalCompletion)
                                              .error(e).rows(rows).build();
                    context.executor().notify(queryInfo);
                }
                var c = e != null ? FAILED : (normalCompletion ? COMPLETION : CANCEL);
                cleanup(c);
                onTermination.onTerminate(c);
            }
        };
        context.executor().scheduler().schedule(this.serializer);
    }

    protected void cleanup(TaskTerminationListener.Cause cause) {
        log.trace("{}.cleanup({})", this, cause);
    }

    public void start() {
        log.trace("{}.start()", this);
        if (!started.compareAndSet(false, true))
            throw new IllegalStateException("Already started");
        assert !normalCompletion : "completed before start!";
        if (cancelled.get()) {
            log.debug("cancel() before start(), will not start()");
        } else {
            info = doStart();
        }
    }

    /** Start task execution. Will be called at most once. */
    protected abstract QueryInfo.@Nullable Builder doStart();

    /**
     * Starts cancellation of this task if it has not yet terminated. Will send
     * {@code !cancelled\n} to the peer if headers have already been sent.
     *
     * Once the task completes, {@code onTermination()} will be called. Note that the task may
     * terminate for other reasons before the cancel takes effect.
     */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.trace("{}.cancel(): sending !cancelled", this);
            serializer.end("!cancelled", null);
        } else {
            log.trace("{}.cancel(): ignoring", this);
        }
    }

    @Override public @NonNull String toString() {
        return getClass().getSimpleName() + '[' + sessionId +
                (normalCompletion ? ",completed"  : ", !completed") +
                (cancelled.get()  ? ", cancelled" : "") +
                (started.get()    ? ""            : ", !started") + ']';
    }

    protected void sendEnd() {
        normalCompletion = true;
        serializer.end("!end\n", null);
    }

    /** Sends the TSV var names header row and calls {@code after.run()} afterwards. */
    protected boolean sendHeaders(@NonNull List<String> vars) {
        assert buf.length() == 0 : "Concurrent use of buf";
        for (String name : vars)
            buf.append('?').append(name).append('\t');
        buf.setLength(Math.max(1, buf.length()) - 1);
        buf.append('\n');
        boolean ok = serializer.send(buf);
        buf.setLength(0);
        return ok;
    }

    protected void serializeRow(@NonNull StringBuilder out, @Nullable Term @NonNull[] row) {
        for (@Nullable Term term : row) {
            if (term != null)
                out.append(TSVCodec.sanitize(term));
            out.append('\t');
        }
        if (row.length > 0)
            out.setLength(out.length()-1);
        out.append('\n');
    }

    /** Send all rows of in {@code solutions} and return true if no error occurred. */
    protected boolean serialize(@NonNull QuerySolutions solutions) {
        boolean ok = true;
        for (Term[] row : solutions) {
            assert buf.length() == 0 : "Concurrent use of buf";
            serializeRow(buf, row);
            ++rows;
            ok = serializer.send(buf);
            buf.setLength(0);
            if (!ok) break;
        }
        if (!ok)
            log.trace("{}.serialize(): stopped due to closed MessageSerializer", this);
        return ok;
    }
}
