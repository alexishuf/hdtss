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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.github.lapesd.hdtss.controller.websocket.task.TaskTerminationListener.Cause.*;
import static java.lang.String.format;
import static java.lang.System.nanoTime;

@Slf4j
public abstract class AbstractQueryTask {
    protected final @NonNull SparqlSessionContext context;
    protected final @NonNull WebSocketSession session;
    private final @NonNull TaskTerminationListener onTermination;

    private enum State {
        CREATED,
        IDLE,
        SENDING,
        COMPLETE,
        FAILED,
        CANCELLED;

        boolean isTerminated() {
            return this.ordinal() > SENDING.ordinal();
        }
    }

    private long rows = 0;
    private @NonNull State state = State.CREATED;
    private int sending = 0;
    protected final StringBuilder buf = new StringBuilder(4096-256);
    protected QueryInfo.@Nullable Builder info;
    private final @NonNull Consumer<Throwable> sendErrorHandler;
    private final @NonNull Runnable sendSuccessHandler;


    public AbstractQueryTask(@NonNull SparqlSessionContext ctx, @NonNull WebSocketSession session,
                             @NonNull TaskTerminationListener onTermination) {
        this.context = ctx;
        this.session = session;
        this.onTermination = onTermination;
        this.sendErrorHandler = t -> {synchronized (this) {
            leaveSending();
            handleError(t);
        }};
        this.sendSuccessHandler = this::leaveSending;
    }

    public synchronized void start() {
        switch (state) {
            case CREATED   -> state = State.IDLE;
            case IDLE      -> throw new IllegalStateException("Already started");
            case CANCELLED -> log.debug("{}: cancel() before start(), not starting", this);
            default        -> throw new IllegalStateException("Completed/failed before start()");
        }
        log.debug("{} started", this);
        info = doStart();
    }

    /** Start task execution. Will be called at most once. */
    protected abstract QueryInfo.@Nullable Builder doStart();

    /** Calls {@code onTermination()} with a rows count. Does not enforce the "call once" rule. */
    protected void notifyTermination(boolean cancelled, @Nullable Throwable error) {
        log.debug("{} terminated", this);
        if (info != null)
            context.executor().notify(info.cancelled(cancelled).error(error).rows(rows).build());
        onTermination.onTerminate(cancelled ? CANCEL : error == null ? COMPLETION : FAILED);
    }

    /**
     * Starts cancellation of this task if it has not yet terminated. Will send
     * {@code !cancelled\n} to the peer if headers have already been sent.
     *
     * Once the task completes, {@code onTermination()} will be called. Note that the task may
     * terminate for other reasons before the cancel takes effect.
     */
    public synchronized void cancel() {
        // wait until not sending
        boolean interrupted = false;
        while (state == State.SENDING) {
            try { wait(); } catch (InterruptedException e) { interrupted = true; }
        }
        if (!state.isTerminated()) { // then transition to CANCELLED
            state = State.CANCELLED;
            context.sendAsync(session, "!cancelled\n", null, null);
            notifyTermination(true, null);
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    @Override public @NonNull String toString() {
        String className = getClass().getSimpleName();
        int hash = System.identityHashCode(this);
        return format("%s@%x[%s, %s]", className, hash, state.name(), session.getId());
    }

    /**
     * Recursively enters sending state.
     *
     * @return true if could recursively enter the sending state, false if already terminated.
     * @throws IllegalStateException if called before {@link AbstractQueryTask#start()}
     */
    private synchronized boolean enterSending() {
        if (state == State.IDLE || state == State.SENDING) {
            state = State.SENDING;
            ++sending;
            return true;
        } else if (!state.isTerminated()) {
            throw new IllegalStateException("cannot send from state="+state);
        } else {
            return false; // terminated
        }
    }

    /** Reverses a previous true-returning {@link AbstractQueryTask#enterSending()} */
    private synchronized void leaveSending() {
        if (--sending == 0 && state == State.SENDING) {
            state = State.IDLE;
            notifyAll();
        }
    }

    /** Sends a message without waiting for it to be flushed or for it to fail. */
    protected synchronized void sendAsync(@NonNull CharSequence msg) {
        if (enterSending()) {
            try {
                context.sendAsync(session, msg, sendSuccessHandler, sendErrorHandler);
            } finally {
                leaveSending();
            }
        } else {
            log.debug("{} is terminated, not sending {}", this, msg);
        }
    }


    /** Helper to send messages blocking until they are flushed or sending fails */
    protected class SyncSender {
        private final Semaphore done = new Semaphore(0);
        private @Nullable Throwable error;
        private final Runnable            onComplete = () -> { leaveSending(); done.release(); };
        private final Consumer<Throwable> onError    =
                t -> { sendErrorHandler.accept(error = t); done.release(); };

        /**
         * Send the message and return true if the task has not terminated.
         *
         * @return {@code true} if the message was sent, {@code false} if this task is already
         *         terminated (the message is not sent in this case).
         * @throws RuntimeException if sending fails despite the task being not terminated.
         */
        public boolean send(@NonNull CharSequence msg) {
            synchronized (AbstractQueryTask.this) {
                if (enterSending()) {
                    try {
                        context.sendAsync(session, msg, onComplete, onError);
                    } catch (Throwable t) { leaveSending(); handleError(t); }
                } else {
                    return false;
                }
            }
            done.acquireUninterruptibly();
            if      (error instanceof RuntimeException) throw (RuntimeException) error;
            else if (error instanceof Error)            throw (Error) error;
            else if (error != null)                     throw new RuntimeException(error);
            return true;
        }
    }

    /** Sends the TSV var names header row and calls {@code after.run()} afterwards. */
    protected void sendHeaders(@NonNull SyncSender sender, @NonNull List<String> vars) {
        StringBuilder b = new StringBuilder();
        for (String name : vars)
            b.append('?').append(name).append('\t');
        b.setLength(Math.max(1, b.length()) - 1);
        b.append('\n');
        sender.send(b);
    }

    /** Idempotent notify termination of this task. Calls {@code notifyTermination();(false, error)}. */
    protected synchronized void terminate(@Nullable Throwable error) {
        if (state.isTerminated()) {
            log.debug("{}: Ignoring terminate({})", this, error);
        } else {
            state = error == null ? State.COMPLETE : State.FAILED;
            notifyTermination(false, error);
        }
    }

    private static final class WindowGenerator implements Runnable {
        private static final Term[] END = new Term[] {new Term("<END>")};
        private final @NonNull BlockingQueue<@Nullable Term @NonNull[]> queue;
        private final @NonNull Iterator<@Nullable Term @NonNull[]> it;
        private final @NonNull AbstractQueryTask parent;
        private final int windowSize;
        private final long windowNanos;

        public WindowGenerator(@NonNull AbstractQueryTask parent,
                               @NonNull QuerySolutions solutions) {
            this.parent = parent;
            this.windowSize = parent.context.windowRows();
            this.windowNanos = parent.context.windowNanos();
            this.queue = new ArrayBlockingQueue<>(windowSize*2);
            this.it = solutions.iterator();
        }

        @Override public void run() {
            while (it.hasNext())
                put(it.next());
            put(END);
        }

        private void put(@Nullable Term @NonNull [] row) {
            for (boolean done = false; !done;) {
                try { queue.put(row); done = true; } catch (InterruptedException ignored) { }
            }
        }

        /**
         * Wait at most {@code nanos} for a row, add it to {@code out} and return remaining nanos.
         *
         * @return {@code nanos-elapsedNs} if at least one row was serialized, or
         *         {@link Long#MIN_VALUE} if there are no more rows to wait for.
         */
        private long take(@NonNull StringBuilder out, long nanos) {
            assert nanos > 0;
            long start = nanoTime();
            @Nullable Term @Nullable [] row = null;
            while (row == null) {
                try {
                    row = queue.poll(nanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) { }
            }
            if (row == END) {
                put(END);
                return Long.MIN_VALUE;
            }
            parent.serializeRow(out, row);
            return nanos-(nanoTime()-start);
        }

        /**
         * Wait for the next rows window and serialize it to {@code out}.
         * Get the next non-empty serialized window of rows.
         *
         * This method will wait:
         * <ul>
         *     <li>without limit until either it meets a row or the solutions end.</li>
         *     <li>at most {@link SparqlSessionContext#windowNanos()}, counting from the
         *         method entry, for additional rows up to
         *         {@link SparqlSessionContext#windowRows()}.</li>
         * </ul>
         *
         * @return {@code true} iff at least one row was written to out, false if solutions ended.
         * */
        public boolean get(@NonNull StringBuilder out) {
            long start = nanoTime();
            assert out.length() == 0 : "concurrent use of buffer";
            if (take(out, Long.MAX_VALUE) == Long.MIN_VALUE)
                return false; // reached end, no row to serialize.
            long rem = windowNanos-(nanoTime()-start);
            for (int n = 1; rem > 999 && n < windowSize; ++n)
                rem = take(out, rem);
            assert !out.isEmpty();
            return true;
        }
    }

    private void serializeRow(@NonNull StringBuilder out, @Nullable Term @NonNull[] row) {
        ++rows;
        unaccountedSerializeRow(out, row);
    }

    protected void unaccountedSerializeRow(@NonNull StringBuilder out,
                                           @Nullable Term @NonNull[] row) {
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
    protected boolean serialize(@NonNull SyncSender sender, @NonNull QuerySolutions solutions) {
        try {
            if (context.windowEnabled()) {
                var gen = new WindowGenerator(this, solutions);
                context.executor().scheduler().schedule(gen);
                while (gen.get(buf)) {
                    sender.send(buf);
                    buf.setLength(0);
                }
            } else {
                for (Term[] row : solutions) {
                    assert buf.length() == 0 : "Concurrent use of buf";
                    serializeRow(buf, row);
                    sender.send(buf);
                    buf.setLength(0);
                }
            }
            return true;
        } catch (Throwable t) {
            handleError(t);
            return false;
        }
    }

    protected synchronized void handleError(@Nullable Throwable err) {
        if (err == null)
            return;
        String msg = "!error "+ err.toString().replaceAll("\\s+", " ");
        if (state.isTerminated()) {
            log.debug("{}: Ignoring {} after terminated", this, err.getClass().getSimpleName());
        } else {
            state = State.FAILED;
            context.sendAsync(session, msg, null, null);
            notifyTermination(false, err);
        }
    }

    private void waitForNotSending() {
        boolean interrupted = false;
        while (state == State.SENDING) {
            try {
                wait();
            } catch (InterruptedException e) { interrupted = true; }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

}
