package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;

import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j
@RequiredArgsConstructor
public abstract class SimpleChunkedPublisher implements ChunkedPublisher {
    private static final @NonNull TerminationListener DEF_TERM_LISTENER
            = (err, cancel, rows, items, nanos) -> {
        long ms = NANOSECONDS.toMillis(nanos);
        if (err != null) {
            log.trace("ChunkedPublisher failed with {} after {} rows, {} items and {}ms",
                      err, rows, items, ms);
        } else {
            log.trace("ChunkedPublisher {} after {} rows, {} items and {}ms",
                      cancel ? "cancelled" : "completed", rows, items, ms);
        }
    };

    protected final @NonNull QuerySolutions solutions;
    private @Nullable Subscriber<? super byte[]> subscriber;
    private @NonNull TerminationListener terminationListener = DEF_TERM_LISTENER;

    @Override public @NonNull ChunkedPublisher onTermination(TerminationListener listener) {
        terminationListener = listener;
        return this;
    }

    @Override public void subscribe(Subscriber<? super byte[]> s) {
        if (subscriber != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException(this+" already subscribed by "+subscriber));
        } else {
            long startNanos = nanoTime();
            subscriber = s;
            AbstractSubscription subscription;
            if (solutions instanceof FluxQuerySolutions)
                subscription = solutions.isAsk() ? new AskFlux(s) : new RowsFlux(s);
            else
                subscription = solutions.isAsk() ? new AskIt(s) : new RowsIt(s);
            subscription.startNanos = startNanos;
            s.onSubscribe(subscription);
        }
    }

    @Override public String toString() {
        return String.format("%s@%x", getClass().getSimpleName(), identityHashCode(this));
    }

    protected abstract byte[] askPrologue();
    protected abstract byte[] askBodyAndPrologue(boolean result);
    protected abstract byte[] rowsPrologue();
    protected abstract byte[] rowBytes(@NonNull SolutionRow row);
    protected abstract byte[] rowsEpilogue();
    protected abstract void   release();

    @RequiredArgsConstructor
    private abstract class AbstractSubscription implements Subscription, Subscriber<SolutionRow> {
        protected enum State {
            UNINITIALIZED,
            INITIALIZED,
            PROLOGUE_SENT,
            PENDING_EPILOGUE,
            LAST_SEGMENT,
            TERMINATED
        }

        protected final @NonNull Subscriber<? super byte[]> downstream;
        protected long requested, rows, items, startNanos;
        private boolean requesting = false, released = false;
        protected @Nullable Subscription upstream;
        protected State state = State.UNINITIALIZED;

        @Override public void onSubscribe(Subscription s) {
            (upstream = s).request(requested);
        }
        @Override public void onNext(SolutionRow row) {
            ++rows;
            if (state != State.TERMINATED) feed(rowBytes(row));
        }
        @Override public void onError(Throwable t) {
            if (state != State.TERMINATED) failDownstream(t);
        }
        @Override public void onComplete() {
            assert state.ordinal() >= State.INITIALIZED.ordinal();
            if (state != State.TERMINATED) {
                if (requested > 0)
                    feedLast(rowsEpilogue());
                else
                    state = State.PENDING_EPILOGUE;
            }
        }

        protected boolean feed(byte[] bytes) {
            assert state != State.UNINITIALIZED && state != State.TERMINATED;
            assert requested > 0;
            if (bytes.length == 0)
                return true; // do not feed empty arrays, that's useless
            requested = Math.max(0, requested-1);
            try {
                ++items;
                downstream.onNext(bytes);
                return true;
            } catch (Throwable t) {
                log.warn("{}: Treating {} from {}.onNext() as cancel()", this, t, downstream, t);
                cancel();
                return false;
            }
        }

        private void callReleaseOnce() {
            if (!released) {
                released = true;
                release();
            }
        }

        protected void feedLast(byte[] bytes) {
            state = State.LAST_SEGMENT;
            feed(bytes);
            completeDownstream();
            callReleaseOnce();
        }

        private void notifyTermination(@Nullable Throwable error, boolean cancelled) {
            terminationListener.onTerminate(error, cancelled, rows, items, nanoTime()-startNanos);
        }

        protected void failDownstream(Throwable error) {
            boolean isFirst = state != State.TERMINATED;
            try {
                state = State.TERMINATED;
                downstream.onError(error);
            } catch (Throwable t) {
                log.warn("{}: Ignoring {} from {}.onError({})", this, t, downstream, error, t);
            } finally {
                callReleaseOnce();
                if (isFirst) notifyTermination(error, false);
            }
        }

        protected void completeDownstream() {
            boolean isFirst = state != State.TERMINATED;
            try {
                state = State.TERMINATED;
                downstream.onComplete();
            } catch (Throwable t) {
                log.warn("{}: Ignoring {} from {}.onComplete()", this, t, downstream, t);
            } finally {
                callReleaseOnce();
                if (isFirst) notifyTermination(null, false);
            }
        }

        protected abstract void init(long n);
        protected abstract void onRequest(long n, boolean reentrant);

        @Override public void request(long n) {
            if (n < 0) {
                failDownstream(new IllegalArgumentException("Negative request(): "+n));
            } else if (n > 0) {
                boolean reentrant = requesting;
                if (!reentrant)
                    requesting = true;
                try {
                    requested += n;
                    if (state == State.UNINITIALIZED) {
                        state = State.INITIALIZED;
                        if (reentrant) assert false : "state=UNINITIALIZED with ongoing request()";
                        else init(n);
                    } else if (state == State.PENDING_EPILOGUE) {
                        feedLast(rowsEpilogue());
                    } else {
                        onRequest(n, reentrant);
                    }
                } finally {
                    if (!reentrant)
                        requesting = false;
                }
            }
        }

        @Override public void cancel() {
            log.trace("{}.cancel()", this);
            boolean isFirst = state != State.TERMINATED;
            state = State.TERMINATED;
            if (upstream != null)
                upstream.cancel();
            if (isFirst) notifyTermination(null, true);
        }

        @Override public String toString() {
            return SimpleChunkedPublisher.this +"$"+getClass().getSimpleName();
        }
    }

    private class AskFlux extends AbstractSubscription implements Subscriber<SolutionRow> {
        private @Nullable Boolean askResult;

        public AskFlux(@NonNull Subscriber<? super byte[]> downstream) { super(downstream); }

        @Override public void onSubscribe(Subscription s)     { (upstream = s).request(1); }
        @Override public void onComplete()                    {         offerResult(false); }
        @Override public void onNext(SolutionRow solutionRow) { ++rows; offerResult(true); }

        @Override protected void init(long n) {
            state = State.PROLOGUE_SENT;
            if (feed(askPrologue()))
                solutions.flux().subscribe(this);
        }

        @Override protected void onRequest(long n, boolean reentrant) {
            if (askResult != null)
                offerResult(askResult);
        }

        private void offerResult(boolean offer) {
            if (askResult == null)
                askResult = offer;
            if (state == State.PROLOGUE_SENT && requested > 0)
                feedLast(askBodyAndPrologue(askResult));
        }
    }


    private class AskIt extends AbstractSubscription {
        private final @NonNull Iterator<SolutionRow> it = solutions.iterator();

        public AskIt(@NonNull Subscriber<? super byte[]> downstream) { super(downstream); }

        @Override protected void init(long n) {
            state = State.PROLOGUE_SENT;
            feed(askPrologue());
            if (requested > 0)
                onRequest(n-1, false);
        }

        @Override protected void onRequest(long n, boolean reentrant) {
            if (!reentrant) {
                boolean result;
                try {
                    result = it.hasNext();
                    rows += result ? 1 : 0;
                } catch (Throwable t) {
                    failDownstream(t);
                    return;
                }
                feedLast(askBodyAndPrologue(result));
            }
        }
    }

    private class RowsFlux extends AbstractSubscription implements Subscriber<SolutionRow> {
        public RowsFlux(@NonNull Subscriber<? super byte[]> downstream) { super(downstream); }

        @Override protected void init(long n) {
            state = State.PROLOGUE_SENT;
            if (feed(rowsPrologue()))
                solutions.flux().subscribe(this);
        }

        @Override protected void onRequest(long n, boolean reentrant) {
            if (upstream != null)
                upstream.request(n);
        }
    }

    private class RowsIt extends AbstractSubscription {
        private final @NonNull Iterator<SolutionRow> it = solutions.iterator();

        public RowsIt(@NonNull Subscriber<? super byte[]> downstream) { super(downstream); }

        @Override protected void init(long n) {
            state = State.PROLOGUE_SENT;
            feed(rowsPrologue());
            if (requested > 0)
                onRequest(n-1, false);
        }

        @Override protected void onRequest(long n, boolean reentrant) {
            if (!reentrant) {
                try {
                    while (requested > 0 && it.hasNext()) {
                        byte[] bytes = rowBytes(it.next());
                        ++rows;
                        if (!feed(bytes))
                            return;
                    }
                } catch (Throwable t) {
                    failDownstream(t);
                    return;
                }
                if (requested > 0) //implies it.hasNext() == false
                    feedLast(rowsEpilogue());
            }
        }
    }

}
