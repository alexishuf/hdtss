package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public interface ChunkedPublisher extends Publisher<byte[]> {
    @FunctionalInterface
    interface TerminationListener {
        /**
         * Notify that the {@link ChunkedPublisher}'s single subscription terminated, either
         * by a {@link Subscription#cancel()}, a {@link Subscriber#onComplete()} or a
         * {@link Subscriber#onError(Throwable)} event. Completion or error events after a cancel
         * are not reported, only the cancel will cause a call to this method.
         *
         *
         * @param error error passed to {@link Subscriber#onError(Throwable)} or null if the
         *              termination was caused by other event.
         * @param cancelled {@code true} iff the termination was caused by a
         *                  {@link Subscription#cancel()} call (explicitly or by a downstream
         *                  {@link Subscriber} throwing from one of its event handling methods).
         * @param rows how many {@link SolutionRow} where received from the upstream source.
         * @param items how many {@code byte[]} fragments where sent downstream.
         * @param nanosSinceSubscribe how many nanoseconds have elapsed since the call to
         *                            {@link ChunkedPublisher#subscribe(Subscriber)}.
         */
        void onTerminate(@Nullable Throwable error, boolean cancelled, long rows, long items,
                         long nanosSinceSubscribe);
    }

    @NonNull ChunkedPublisher onTermination(TerminationListener listener);
}
