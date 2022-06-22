package com.github.lapesd.hdtss.controller.websocket.task;

import io.micronaut.websocket.WebSocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.micronaut.http.MediaType.TEXT_PLAIN_TYPE;
import static java.lang.System.nanoTime;

@Slf4j
@RequiredArgsConstructor
public abstract class MessageSerializer implements Runnable {
    private static final String END_MESSAGE = MessageSerializer.class.getName()+"::END";

    private final long windowTimeoutNanos;
    private final int maxMessagesInWindow;
    private final boolean trace = log.isTraceEnabled();
    protected final WebSocketSession session;
    private final ReentrantLock lock = new ReentrantLock(false);
    private @MonotonicNonNull Throwable injectedException;
    private boolean open = true;
    private final Condition enqueued = lock.newCondition();
    private final ArrayDeque<String> queue = new ArrayDeque<>();

    /**
     * Called when {@link MessageSerializer#run()} finishes.
     * @param error the error that caused the termination
     */
    public abstract void onCompletion(@Nullable Throwable error);

    /** Used only for unit testing */
    protected boolean queueEmpty() { return queue.isEmpty(); }

    /** Used only for testing */
    protected boolean isOpen() { return open; }

    /**
     * Send the given message through the {@link WebSocketSession} asynchronously.
     *
     * @param message The message to send. {@link Object#toString()} will be called to ensure
     *                the queued message is immutable.
     *
     * @return {@code true} if the message was queued, {@code false}
     *         if {@link MessageSerializer#end(CharSequence, Throwable)} has been previously called.
     */
    public boolean send(CharSequence message) {
        lock.lock();
        try {
            queue.add(message.toString());
            return open;
        } finally {
            enqueued.signalAll();
            lock.unlock();
        }
    }

    /**
     * Close this serializer, optionally send a final message
     *
     * @param message The optional last message to send. If non-null and not a {@link String},
     *                {@link Object#toString()} will be called so that the message does not
     *                mutate after being queued.
     * @param cause Optional exception to be delivered to
     *              {@link MessageSerializer#onCompletion(Throwable)}.
     * @return {@code true} iff this is the first {@code end()} call.
     */
    public boolean end(@Nullable CharSequence message, @Nullable Throwable cause) {
        lock.lock();
        try {
            if (!open)
                return false;
            if (message != null)
                queue.add(message.toString());
            if (cause != null)
                injectedException = cause;
            queue.add(END_MESSAGE);
            open = false;
            return true;
        } finally {
            enqueued.signalAll();
            lock.unlock();
        }
    }

    private String take() {
        lock.lock();
        try {
            while (queue.isEmpty()) enqueued.awaitUninterruptibly();
            return queue.remove();
        } finally {
            lock.unlock();
        }
    }

    private @Nullable String poll(long nanos) {
        lock.lock();
        try {
            while (nanos > 0 && queue.isEmpty()) {
                try { nanos = enqueued.awaitNanos(nanos); } catch (InterruptedException ignored) {}
            }
            return queue.isEmpty() ? null : queue.remove();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Run a loop consuming queued messages with {@link MessageSerializer#send(CharSequence)}
     * bundling multiple messages together within the allowed window time and size when possible.
     */
    @SuppressWarnings("StringEquality")
    @Override public final void run() {
        String originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName("MessageSerializer-"+session.getId());
        StringBuilder sb = new StringBuilder();
        Throwable cause = null;
        try {
            for (boolean end = false; !end; ) {
                long start = nanoTime(), elapsed = 0;
                String singleMessage = take();
                if (singleMessage == END_MESSAGE)
                    break;
                for (int n = 1; !end && elapsed < windowTimeoutNanos && n < maxMessagesInWindow
                              ; n++, elapsed = nanoTime()-start) {
                    String msg = poll(windowTimeoutNanos-elapsed);
                    if (msg != null) {
                        end = msg == END_MESSAGE;
                        if (!end) {
                            if (singleMessage != null) {
                                sb.setLength(0);
                                sb.append(singleMessage);
                                singleMessage = null;
                            }
                            sb.append(msg);
                        }
                    }
                }
                String effMessage = singleMessage == null ? sb.toString() : singleMessage;
                if (trace)
                    log.trace("{} << {}", session.getId(), effMessage.replace("\n", "\\n"));
                session.sendSync(effMessage, TEXT_PLAIN_TYPE);
            }
        } catch (Throwable t) {
            cause = t;
        } finally {
            onCompletion(cause == null ? injectedException : cause);
            Thread.currentThread().setName(originalThreadName);
        }
    }

    @Override public String toString() {
        return "MessageSerializer["+session.getId()+"]";
    }
}
