package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.ProtocolException;
import com.github.lapesd.hdtss.controller.websocket.SparqlSession;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.utils.Binding;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.nanoTime;

@Slf4j
public class BindTask extends AbstractQueryTask {
    private static final Term[] END_BINDING = new Term[]{new Term("<END>")};

    private final @NonNull String sparql;
    private @MonotonicNonNull Binding binding;
    private final long batchSize;
    private long requested;
    private final @NonNull ReentrantLock lock;
    private final @NonNull Condition hasBindings;
    private final @NonNull ArrayDeque<@Nullable Term @NonNull[]> bindingsQueue;
    private @MonotonicNonNull Op template;
    private volatile boolean hasWorker = false;


    public BindTask(@NonNull SparqlSession session, @NonNull TaskTerminationListener onTermination,
                    @NonNull String sparql) {
        super(session, onTermination);
        this.sparql = sparql;
        this.batchSize = Math.max(1, session.context().bindRequest() / 2);
        this.bindingsQueue = new ArrayDeque<>((int)(batchSize*2)+2);
        this.lock = new ReentrantLock(false);
        this.hasBindings = this.lock.newCondition();
    }

    private void raiseProtocolException(String message) throws ProtocolException {
        ProtocolException ex = new ProtocolException(message);
        serializer.end("!error "+message.replace("\n", "\\n"), ex);
        throw ex;
    }

    /** Delivers a binding values row received from the client. */
    public void receiveBinding(@Nullable Term @NonNull[] row) throws ProtocolException {
        log.debug("{}.receiveBinding()", this);
        if (binding == null)
            raiseProtocolException("row received before var names");
        lock.lock();
        try {
            bindingsQueue.add(row);
            hasBindings.signalAll();
        } catch (IllegalStateException e) {
            raiseProtocolException("Client sent more bindings than allowed");
        } finally {
            --requested;
            tryIncrementalRequest();
            lock.unlock();
        }
    }

    /** Delivers the list of vars to be bound in future calls to {@code receiveBinding()}. */
    public void receiveVarNames(@NonNull List<@NonNull String> names) throws ProtocolException {
        log.debug("{}.receiveVarNames({})", this, names);
        if (binding != null)
            raiseProtocolException("client already sent var names row");
        assert template != null;
        binding = new Binding(names);
        hasWorker = true;
        session.context().executor().scheduler().schedule(this::work);
    }

    /** Notifies an {@code !end} received from the peer. */
    public void receiveBindingsEnd() throws ProtocolException {
        log.debug("{}.receiveBindingsEnd()", this);
        if (binding == null)
            raiseProtocolException("Received !end before var names");
        queueEndBinding();
    }

    /** Enqueue END_BINDING */
    private void queueEndBinding() {
        lock.lock();
        try {
            if (hasWorker) {
                bindingsQueue.add(END_BINDING);
                hasBindings.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override protected QueryInfo.@Nullable Builder doStart() {
        QueryInfo.Builder info = QueryInfo.builder(sparql);
        long start = nanoTime();
        template = session.context().executor().parser().parse(sparql);
        info.addParseNs(nanoTime()-start);

        requested = batchSize*2;
        serializer.send("!bind-request " + requested+ "\n");
        return info;
    }

    @Override protected void cleanup(TaskTerminationListener.Cause cause) {
        log.trace("{}.cleanup({})", this, cause);
        queueEndBinding();
    }

    /** Uninterruptible {@code bindingsQueue.take()} followed by {@code tryIncrementalRequest()}. */
    private @Nullable Term @NonNull[] takeBinding() {
        Term[] binding;
        boolean interrupted = false;
        lock.lock();
        try {
            while (bindingsQueue.isEmpty()) {
                try { hasBindings.await(); }
                catch (InterruptedException e) { interrupted = true; }
            }
            binding = bindingsQueue.remove();
            tryIncrementalRequest();
            if (interrupted)
                Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
        return binding;
    }

    /** Send a {@code !bind-request +n} if appropriate */
    private void tryIncrementalRequest() {
        assert lock.isHeldByCurrentThread();
        if (requested <= batchSize && bindingsQueue.size() <= batchSize) {
            requested += batchSize;
            serializer.send("!bind-request +"+batchSize+"\n");
        }
    }

    /** Entry point for the worker thread that consumes bindings and send solutions over the WS. */
    private void work() {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("BindTask-worker-"+session.id());
        try {
            consumeBindings();
        } finally {
            hasWorker = false;
            Thread.currentThread().setName(originalName);
        }
    }

    /** Consumes bindings and serializes solutions to the client, including a final {@code !end}. */
    private void consumeBindings() {
        assert binding != null && template != null && info != null;
        boolean optimized = false;
        SparqlExecutor executor = session.context().executor();
        var dispatcher = executor.dispatcher();
        boolean ok = sendHeaders(binding.unbound(template.outputVars()));
        for (var terms = takeBinding(); ok && terms != END_BINDING; terms = takeBinding()) {
            if (!optimized) {
                optimized = true;
                long start = nanoTime();
                binding.setTerms(terms);
                template = executor.optimizer().optimize(template, binding);
                info.addOptimizeNs(nanoTime()-start);
            }
            long start = nanoTime();
            var solutions = dispatcher.execute(template, binding.setTerms(terms));
            info.addDispatchNs(nanoTime()-start);
            assert buf.length() == 0 : "Concurrent use of buf";
            buf.append("!active-binding ");
            serializeRow(buf, terms);
            ok = serializer.send(buf);
            buf.setLength(0);
            ok = ok && serialize(solutions);
        }
        hasWorker = false;
        sendEnd();
    }
}
