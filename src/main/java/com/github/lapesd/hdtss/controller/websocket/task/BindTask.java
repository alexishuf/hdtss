package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.websocket.ProtocolException;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.utils.Binding;
import io.micronaut.websocket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static java.lang.System.nanoTime;

@Slf4j
public class BindTask extends AbstractQueryTask {
    private static final Term[] END_BINDING = new Term[]{new Term("<END>")};

    private final @NonNull String sparql;
    private @MonotonicNonNull Binding binding;
    private final long batchSize;
    private long requested;
    private final @NonNull Queue<@Nullable Term @NonNull[]> bindingsQueue;
    private @MonotonicNonNull Op template;


    public BindTask(@NonNull SparqlSessionContext context, @NonNull WebSocketSession session,
                    @NonNull TaskTerminationListener onTermination,
                    @NonNull String sparql) {
        super(context, session, onTermination);
        this.sparql = sparql;
        this.batchSize = Math.max(1, context.bindRequest() / 2);
        this.bindingsQueue = new ArrayDeque<>((int)(batchSize*2)+16);
    }

    /** Delivers a binding values row received from the client. */
    public synchronized void receiveBinding(@Nullable Term @NonNull[] row) throws ProtocolException {
        log.debug("{}.receiveBinding()", this);
        if (binding == null)
            throw new ProtocolException("row received before var names");
        try {
            bindingsQueue.add(row);
            notifyAll();
        } catch (IllegalStateException e) {
            handleError(e);
            return;
        }
        --requested;
        tryIncrementalRequest();
    }

    /** Delivers the list of vars to be bound in future calls to {@code receiveBinding()}. */
    public void receiveVarNames(@NonNull List<@NonNull String> names) throws ProtocolException {
        log.debug("{}.receiveVarNames({})", this, names);
        if (binding != null)
            throw new ProtocolException("var names already set");
        assert template != null;
        binding = new Binding(names);

        context.executor().scheduler().schedule(() -> {
            SyncSender sender = new SyncSender();
            sendHeaders(sender, binding.unbound(template.outputVars()));
            consumeBindings(sender);
        });
    }

    /** Notifies an {@code !end} received from the peer. */
    public synchronized void receiveBindingsEnd() throws ProtocolException {
        log.debug("{}.receiveBindingsEnd()", this);
        if (binding == null)
            throw new ProtocolException("Received !end before var names");
        bindingsQueue.add(END_BINDING);
    }

    @Override protected QueryInfo.@Nullable Builder doStart() {
        QueryInfo.Builder info = QueryInfo.builder(sparql);
        long start = nanoTime();
        template = context.executor().parser().parse(sparql);
        info.addParseNs(nanoTime()-start);

        requested = batchSize*2;
        String requestMsg = "!bind-request " + requested+ "\n";
        context.executor().scheduler().schedule(() -> sendAsync(requestMsg));
        return info;
    }

    /** Uninterruptible {@code bindingsQueue.take()}. */
    private synchronized @Nullable Term @NonNull[] takeBinding() {
        boolean interrupted = false;
        while (bindingsQueue.isEmpty()) {
            try { wait(); } catch (InterruptedException e) { interrupted = true; }
        }
        @Nullable Term[] binding = bindingsQueue.remove();
        tryIncrementalRequest();
        if (interrupted)
            Thread.currentThread().interrupt();
        return binding;
    }

    /** Send a {@code !bind-request +n} if appropriate */
    private synchronized void tryIncrementalRequest() {
        if (requested <= batchSize && bindingsQueue.size() <= batchSize) {
            requested += batchSize;
            sendAsync("!bind-request +"+batchSize+"\n");
        }
    }

    /** Consumes bindings and serializes solutions to the client, including a final {@code !end}. */
    private void consumeBindings(@NonNull SyncSender sender) {
        assert binding != null && template != null && info != null;
        boolean optimized = false;
        var dispatcher = context.executor().dispatcher();
        for (var terms = takeBinding(); terms != END_BINDING; terms = takeBinding()) {
            if (!optimized) {
                optimized = true;
                long start = nanoTime();
                binding.setTerms(terms);
                template = context.executor().optimizer().optimize(template, binding);
                info.addOptimizeNs(nanoTime()-start);
            }
            long start = nanoTime();
            var solutions = dispatcher.execute(template, binding.setTerms(terms));
            info.addDispatchNs(nanoTime()-start);
            assert buf.length() == 0 : "Concurrent use of buf";
            buf.append("!active-binding ");
            unaccountedSerializeRow(buf, terms);
            sender.send(buf);
            buf.setLength(0);
            if (!serialize(sender, solutions))
                return; // !error message already sent
        }
        if (sender.send("!end\n"))
            terminate(null);
    }
}
