package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.websocket.ProtocolException;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.utils.Binding;
import io.micronaut.websocket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

@Slf4j
public class BindTask extends AbstractQueryTask {
    private static final Term[] END_BINDING = new Term[]{new Term("<END>")};

    private final @NonNull String sparql;
    private @MonotonicNonNull Binding bindingTemplate;
    private final long batchSize;
    private long requested;
    private final @NonNull BlockingQueue<@Nullable Term @NonNull[]> bindingsQueue;
    private @Nullable Op template;


    public BindTask(@NonNull SparqlSessionContext context, @NonNull WebSocketSession session,
                    @NonNull TaskTerminationListener onTermination,
                    @NonNull String sparql) {
        super(context, session, onTermination);
        this.sparql = sparql;
        this.batchSize = Math.max(1, context.bindRequest() / 2);
        this.bindingsQueue = new ArrayBlockingQueue<>((int)(2*batchSize+Math.max(16, batchSize)));
    }

    /** Delivers a binding values row received from the client. */
    public void receiveBinding(@Nullable Term @NonNull[] row) throws ProtocolException {
        log.debug("{}.receiveBinding()", this);
        if (bindingTemplate == null)
            throw new ProtocolException("row received before var names");
        try {
            bindingsQueue.add(row);
        } catch (IllegalStateException e) {
            handleError(e);
            return;
        }
        --requested;
        if (requested == batchSize) {
            requested += batchSize;
            sendAsync("!bind-request +"+batchSize+"\n");
        }
    }

    /** Delivers the list of vars to be bound in future calls to {@code receiveBinding()}. */
    public void receiveVarNames(@NonNull List<@NonNull String> names) throws ProtocolException {
        log.debug("{}.receiveVarNames({})", this, names);
        if (bindingTemplate != null)
            throw new ProtocolException("var names already set");
        bindingTemplate = new Binding(names);

        context.executor().scheduler().schedule(() -> {
            List<@NonNull String> allVars = requireNonNull(template).outputVars();
            List<@NonNull String> freeVars = new ArrayList<>(allVars.size());
            for (String name : allVars) {
                if (!bindingTemplate.contains(name))
                    freeVars.add(name);
            }
            SyncSender sender = new SyncSender();
            sendHeaders(sender, freeVars);
            consumeBindings(sender);
        });
    }

    /** Notifies an {@code !end} received from the peer. */
    public void receiveBindingsEnd() throws ProtocolException {
        log.debug("{}.receiveBindingsEnd()", this);
        if (bindingTemplate == null)
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
    private @Nullable Term @NonNull[] takeBinding() {
        while (true) {
            try { return bindingsQueue.take(); } catch (InterruptedException ignored) { }
        }
    }

    /** Consumes bindings and serializes solutions to the client, including a final {@code !end}. */
    private void consumeBindings(@NonNull SyncSender sender) {
        assert bindingTemplate != null && template != null && info != null;

        for (var terms = takeBinding(); terms != END_BINDING; terms = takeBinding()) {
            long start = nanoTime();
            Op bound = template.bind(bindingTemplate.setTerms(terms));
            info.addDispatchNs(nanoTime()-start);

            start = nanoTime();
            Op optimized = context.bindOptimizer().optimize(bound);
            info.addOptimizeNs(nanoTime()-start);

            start = nanoTime();
            QuerySolutions solutions = context.executor().dispatcher().execute(optimized);
            info.addDispatchNs(nanoTime()-start);

            if (!serialize(sender, solutions))
                return; // !error message already sent
        }
        if (sender.send("!end\n"))
            terminate(null);
    }
}
