package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public abstract class MessageParser {
    private static final Pattern WS_RX = Pattern.compile("\s+");
    private enum State {
        ROOT,
        VARS,
        BINDINGS
    }

    private @NonNull State state = State.ROOT;
    private int columns = -1;

    /** Called once for every {@code !action}  (e.g., !query) sent by the client. */
    protected abstract void onAction(@NonNull Action action);
    /** Called once for the headers of a bindings sequence.*/
    protected abstract void onVars(@NonNull List<String> vars) throws ProtocolException;
    /** Called once for each binding values row. */
    protected abstract void onRow(@Nullable Term @NonNull[] row) throws ProtocolException;
    /** Called exactly once when an {@code !end} line is met */
    protected abstract void onEndRows() throws ProtocolException;
    /** Called when the message contains an syntax or protocol violation  */
    protected abstract void onError(String reason);

    /** Parse the client-send message calling the adequate {@code on}* methods. */
    public void parse(@NonNull String msg) {
        try {
            if (state == State.BINDINGS || state == State.VARS) {
                bindingsParse(msg);
            } else if (msg.charAt(0) == '!') {
                assert state == State.ROOT;
                rootParse(msg);
            } else {
                String reason = "Message should start with ! followed by an action. " +
                                "Whitespace-stripped message was " +
                                WS_RX.matcher(msg).replaceAll(" ");
                throw new ProtocolException(reason);
            }
        } catch (ProtocolException e) {
            log.info("WebSocket client sent illegal message: {}. msg={}", e.getMessage(), msg.replace("\n", "\\n"));
            onError(e.getMessage().replaceAll("\n", " "));
        }
    }

    /**
     * Read {@code \t}-separated var names until a line feed (@code \n).
     * @return the index of the first char after the line feed ({@code \n}).
     */
    static @NonNull List<@NonNull String> parseVars(@NonNull String msg,
                                                    int begin) throws ProtocolException {
        int eol = msg.indexOf('\n', begin);
        if (eol < 0)
            throw new ProtocolException("Missing line feed (\\n) on headers line");
        List<@NonNull String> out = new ArrayList<>();
        for (int i = begin, end; i < eol; i = end+1) {
            end = Math.min(msg.indexOf('\t', i), eol);
            if (end < 0)
                end = eol;
            if (msg.charAt(i) == '?') {
                out.add(msg.substring(i + 1, end));
            } else {
                String bad = msg.substring(i, end);
                throw new ProtocolException("var name '"+bad+"' must start with ?");
            }
        }
        return out;
    }

    /**
     * Read {@code \t}-separated RDF terms in N-Triples syntax in a line ended by {@code \n}.
     * @return A {@link Term} array with length {@code columns}.
     * @throws ProtocolException On invalid term or if their number is not {@code columns}.
     */
    static @Nullable Term @NonNull []
    parseTerms(@NonNull String msg, int begin, int columns) throws ProtocolException {
        int eol = msg.indexOf('\n', begin), out = 0;
        if (eol < 0)
            throw new ProtocolException("Missing line feed (\\n) ending values line");
        if (columns == 0) {
            if (eol == begin) {
                return Row.EMPTY;
            } else {
                String line = "\"" + msg.substring(begin, eol) + "\n\"";
                throw new ProtocolException("Expected empty line for 0 columns, got "+ line);
            }
        }
        Term[] terms = new Term[columns];
        for (int i = begin, end; i <= eol; i = end+1) {
            end = Math.min(eol, msg.indexOf('\t', i));
            if (end < 0)
                end = eol;
            String sparql = msg.substring(i, end).trim();
            Term term = sparql.isEmpty() || sparql.equals("null") ? null : new Term(sparql);
            if (term != null) {
                try {
                    term.validate();
                } catch (IllegalArgumentException e) {
                    throw new ProtocolException("RDF term " + term.sparql() + " is invalid");
                }
            }
            if (out < columns)
                terms[out] = term;
            out++;
        }
        if (out != columns) {
            throw new ProtocolException("Expected "+columns+" terms in line, found "+out);
        }
        return terms;
    }

    private static final Pattern VERB_RX = Pattern.compile("^!(\\S+)\\s*");
    private static final String MISSING_QUERY_SEP_MSG = "Missing '\\n\"\"\"\\'\\'\\'!<#>\\n' " +
                                                        "separator in !bind message";

    private void rootParse(@NonNull String msg)
            throws ProtocolException {
        Matcher m = VERB_RX.matcher(msg);
        String verb = m.find() ?  m.group(1) : "";
        switch (verb) {
            default          -> raiseUnexpectedVerb(msg);
            case "queue-cap" -> onAction(Action.QUEUE_CAP);
            case "query"     -> onAction(new Action.Query(msg.substring(7)));
            case "cancel"    -> onAction(Action.CANCEL);
            case "bind" -> {
                onAction(new Action.Bind(msg.substring(6)));
                state = State.VARS;
            }
        }
    }

    private void raiseUnexpectedVerb(@NonNull String msg) throws ProtocolException {
        Matcher m = VERB_RX.matcher(msg);
        var reason = "Unexpected command verb: '"+(m.find() ? m.group(1) : "")+"'";
        throw new ProtocolException(reason);
    }


    private static final char[] END_ACTION = "!end".toCharArray();
    private void bindingsParse(@NonNull String msg) throws ProtocolException {
        for (int i = 0, end, len = msg.length(); i < len; i = end+1) {
            if (msg.charAt(i) == '!') {
                boolean isEnd = true;
                for (int j = 1; isEnd && j < END_ACTION.length; j++)
                    isEnd = msg.charAt(i+j) == END_ACTION[j];
                if (isEnd) {
                    state = State.ROOT;
                    columns = -1;
                    onEndRows();
                    break;
                } else {
                    raiseUnexpectedVerb(msg.substring(i));
                }
            }
            end = msg.indexOf('\n', i);
            if (end < 0) {
                String reason = "Values line not terminated with '\n': "+ msg.substring(i);
                throw new ProtocolException(reason);
            }
            if (state == State.VARS) {
                List<String> vars = parseVars(msg, i);
                columns = vars.size();
                state = State.BINDINGS;
                onVars(vars);
            } else {
                onRow(parseTerms(msg, i, columns));
            }
        }
    }
}
