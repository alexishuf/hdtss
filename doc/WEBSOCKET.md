> **Warning**: websocket support is not standardized this is a vendor-specific
> extension

SPARQL queries can be sent over websocket connections. In the typical scenario,
this only adds websocket upgrade overhead compared to
`sparql.endpoint.flow=CHUNKED`. However, if the client intends to repeatedly
submit the same query only changing which values are bound to a subset of
variables, a websocket will allow it to send only the new values instead of
the whole query rewritten with the new values. Sending only the new values
avoids the cost of sending, parsing and optimizing the query (assuming the
client is already reusing the HTTP connection).

Once the websocket is established, there are two actors: `CLIENT`
(the HTTP client) and `SERVER` (hdtss). Both actors exchange messages
which are carried over websocket text
[data frames](https://datatracker.ietf.org/doc/html/rfc6455#section-5.6).

Over the websocket, communication is initiated by the client, which can send
as many messages as it wishes without the need to wait the server to start
or complete answering previously sent messages. All `SERVER`-sent messages
are sent in response to a previous `CLIENT`-sent message.

#### Client-sent messages

To describe message sequences as well as their contents, the following EBNF
segments follow usual RFC syntax with two extensions:

- `UPPERCASE` symbols represent non-terminals defined elsewhere
    - `SPARQL`: A valid SPARQL query
    - `VARNAME`: A SPARQL var name (not including the leading '?' or '$')
    - `NT_VALUE`: A RDF term value in N-Triples syntax
- `$` denotes the end of a websocket message (a message may still be
  fragmented across multiple websocket data frames).

Valid message sequences sent by the `CLIENT` are described by the following
grammar:

```text
client_session = action*
action         = '!queue-cap' $
               | '!query ' SPARQL $ 
               | '!bind ' SPARQL $ bindings 
               | '!cancel ' $
bindings       = headers ( values* $ )* values* '!end\n' $ 
headers        = '?' VARNAME ('\t?' VARNAME)* '\n'
values         = NT_VALUE (\t NT_VALUE)* '\n'
```

> Note that `bindings` corresponds to TSV encoding of SPARQL query results
> followed by `!end`.

> Also note that bindings can be fragmented across multiple websocket messages,
> but a row of values cannot be fragmented and a `bindings` sequence must
> always include at least one values row sent in the first websocket message.

The terminals at the start of an `action` have the following semantics:

- `!queue-cap`: How many `!query` messages the client can send before
  the server sends the `!end`/`!error`/`!cancelled` finalizing the results of
  the first query. The minimum length is 2, thus a client only needs to send
  this command if it intends to send 3 or more `!query` messages without
  waiting the first one to complete.
- `!query`: execute the following SPARQL query
- `!bind`: execute the following SPARQL query once for each row of bindings.
  In more detail, for each `values` the `CLIENT` sends, the `SERVER` will
  send the results of evaluating `SPARQL` replacing the i-th `VARNAME` with
  the i-th `NT_VALUE`. **Important: the client may send a headers line in a 
  subsequent message, but before sending value lines, the client 
  must wait for a `!bind-request` message.** See the 
  [flow control section below](#websocket-flow-control).
- `!cancel`: stop processing (or do not start) processing and serialization
  of the last `!query` or `!bind` sent by the client. The `CLIENT` MUST be 
  ready to accept any number of solutions to the cancelled  query after it 
  has sent a `!cancel` message.

> Note that `!cancel` cancels the last request sent, not the last request
> processed by the server nor the request being currently processed by
> the server.


#### Server-sent response messages

The `SERVER`-produced message streams corresponds to the following grammar:

```text
server           = response*
response         = queue-cap-resp
                 | query-resp
                 | bind-resp
queue-cap-resp   = '!action-queue-cap=' [0-9]+ $
query-resp       = results | error
bind-resp        = bind-request bind-results
error            = ( '!error ' .* '\n' | '!cancelled\n' ) $
bind-request     = '!bind-request ' [0-9]+
bind-request-inc = '!bind-request +' [0-9]+
active-binding   = '!active-binding ' values  
results          = headers ( values* $ )* values* ('!end\n' | error) $ 
bind-results     = headers ( bound-values* $ | bind-request-inc $ )* bound-values* ('!end\n' | error) $ 
headers          = '?' VARNAME ('\t?' VARNAME)* '\n'
bound-values     = active-binding values* 
values           = NT_VALUE (\t NT_VALUE)* '\n'
```

> `results` corresponds to `bindings` but can also end in `!cancelled` or `!error` 

> `bind-request` and `bind-request-inc` only occur when the server is 
> responding to a `!bind` request. See 
> [bind flow control section](#bind-flow-control) below for their semantics. 

> The `!error` and `!cancelled` messages can occur at any point during the 
> response to a `!query` or `!bind` but they will never fragment a line 
> (i.e., `headers` or `values`) in the results.  

The server will send a `results` per processed `!query` or `!bind` request.
The `results` will always be syntactically valid (including the correspondence
between `NT_VALUE` and `VARNAME`s), even if the request in cancelled or
an error occurs while processing the query (e.g., IO/failure or invalid query).

In case of `!cancel` the server will end the `results` with `!cancelled`. If
the cancel is processed before execution of the query starts, the server must
nevertheless send the `headers` and a following `!cancelled`.

In case of errors, including errors that occur while serializing a individual
`NT_VALUE`, the server must still send a complete `values` sequence (matching
the length of the previous `headers` sequence) before sending the closing
`!error` followed by an optional description.

#### Websocket Flow Control

Websockets are full-duplex channels, where `CLIENT` and `SERVER` could send
messages without waiting the peer to acknowledge the previous messages have
been processed. However, this creates an easy to exploit Denial-of-Service
vulnerability for the server. Thus, the `!query` and `!bind` actions are
subject to flow control under two different mechanisms:

##### `!query` flow control

Any server must maintain a queue of received but not yet executed `!query`
actions. The minimum length of that queue is of 2 `!query` messages per
websocket session. Servers can support larger queues, the capacity of which
can be obtained by the client via the `!queue-cap` message. The capacity
reported by `!queue-cap` is unique to the websocket session and immutable
in that session.

##### `!bind` flow control

By design, `!bind` sequences include the client hammering the server with
`values` lines. In this case, the client must always wait the server to
indicate how many `values` lines it can receive from the client. Thus, the 
client must not send any message other than the `headers` line or an `!end` 
before the server sends a `!bind-request N` where N is the number of 
`values` lines the server is ready to receive.

The `!bind-request N` message is sent only once by the server after it processes
a `!bind` message. At any point after that the server may send
`!bind-request +N2` messages which are incremental. That is, if the client had
sent _k_ lines after a `!bind-request N1` message and the client receives a
`!bind-request N2` message, then it is now allowed to send _N1-k+N2_ messages.

Since there is no synchronization between the client sending `values` lines 
and the server sending sending `values` lines, the server will send one  
`!tive-binding` line before sending the `values` that spawned from that binding. 
This allows the client to correlate each `values` line it receives to a 
binding it previously sent as a `values` line earlier.