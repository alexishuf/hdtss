Configuration properties
========================

Table of Contents:

- [Setting properties](#setting-properties)
- [Micronaut/netty Configuration](#micronautnetty-configuration)
- [Logging](#logging)
- [HDT configuration properties](#hdt-configuration-properties)
  - [Loading](#loading)
    - [Progress Listener](#progress-listener)
  - [Cardinality Estimation](#cardinality-estimation) 
- [Querying configuration properties](#querying-configuration-properties)
  - [Flow Control](#flow-control) 
  - [Implementation Strategies](#implementation-strategies)
  - [Optimization](#optimization)
- [SPARQL protocol configuration properties](#sparql-protocol-configuration-properties)
  - [Websocket support](#websocket-support) 

Setting Properties
------------------

Configuration is handled via [Micronaut](https://micronaut.io/). Specifically, 
check the default sources and priority at 
the [Included PropertySource Loaders sub-subsection](https://docs.micronaut.io/latest/guide/#propertySource).

Configuration properties are described here in kebab-case with dot as a 
hierarchy separator. This is the style used in java properties (.properties 
file and the -D java flag). application.json and application.yaml files 
are also supported as sources and in those cases the `a.b` used here 
means `b` property of the object that is the value of `a` in the root object. 
For environment variables, names are upper case and both `.` and `-` become `_`.  

Popular configuration sources, from highest to lowest priority:

1. Command-line flags
2. File at the path in the `SPRING_APPLICATION_JSON` environment variable
3. File at the path in the `MICRONAUT_APPLICATION_JSON` environment variable
4. JVM properties (set with -D or .properties files in the classpath)
5. Environment variables
6. Files at the `,`-separated paths listed in `micronaut.config.files` 
   java property or `MICRONAUT_CONFIG_FILES` env var. The last files in such 
   lists has the highest precedence. 
7. `application-{environment}.{extension}` files
   - see the micronaut docs for details about [environments](https://docs.micronaut.io/latest/guide/#environments)
   - .properties, .json and .yaml are supported (among them, precedence is undefined)
8. `application.{extension}`

For items `7.` and `8.` above, files are searched in the classpath root and 
on the `config` directory, relative to the current working dir. values from 
the `config/` dir override values from the classpath.

The following sections serve as a reference for the several configuration 
properties.

> An example yaml configuration file listing all default values can be 
> found in the [docs directory](./application-example.yaml)
> 


Micronaut/netty configuration
-----------------------------

All properties under `micronaut.server` can be set from the command line 
omitting the `micronaut.server` prefix. Examples:

- `-port=8090` sets `micronaut.server.port` to 8090
- `--netty.max-initial-line-length 8192` reduces the maximum length of GET 
  requests to 8 KiB (the hdtss default is 32 KiB while the netty default is 
  4 KiB). 

> Unlike GNU style options, `-` is handled the same as `--`


Logging
-------

Changing the log level for a package is handled by micronaut property 
`logger.levels.PACKAGE`. For the `com.github.lapesd.hdtss` package, which 
hosts hdtss code, there is a shortcut command-line option with the following 
levels:

- `-v ERROR` something went wrong, the server may continue working nevertheless
- `-v WARN` something looks wrong, usually hdtss makes a possibly wrong guess 
            to avoid crashing or failing completely.
- `-v INFO` a few initialization milestones, periodic liveness status, and 
            non-trivial situations that are not worthy of a `WARN`message
- `-v DEBUG` coarse-grained messages per query
- `-v TRACE` fine-grained messages for multiple steps in query processing.
- 

This `-v` (or also `--v`, `-verbosity` and `--verbosity`) expands to 
`-logger.levels.com.github.lapesd.hdtss`. To set the level for other packages, 
use the longer form.

By default, a query only causes a log message if its execution fails. The 
following property forces logging even for successful queries:

> sparql.log.query=true|false
> 
> Whether to log a `INFO`-level message upon successful or client-cancelled 
> execution of a query. The log message will include the SPARQL string. The 
> **default** is `true`. If this is set to false, failed queries will still 
> yield `ERROR`-level messages, while sucessful and user-cancelled queries 
> will yield `DEBUG`-level messages.

HDT configuration properties
----------------------------

These properties select implementations responsible for loading and 
querying triple patterns against HDT files.

### Loading

The only mandatory configuration is the location of the HDT file to be loaded:

> `hdt.location=string`
>  
>  The string should be a file path or URI to an HDT file.

Usage (and generation) of the `.index.v1-1` sidecar file is controlled 
by the following property:

> `hdt.load.indexed=boolean`
> 
> **True by default**, causes the querying to use additional index 
> permutations in the `.index.v1-1` sidecar file, generating such file if 
> it does not exist and the `HDTLoader` can generate it. Note that 
> generating the sidecar file can be slow, but querying without it is also 
> slow. If the generation time is an issue, run the `hdtSearch` binary 
> from [hdt-cpp](https://github.com/rdfhdt/hdt-cpp) manually to create 
> the index file ahead of time on a powerful enough machine.
> 
> If false, the index file will not be used, even if it already exists.

HDT files usually contain a single SPO index. Other permutations are kept in a 
`.index.v1-1` sidecar file, which is usually generated the first time the HDT 
file is queried. To use such indexing functionality, the following property must be true

The actual loading of an HDT file will be done by one of multiple `HDTLoader`s.
The selection of an `HDTLoader` implementation takes into account the type 
of string (e.g. URI scheme or whether is it is a local path)  and the 
attributes requested via the configuration properties below.

> `hdt.load.native=boolean`
> 
> Whether HDT files should be loaded using the [C++ HDT](https://github.com/rdfhdt/hdt-cpp) 
> implementation instead of the [java one](https://github.com/rdfhdt/hdt-java) 
> **Default is `true`**, but since `hdt.load.require-all` is false by default, 
> a java implementation will be selected if a native is not available.

> `hdt.load.mmap=boolean`
> 
> Whether HDT files should be memory mapped instead of copying all data 
> into RAM. The **default is `true`**, since memory-mapped files are faster 
> to "load", and after warm-up on a host with sufficient free physical memory 
> should provide equivalent performance to explicitly loading into RAM.

> `hdt.load.can-create-index=boolean`
> 
> Whether the `HDTLoader` has the ability to generate `.index.v1-1` sidecar 
> files when they are missing and their use is requested in `HDTLoader.load()`
> (e.g., `hdt.load.index` is `true`).
> 
> The default value is `true`. If false, HDTLoaders without this ability will 
> be accepted, but will still be weighed at a lower score than the 
> implementations that can generate the index file. 
 
While unlikely, it is possible that no loader satisfies all requested 
attributes and is able to load files of the type given in `hdt.location`. 
In such scenario the loader with higher weight will be selected with each 
of the above loader properties having the following weights:


| Property                    | Weight  |
| --------------------------- |:-------:|
| `hdt.load.mmap`             | 4       |
| `hdt.load.native`           | 2       |
| `hdt.load.can-create-index` | 1       |

> `hdt.load.require-all=boolean`
> 
> If true, disables the aforementioned graceful selection of the least 
> disappointing `HDTLoader`, causing the loading to outright fail. 
> **Default is `false`**, i.e., constraints on `HDTLoader` characteristics 
> will be relaxed if unsatisfiable. 

#### Progress listener

As HDT files are loaded (or memory-mapped), the progress will be displayed 
via a `org.rdfhdt.hdt.listener.ProgressListener` implementation. 
These properties choose the implementation and configure it.    

> `hdt.load.progress.impl=string`
> 
> The string should be one of the following implementation `names`:
> > `log` (default) -- Write messages to the SLF4J logger
> 
> > `none` -- Do not report progress

##### `log` progress listener configuration

> `hdt.load.progress.log.level=string`
> 
> The SLF4J Level under which progress messages shall be logged. 
> Possible values are:
> 
> > `ERROR`, `WARN`, `INFO` (**default**), `DEBUG` and `TRACE`

> `hdt.load.progress.log.period=string`
> 
> The minimum interval between each logged progress message. 
> Values should consist of a positive integer followed by an `ms`, `s` or 
> `m` suffix indicating the duration time unit. Examples:
> 
> > `5s` (the **default**), `1500ms` or `1m`

> `hdt.load.progress.log.on-start=boolean`
> 
> If `true` (the **default**), the first progress event will generate a 
> log message even if period has not yet elapsed. If `false`, the first 
> progress message will only be logged after the set period is elapsed 
> (countdown starts on `ProgressListener` creation, close to load start).

> `hdt.load.progress.log.on-end=boolean`
> 
> If `true` (the **default**), will log a 100% progress message once 
> progress reaches 100%, even if the minimum period constraint is not 
> satisfied. If `false`, the last progress event will only trigger a progress
> message to be logged if the  minimum period constraint is satisfied. 

### Cardinality estimation

Estimating how many results a triple pattern will yield is a core service 
required to implement query optimization. While HDT files do not have advanced
statistics a triple store would collect, some estimation is possible.
The following property controls how cardinality estimation will be done:

> `hdt.estimator=string`
> Select the `CardinalityEstimator` implementation.
> 
> > `NONE`
> > All triple patterns will be estimated as having 1 result. 
>
> > `PATTERN`
> > Cardinality will be estimated using a hard-coded heuristic taking into 
> > account the number of variables in the pattern and their positions:
> > - ` s  p  o`: 1
> > - `?s  p  o`: 1000
> > - ` s ?p  o`: 10
> > - ` s  p ?o`: 20
> > - `?s ?p  o`: 2000
> > - `?s  p ?o`: 10000
> > - ` s ?p ?o`: 100
> > - `?s ?p ?o`: 100000
>
> > `PEEK`
> > For every triple pattern will use HDTs own cardinality estimation. 
> > This will incur the cost of looking up grounded terms in the HDT 
> > dictionary, selecting the best index and building an iterator over that 
> > index. Queries with the form `?s <p> ?o` or `?s rdf:type <class>` will 
> > have their estimations cached, thus reducing the overall estimation cost. 
> > When the HDT estimation is not available, an heuristic based on number 
> > of subject, predicates, objects and triples will be used. If the HDT 
> > estimation is overestimating, it will be averaged with the number of terms
> > heuristic.
> > 
> > > To disable all lookup-based estimations, set `hdt.estimator.lookup=NEVER`
> >
> > > To restrict lookup-based estimation to cacheable query patterns, 
> > > set `hdt.estimator.lookup=CACHED`. The default is `ALWAYS`
> > 
> > > By default, predicates and classes will be enumerated on background to 
> > > fill caches. During this process, the estimator is responsive, although 
> > > there might be some background load affecting I/O. To disable this, set
> > > `hdt.estimator.prefetch=false` (the default is `true`). Without a 
> > > complete prefetch, the estimator may apply smaller bonuses to triple 
> > > patterns mentioning infrequent predicates or classes (since the most 
> > > frequent class/predicates may not be known yet). 

 
Querying configuration properties
---------------------------------

### Flow control

When processing queries, the flow control pattern can be one of three:

1. `REACTIVE`: The code immediately querying the HDT pushes results to 
   further transformations and processing steps which are eventually 
   pulled by a subscriber (e.g., code serializing solutions).
2. `ITERATOR`: All query processing is purely pull-based: the final consumer 
   (e.g., code serializing solutions) pulls one solution, triggering all 
   required processing only when it does so.
3. `BATCH`: All processing steps are eagerly computed generating lists of 
   solutions as result. This pattern favors cache coherence when generating 
   such lists but is likely to stress the garbage collector or cause 
   `OutOfMemoryError`s.

The flow control type can be set for each individual SPARQL operator, as well 
for the processing of triple pattern queries against an HDT file. 

> `sparql.flow=REACTIVE|ITERATOR|HDT_REACTIVE|HEAVY_REACTIVE`
> 
> Sets the flow control pattern for all SPARQL intermediary operators (for 
> `REACTIVE|ITERATOR|BATCH`). The `*_REACTIVE` apply reactive flow control to 
> subsets of the operators. The **default** is `ITERATOR`.
> 
> While REACTIVE flow is the selling point of hdtss having only 
> `sparql.endpoint.flow=REACTIVE` with all algebra operators being purely 
> pull-based (`ITERATOR`) gives the best performance for most scenarios since 
> the overhead of flow control is paid only once. However, reactive algebra 
> operators might provide a performance boost if there is few query 
> concurrency and queries have deep join trees or UNIONs. In that scenario, 
> the reactive operators will better exploit parallelism that is not being 
> explored by due to lack of concurrent SPARQL queries.   
> 
> > With `HDT_REACTIVE` only triple pattern processing is made reactive and 
> > offloaded to a worker thread in the `sparql.reactive.scheduler`.
> 
> > With `HEAVY_REACTIVE` only triple pattern processing and SPARQL intermediary 
> > operators with heaver "self" processing are made reactive (and offloaded):
> > `hdt`, `join`, `filter`, `assign`, `exists` and `minus`.


The flow control for individual operators can be controlled with 
`sparql.OP_NAME.flow` configuration properties. The default for these 
properties is null, delegating control to `sparql.flow`:

> `sparql.hdt.flow=REACTIVE|ITERATOR|BATCH`
> 
> `sparql.filter.flow=REACTIVE|ITERATOR`
> 
> `sparql.join.flow=REACTIVE|ITERATOR`
> 
> `sparql.union.flow=REACTIVE|ITERATOR`
> 
> `sparql.distinct.flow=REACTIVE|ITERATOR`
> 
> `sparql.weakDistinct.flow=REACTIVE|ITERATOR`
> 
> `sparql.project.flow=REACTIVE|ITERATOR`
> 
> `sparql.values.flow=REACTIVE|ITERATOR`
> 
> `sparql.slice.flow=REACTIVE|ITERATOR`
> 
> `sparql.assign.flow=REACTIVE|ITERATOR`
> 
> `sparql.exists.flow=REACTIVE|ITERATOR`
> 
> `sparql.minus.flow=REACTIVE|ITERATOR`
> 
> `sparql.ask.flow=REACTIVE|ITERATOR`

The main advantage of `REACTIVE` is its hybrid push/pull model where publishers 
can start computing new elements in the sequence before a downstream operator 
pull them. However, if a no stage of a Flux is never offloaded to a Scheduler, 
they will behave as a java Stream with a purely pull-based model. 

> `sparql.reactive.scheduler=IO|ELASTIC`
> 
> Which Project Reactor Scheduler to use when offloading Flux<> processing 
> for the SPARQL operators set to `flow=REACTIVE`. The **default** is `IO`.
> 
> > `IO` selects to the Micronaut built-in IO scheduler. See Micronaut 
> documentation for configuration properties.
> 
> > `ELASTIC` creates a new "bounded elastic" Scheduler. The maximum number 
> of threads in this executor is controlled by `sparql.reactive.max-threads` 
> and the queue size for each thread by `sparql.reactive.max-queue` 

The `sparql.reactive.scheduler=ELASTIC` scheduler can be configured with the 
following two properties:

> `sparql.reactive.max-threads=integer`
> 
> The maximum number of alive threads in the scheduler. The **default** is -1,
> which delegates to Project Reactor's default of 10 times the number of 
> processor cores (including HyperThreading cores) available on the system. 
 
> `sparql.reactive.max-queue=integer`
> 
> The maximum number of tasks scheduled for a single worker thread in the 
> executor. The **default**, -1, delegates to the Project Reactor default, 
> which is 100000, overridable by property 
> `reactor.schedulers.defaultBoundedElasticQueueSize`. 

SPARQL queries more complex than a `SELECT * ` with a single triple pattern 
require operators to combine the many triple patterns. Each operator has a 
configuration property to select the flow control type:

### Implementation strategies 

Some SPARQL operators have more than one implementation, some of which have 
configuration parameters. The following discusses how to choose and configure 
such implementations.

#### DISTINCT

> `sparql.distinct.strategy=HASH|WINDOW`
>
> Implementation of the `Distinct` operator:
>
>  > `HASH`: store all solutions in a `HashSet`
> 
>  > `WINDOW`: store only the past `sparql.distinct.window` solutions in
>  > a hash set. This does not conform to the [W3C recommendation](https://www.w3.org/TR/sparql11-query/).

> `sparql.distinct.window=integer`
>
> If `sparql.distinct.strategy` is WINDOW, this property sets the maximum window
> size. By **default** the window comprises the last `131072` solutions.

Optimizers may introduce `WEAK_DISTINCT` operators, which behave like a 
`DISTINCT` but which enforce a executor using the `WINDOW` strategy presented 
above. This allows the user-requested `DISTINCT` to be processed with a higher 
window or with a hash table, while de-duplication of intermediate results 
have minimal overhead. 

> sparql.weakDistinct.window=integer
> 
> The window size to use when executing WEAK_DISTINCT operators. The **default** 
> is `8192`.

#### MINUS

> `sparql.minus.strategy=SET|BIND`
> 
> In order to accept a solution from `L`, an implementation of `Minus(L, R)`
> needs to check that it does not share variable-value pairs with any solution 
> in `R`. This can be accomplished building a `Set<>` of the relevant 
> projection of `R` (`SET`, the **default**) or trying a bind join with `R`
> for every solution of `L`. 

> `sparql.minus.set=HASH|TREE`
> 
> When `sparql.minus.set=SET`, use this particular `Set` implementation:
> `HashSet<>` (if `HASH`) or `TreeSet<>` (if `TREE`). The **default** is `HASH`. 
 

#### Join implementations

>  sparql.join.strategy=BIND
>
> The join implementation to use. The **default** is `BIND`
> 
> > `BIND`: For every solution on the left side, rewrite the query on the 
> > right binding the join variables to the obtained solutions and execute 
> > such bound rewriting.

### Optimization

Optimization is applied after parsing by an `OptimizerRunner`. There are only 
two implementations to choose from:

> `sparql.optimizer.runner=ALL|NONE`
> 
> > `ALL` (the **default**) will execute all enabled operation optimizers

> > `NONE` will not execute any optimizer, effectively disabling optimization 
> > and executing queries as parsed

#### Distinct pushing

Under a `ASK` or `SELECT DISTINCT` root, or under the right operand of 
a `MINUS`, `EXISTS` or `NOT EXISTS` operator, deduplication of intermediate 
results will not cause incompleteness of overall results. 

This optimizer injects **weak** `DISTINCT` operators wrapping the operands of 
joins, left joins and filters.

**Weak** `DISTINCT` de-duplicates rows on a best-effort basis with limited 
memory overhead. Limiting the overhead is achieved by checking incoming rows 
against a small subset of previous rows instead of all previous rows.  

> `sparql.optimizer.distinct=true|false`
> 
> Whether to enable (`true`, the **default**) or not (`false`) the 
> distinct-pushing optimizer. The value of this property is case-insensitive.

#### Projection pushing

When the query has an outer projection clause, the projection can be pushed 
downward to intermediate results, dropping unnecessary columns.

> `sparql.optimizer.project=true|false`
> 
> Whether the optimizer will attempt to inject `PROJECT` operators to 
> intermediary results, dropping useless columns as early as possible in 
> evaluation. The **default** is `true`.

### Join-reordering

Join order optimizer consists in executing the operand with lower estimated 
cardinality before its counterparts. This strategy minimizes the number of 
intermediate results if the estimate is perfectly accurate.

> `sparql.optimizer.join=true|fase`
> 
> Whether join-order optimization is enabled. Join operands may be re-ordered 
> (with corresponding projection if needed) to reduce the number of 
> intermediate results. The **default** is `true`.
 
The join-order optimizer relies on the triple pattern cardinality estimator 
set via `hdt.estimator`, as discussed in [HDT > Cardinality Estimation](#cardinality-estimation).

When join operands are not triple patterns, the cardinality of any other 
operator is estimated by summing the cardinality of all triple patterns 
recursively contained in the tree rooted at that operator, excluding the 
right-side operands of `EXISTS` and `MINUS` operators.

> When [filter-pushing](#filter-join-pushing) is enabled, the join-order 
> optimizer will change its estimation to apply a penalty on join operands 
> that do not contribute to any upstream filter expression but the join 
> itself fully feeds an upstream `FILTER`/`EXISTS`/`MINUS` operator.  

> `sparql.optimizer.filter-join-penalty=integer` 
> 
> The percentage of the operand's original estimated cardinality to apply as 
> penalty when that operand does not contribute variables to the nearest 
> upstream filter which the join operator can fully feed. If the penalty is `x`,
> `cost += max(1, cost*(x/100))`. The **default** is `5`, meaning 5%.
> 
> > Negative values are not allowed. Zero disables this functionality.

#### Filter-Join pushing

When a `FILTER`/`MINUS`/`EXISTS` is applied to a `JOIN` or `LEFT_JOIN` but 
all filter variables can be filled with only a subset of the join operands the 
following modifications yield the same results with fewer intermediate results: 

- Replace the enclosing filter with its input child.
- Replace the aforementioned subset of join operands with the enclosing 
  filter operator, now having a join among the operand subset as its input
  - If the subset is a unit set, there is no need for wrapping into a `JOIN` 

This optimization can be toggled with the following property:

> `sparql.optimizer.filter=true|false`
> 
> Whether to attempt to push `FILTER`/`EXISTS`/`MINUS` operators into 
> subsets of `JOIN`/`LEFT_JOIN` operands. The **default** is `false`.

This optimization interacts with the join-order optimizer, which executes 
previously. The join-order optimizer will apply penalties to operands that do 
not contribute bindings to the nearest enclosing filter. Such penalties 
contribute to clustering the eligible subset into the beginning of the 
join order. 

> The current implementation will only replace contiguous subsets of join 
> operands. This is a cheap condition that avoids introduction of cartesian 
> products and does not override the (estimated) optimal join order.

SPARQL protocol configuration properties
----------------------------------------

The main point of hdt-server is exposing a SPARQL endpoint over an HDT file.
As with the querying triple patterns against HDT and processing SPARQL algebra 
operators, the SPARQL endpoint has multiple flow-control/implementation flavors:

1. `BATCH`: A serialization of the SPARQL results for the query is built 
   in-memory and then sent as a response.
2. `CHUNKED`: The server will start sending the serialized SPARQL solutions 
   before they are completely enumerated, and thus will not try to hold the
   entire serialization in memory. This is implemented using HTTP 1.1 chunked 
   transfer encoding. If the client closes the connection soon enough, the 
   server may stop processing the query before all solutions are enumerated. 
   The benefit for distinct or sorted is limited, given their semantics. 

The endpoint implementation is selected using the following property:

> `sparql.endpoint.flow=BATCH|CHUNKED`
> 
> Whether to use a `BATCH` or `CHUNKED` (the **default**) strategy for SPARQL 
> query result serializations.
 
Regardless of the the `BATCH` or `CHUNKED` endpoint implementations, 
serialization will always be offloaded to an IO-thread, as is recommended 
by micronaut in order to not block the netty event loop.

hdtss periodically logs simple statistics about submitted queries. This occurs 
at a configurable fixed rate:

> `sparql.heartbeat.period=DURATION`
> 
> The period between each heartbeat log message. **The default** period is `1m`.

> `sparql.heartbeat.level=ERROR|WARN|INFO|DEBUG|TRACE`
> 
> Each heartbeat log message will be written from a class within `com.github.lapesd.hdtss.controller` 
> at this SLF4J level. The **default** is `INFO`
 

### Websocket support

In addition to the standard [HTTP-based](https://www.w3.org/TR/sparql11-protocol/) 
protocol for SPARQL, hdtss also supports websocket as a vendor-specific 
alternative. The targeted use case for websocket is SPARQL mediators which 
perform joins by replacing variables on the right-side operand and executing 
the now bound query. With a websocket, the client can concurrently send 
additional bindings, avoiding parse&optimize cycles on the server and reducing 
the total client-server communication.     

The protocol on top of websockets is described at [WEBSOCKET.md](WEBSOCKET.md).

The following configuration properties set per-session limits on the server: 

> `sparql.ws.action-queue=INTEGER`
> 
> The size of the actions queue held per websocket session by the server. This 
> size will be reported to clients in response to `!qeuue-cap`. The **default** 
> value is 8.
 
> `sparql.ws.bind-request=INTEGER`
>
> The _n_ in `!bind-request n` sent by the server to receive bindings when 
> processing a `!bind` action. The default value is `64`.  

When the server sends result rows (assigning RDF terms to unbound variables 
in a query), it will try to bundle multiple rows into a single websocket 
message. This has the benefit of reducing protocol overhead, but introduces 
a minimum latency. The following properties change that behavior.

> `sparql.ws.window.size=INTEGER"
> 
> The maximum number of rows to be bundled into a single websocket message. 
> A message will be sent once this number of rows is available or the set 
> maximum wait time (`sparql.ws.window.us`) has elapsed, whichever happens 
> first. The **default** is `16` rows. Setting this to a value `<= 1` will 
> disable bundling (yielding one immediate message for every row). 

> `sparql.ws.window.us=INTEGER"
> 
> The maximum number of microseconds to wait for rows when bundling multiple 
> rows into a single websocket message. A websocket message with all rows will
> be emitted once this amount of microseconds has elapsed or once 
> `sparql.ws.window.size` rows have been collected, whichever occurs first. 
> The **default** is `500` microseconds. If this is set to something `<= 0`, 
> no bundling will occur (each row will immediately yield a websocket message).
 
A websocket session may become innactive after some time if the client is 
slow at providing new bindings or the server is slow at finding results. Such 
scenario could arise on queries that apply selective FILTER clauses on large 
result sets. To signal its presence, each peer may send a `!ping` message to 
be answered with an `!ping-ack`. The server by default sends a ping every 
2 minutes:

> `sparql.ws.ping.secs=INTEGER`
> 
> The interval in seconds after which a !ping will be sent by the server on a 
> session that had no activity from neither the client nor the server. Any 
> incoming or outgoing message resets this timer. 
> The **default** is `120`, i.e., 2 minutes