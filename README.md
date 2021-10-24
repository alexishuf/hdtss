HDT SPARQL Endpoint (`hdtss`)
=============================

`hdt-server` exposes a SPARQL endpoint over HTTP from an HDT file. 

The goals of this implementation are:

- Quick startup (if the HDT index sidecar already exists)
  - aka [GraalVM](https://www.graalvm.org/) native images
- Concurrent query processing and results transmission
  - `Transfer-Encoding: chunked` allows any solution to a query to be sent 
    ASAP. For large query results and **clients capable of incremental 
    parsing**, this can provide dramatic latency improvements. Paging over query 
    solutions with `LIMIT` and `OFFSET` is thus not recommended.  
- Configurable internal flow control strategies
  - Choose between pull-based `Iterator`s or push/pull reactive 
    streams (`Flux<>`).   

The goal is to support all of [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/). 
The [SPARQL Update language](https://www.w3.org/TR/2013/REC-sparql11-update-20130321/) 
is out of scope, since HDT files are not designed for mutability.

Currently, the missing SPARQL 1.1 features are:

- `CONSTRUCT` queries
- Property paths
- Named graph blocks (`GRAPH <...> {...}` or `GRAPH ?g {...}`)
- Named graphs set with `FROM` clause or via the SPARQL protocol

These will (hopefully) be implemented soon. 

Quickstart
----------

Build (fast) and expose a SPARQL endpoint at [http://localhost:8080/sparql](http://localhost:8080/sparql):

```shell
./mvnw package -DskipTests=true 
target/hdtss -hdt.location=doc/foaf-graph.hdt
```

> hdtss as generated with packaging=jar (the default) is both a shell-script 
> and fat jar (including all dependencies). It will behave as an executable 
> as long as `java` is in `$PATH` 

A statically linked binary can be built with [GraalVM](https://www.graalvm.org/), either:
 - using a GraalVM in the host system (`-Dpackaging=native-image`), or 
 - using a Docker container (`-Dpackaging=docker-native`).  

```shell
./mvnw package -Dpackaging=native-image 
target/hdtss -hdt.location=doc/foaf-graph.hdt
```

> GraalVM native images are start faster and are executable without a JVM
> The build will be slower (>3min) and require ~9 GiB of RAM.

SPARQL queries can be submitted to `/sparql` as per the 
[SPARQL protocol](https://www.w3.org/TR/sparql11-protocol/): 
```shell
curl -data-urlencode query=@doc/who_knows_alice.sparql \
      -H "Accept: text/tab-separated-values" \
      http://localhost:8080/sparql
```
 
Examples
--------

Change the port:
```shell
hdtss -port=8080 file.hdt
```

Use reactive streams on all intermediary operators:
```shell
hdtss -sparql.flow=REACTIVE file.hdt
```

Do not use reactive streams anywhere (the SPARQL endpoint will not use
`Transfer-Encoding chunked`):
```shell
hdtss -sparql.flow=ITERATOR -sparql.endpoint.flow=BATCH file.hdt
```

Configuration
-------------

Configuration is handled by [Micronaut](http://micronaut.io), and centers 
around _configuration properties_. Each configuration property can be named 
in different styles, depending on where it is being set. The command line and 
documentation uses Java properties syntax: `parent.child.property`. On JSON 
or YAML files, the `.` yields a hierarchical structure. In environment 
variables, `.` and `-`  become `_` and everything is upper case. 

> For usability, a positional command-line argument (i.e., it is not a 
> value of an -option or --option) is assumed to be a value for the 
> `-hdt.location` option. 

The `hdtss` configuration properties and their defaults are documented 
[here](./doc/CONFIG.md) 

> Micronaut itself exposes some properties, such as -port and -host.

The command-line interface can take properties preceded with either a single 
`-` or `--`. There are single-letter options. Properties can be set in a 
single `-name=value` argument or with a follow-up argument as in `-name value`.
If a property such as `thing.enable` expects a boolean, simply passing 
`-thing.enable` will set it to true. 

For an example of file-based configuration, see this 
[example file](./doc/application-example.yaml). Notice that it will have no 
effect unless copied as `application.yaml` to the current working directory.

The most popular configuration sources loaded by Micronaut from highest to 
lowest precedence are:

1. Command-line flags
2. JVM properties (set with -D or .properties files in the classpath)
3. Environment variables 
4. `application-{environment}.{extension}` files
    - see the micronaut docs for details about [environments](https://docs.micronaut.io/latest/guide/#environments)
    - `.properties`, `.json` and `.yaml` are supported (among them, precedence is undefined)
5. `application.{extension}`

For more detail about property sources, syntax variations and their precedence,
see the [Micronaut docs](https://docs.micronaut.io/latest/guide/#propertySource).
