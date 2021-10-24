native-image-agent
==================

Micronaut ships (and generates) `META-INF/native-image/**/` configuration files 
that instruct GraalVM `native-image` tool to preserve reflection metadata. 
Some dependencies, mainly `org.rdfhdt:hdt-java-core` and 
`org.apache.jena:apache-jena-libs` do not ship those files and may cause 
(sometimes hard to diagnose) errors at runtime when missing data is requested.   

First, create a fat jar:

```shell
./mvnw clean package -DskipTests=true
```

Run `hdtss` with GraalVM tracing agent.  

```shell
java -agentlib:native-image-agent=config-output-dir=doc/native/config,caller-filter-file=doc/native/caller-filter.json \
  -jar target/hdtss doc/foaf-graph.hdt
```

> `config-output-dir` truncates any old files. Use `config-merge-dir` to 
> merge old files with the results of the current run.
 
> `caller-filter` removes accesses from code that should be already configured 

> If `config-{output,merge}-dir` is replaced `trace-output=doc//native/config/trace.json` 
> which class made with reflection API call will be logged. This logging does 
> not include the caller method. 

All configurations should be shipped under `src/main/resources/META-INF/native-image/${groupId}/${artifactId}/`,
create the relevant `*.json` in that directory if it does not already exist.
