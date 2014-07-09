HGraph
======

HGraph is an opensource (apache license v2.0) for storing graph data in HBase and running graph related algorithmns

We develop this project to fulfill requirements for processing large volumne of graph data, in our internal services. The usecase shown in following picture.

![our usecase](https://dl.dropboxusercontent.com/u/9473777/hgraph/usecase-01.png)


So what HGraph mainly supports are...

* a HBase schema design specifically for storing graph data

For writing data, we use MR/pig/bulkload tool to write big volumne of data into HBase, which means that currently HGraph does not support writing feature.

* a partial graph API impl. derived from [Blueprints API](https://github.com/tinkerpop/blueprints)

HGraph currently supports simple traversal features, not including complex search yet.

* Based on the HBase schema design, graph algorithms can process on the graph data with MapReduce job

Here is a simple pagerank impl. in package 'org.trend.hgraph.mapreduce.pagerank', you can run it to get some feelings. Or you can write your own algorithms to use these code as templates.


## HBase schema design
We use following schema to store our graph data

    --hbase schema
    <rowkey>, <column-family:column-qualifier>, <value>
    --Table: vertex
    '<vertex-id>||<entity-type>', 'property:<property-key>@<property-value-type>', <property-value>
    --Table: edge
    '<vertex1-row-key>--><label>--><vertex2-row-key>', 'property:<property-key>@<property-value-type>', <property-value>

Here is a example

    -- vertex table, has two vertex instances, one domain, and one url
    'myapps-ups.com||domain', 'property:ip@String', '…'
    'myapps-ups.com||domain', 'property:asn@String', '…'
    …
    'track.muapps-ups.com/InvoiceA1423AC.JPG.exe||url', 'property:path@String', '…'
    'track.muapps-ups.com/InvoiceA1423AC.JPG.exe||url', 'property:parameter@String', '…'
    -- edge table, has one edge instances, with two properties 
    'myapps-ups.com||domain-->host-->track.muapps-ups.com/InvoiceA1423AC.JPG.exe||url', 
    'property:property1', '…'
    'myapps-ups.com||domain-->host-->track.muapps-ups.com/InvoiceA1423AC.JPG.exe||url', 
    'property:property2', '…'
    
So you can use following command to create the tables in hbase shell

    -- create vertex table
    create 'vertex', {NAME => 'property', BLOOMFILTER => 'ROW', COMPRESSION => ‘SNAPPY', TTL => '7776000'}
    -- create edge table
    create 'edge', {NAME => 'property', BLOOMFILTER => 'ROW', COMPRESSION => ‘SNAPPY', TTL => '7776000'}
    
## Access data via graph API
We use a graph API as a wrapper for the underlying HBase client API manipulations, this provides better semantic for user to access the graph data. Following is a sample code to use the graph API to get the vertex and edge instances

```java
// initial configuration
Configuration conf = HBaseConfigurastion.create();
conf.set("hbase.graph.table.vertex.name", "vertex");
conf.set("hbase.graph.table.edge.name", "edge");
Graph graph = HBaseGraphFactory.open(TEST_UTIL.getConfiguration());

// get vertex and edge instances
Vertex vertex = this.graph.getVertex("malware");
Vertex subVertex = null;
Iterable<Edge> edges = 
	vertex.getEdges(Direction.OUT, "connect", "infect", "trigger");
for(Edge edge : edges) {
  subVertex = edge.getVertex(Direction.OUT);
  // do further process...
}
```

You can refer to our [testcases](https://github.com/trendmicro/HGraph/tree/master/src/test/java/org/trend/hgraph) for more detailed info.

## Run PageRank
Here is a bunch of MR classes to assemble a default PageRank impl., pls see our [sources](https://github.com/trendmicro/HGraph/tree/master/src/main/java/org/trend/hgraph/mapreduce/pagerank) for more details.

To run the pageRank, execute following script directly

    HGRAPH_HOME/scripts/pr/pr.sh -c -i vertex edge pr-results

After pageRank finished, you can see the ranks already stored in your vertex table


## Scala / Clojure / Groovy
Refer to samples/ for how HGraph is called from Scala / Clojure / Groovy.

License
=======
Apache 2.  See LICENSE.txt for details.
