HGraph
======

HGraph is an opensource (apache license v2.0) for storing graph data in HBase and running graph related algorithmns

We develop this project to fulfill requirements for processing large volumne of graph data, in our internal services. The usecase shown in following picture.

![our usecase](https://dl.dropboxusercontent.com/u/9473777/hgraph/usecase-01.png)


So what HGraph mainly supports are...

2. a HBase schema design specifically for storing graph data
For writing data, we use MR/pig/bulkload tool to write big volumne of data into HBase, which means that currently HGraph does not support writing feature.

3. a partial graph API impl. derived from [Blueprints API](https://github.com/tinkerpop/blueprints)
HGraph currently supports simple traversal features, not including complex search yet.

4. Based on the HBase schema design, graph algorithms can process on the graph data with MapReduce job
Here is a simple pagerank impl. in package 'org.trend.hgraph.mapreduce.pagerank', you can run it to get some feelings. Or you can write your own algorithms to use these code as templates.


## HBase schema design
We use following schema to store our graph data
