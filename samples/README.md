Call HGraph from Groovy
=======================
```groovy
import org.trend.hgraph.HBaseGraphFactory
import org.apache.hadoop.hbase.HBaseConfiguration
conf = HBaseConfiguration.create()
conf.set("hbase.graph.table.vertex.name", "hgraph.vertex-v06")
conf.set("hbase.graph.table.edge.name", "hgraph.edge-v06")
graph = HBaseGraphFactory.open(conf)
v = graph.getVertex("jupiter||name")
```

Call HGraph from Scala
======================
```scala
import org.trend.hgraph.HBaseGraphFactory
import org.apache.hadoop.hbase.HBaseConfiguration
val conf = HBaseConfiguration.create()
conf.set("hbase.graph.table.vertex.name", "hgraph.vertex-v06")
conf.set("hbase.graph.table.edge.name", "hgraph.edge-v06")
val graph = HBaseGraphFactory.open(conf)
var v = graph.getVertex("jupiter||name")
```


Call HGraph from Clojure
========================
```clojure
(import org.trend.hgraph.HBaseGraphFactory)
(import org.apache.hadoop.hbase.HBaseConfiguration)
(def conf (. HBaseConfiguration create))
(. conf set "hbase.graph.table.vertex.name" "hgraph.vertex-v06")
(. conf set "hbase.graph.table.edge.name" "hgraph.edge-v06")
(def graph (. HBaseGraphFactory open conf))
(def v (. graph getVertex "jupiter||name"))
```
