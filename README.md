[![Build Status](https://travis-ci.org/opencypher/cypher-for-apache-spark.svg?branch=master)](https://travis-ci.org/opencypher/cypher-for-apache-spark)
# CAPS: Cypher for Apache Spark

CAPS extends [Apache Spark™](https://spark.apache.org) with [Cypher](https://neo4j.com/docs/developer-manual/current/cypher/), the industry's most widely used [property graph](https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc) query language defined and maintained by the [openCypher](http://www.opencypher.org) project.
It allows for the **integration** of many **data sources** and supports **multiple graph** querying.
It enables you to use your Spark cluster to run **analytical graph queries**.
Queries can also return graphs to create **processing pipelines**.

Below you see a screenshot of running a Cypher query with CAPS running in a [Zeppelin notebook](https://github.com/opencypher/cypher-for-apache-spark/wiki/Use-CAPS-in-a-Zeppelin-notebook):
![CAPS Zeppelin Screenshot](doc/images/zeppelin_screenshot.png)

## Intended audience

CAPS allows you to develop complex processing pipelines orchestrated by a powerful and expressive high-level language.
In addition to **developers** and **big data integration specialists**, CAPS is also of practical use to **data scientists**, offering tools allowing for disparate data sources to be integrated into a single graph. From this graph, queries can extract subgraphs of interest into new result graphs, which can be conveniently exported for further processing.

In addition to traditional analytical techniques, the graph data model offers the opportunity to use Cypher and *[Neo4j graph algorithms](https://neo4j.com/blog/efficient-graph-algorithms-neo4j/)* to derive deeper insights from your data.
<!-- TODO: WIKI How does it relate to GraphX and GraphFrames -->
<!--- **Data Analysts**: -->
<!--  This example shows how to aggregate detailed sales data within a graph — in effect, performing a ‘roll-up’ — in order to obtain a high-level summarized view of the data, stored and returned in another graph, as well as returning an even higher-level view as an executive report. The summarized graph may be used to draw further high-level reports, but may also be used to undertake ‘drill-down’ actions by probing into the graph to extract more detailed information.-->

## Current status: Alpha

The project is currently in an alpha stage, which means that the code and the functionality are still changing. We haven't yet tested the system with large data sources and in many scenarios. We invite you to try it and welcome any feedback.

## CAPS Features

CAPS is built on top of the Spark DataFrames API and uses features such as the Catalyst optimizer.
The Spark representations are accessible and can be converted to representations that integrate with other Spark libraries.

CAPS supports a subset of Cypher <!-- TODO: WIKI supported features --> and is the first implementation of [multiple graphs](https://github.com/boggle/openCypher/blob/CIP2017-06-18-multiple-graphs/cip/1.accepted/CIP2017-06-18-multiple-graphs.adoc) and graph query compositionality.

CAPS currently supports importing graphs from both Neo4j and from custom [CSV format](https://github.com/opencypher/cypher-for-apache-spark/tree/master/caps-core/src/test/resources/csv/sn) in HDFS.
CAPS has a data source API that allows you to plug in custom data importers for external sources.

## CAPS Roadmap

CAPS is under rapid development and we are planning to:
- Support more Cypher features
- Make it easier to use by offering it as a Spark package and by integrating it into a notebook
- Provide additional integration APIs for interacting with existing Spark libraries such as SparkSQL and MLlib

## Get started with CAPS
CAPS is currently easiest to use with Scala. Below we explain how you can import a simple graph and run a Cypher query on it.

### Building CAPS

CAPS is built using Maven

```
mvn clean install
```

<!--
#### Add the CAPS dependency to your project
In order to use CAPS add the following dependency to Maven:
<!-- TODO: Publish to Maven Central -- >
```
<dependency>
  <groupId>org.opencypher.caps</groupId>
  <artifactId>caps_2.11</artifactId>
  <version>0.1.0-NIGHTLY</version>
</dependency>
```
-->

### Generating API documentation (in progress)

```
mvn scala:doc
```

Documentation will be generated and placed under `caps-core/target/site/scaladocs/index.html`

### Hello CAPS

Cypher is based on the [property graph](https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc) model, comprising labelled nodes and typed relationships, with a relationship either connecting two nodes, or forming a self-loop on a single node. 
Both nodes and relationships are uniquely identified by an ID of type `Long`, and contain a set of properties. 

The following example shows how to convert a social network represented as Scala case classes to a `PropertyGraph` representation. 
The `PropertyGraph` representation is internally transformed into Spark data frames. 
If you have existing data frames which you would like to treat as a graph, have a look at (this example)[TODO].   

Once the property graph is constructed, it supports Cypher queries via its `cypher` method.

```scala
import org.opencypher.caps.api._

object SimpleExample extends App {

  // 1) Create CAPS session
  implicit val session = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFromSeqs(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since""".stripMargin
  )

  // 4) Print results to console
  results.print
}

/**
  * Specify schema and data via case classes
  */
object SocialNetworkData {

  case class Person(id: Long, name: String) extends schema.Node

  @schema.RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends schema.Relationship

  val alice = Person(0, "Alice")
  val bob = Person(1, "Bob")
  val carol = Person(2, "Carol")

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
```

The above program prints:
```
+--------------------------------------------------------------------+
| a.name               | b.name               | r.since              |
+--------------------------------------------------------------------+
| 'Alice'              | 'Bob'                | '23/01/1987'         |
| 'Bob'                | 'Carol'              | '12/12/2009'         |
+--------------------------------------------------------------------+
```

More examples, including [multiple graph features](https://github.com/opencypher/cypher-for-apache-spark/tree/master/caps-spark/src/main/scala/org/opencypher/caps/demo/MultiGraphExample.scala), can be found [in the demo package](https://github.com/opencypher/cypher-for-apache-spark/tree/master/caps-spark/src/main/scala/org/opencypher/caps/demo).

Remember to add `fork in run := true` in your `build.sbt` for scala projects; this is not CAPS
specific, but a quirk of spark execution that will help 
[prevent problems](https://stackoverflow.com/questions/44298847/why-do-we-need-to-add-fork-in-run-true-when-running-spark-sbt-application)

### Loading CSV Data

See the documentation in `org.opencypher.caps.impl.spark.io.hdfs.CsvGraphLoader`, which specifies how to structure the
CSV and the schema mappings that describe the graph structure for the underlying data.

#### Next steps

- How to use CAPS in [Apache Zeppelin](https://github.com/opencypher/cypher-for-apache-spark/wiki/Use-CAPS-in-a-Zeppelin-notebook)
- Look at and contribute to the [Wiki](https://github.com/opencypher/cypher-for-apache-spark/wiki)
<!-- TODO: Multiple graphs example -->
<!-- TODO: Steps needed to run the demo with toy data -->
<!-- TODO: WIKI article that demonstrates a more realistic use case with HDFS data source -->
<!-- TODO: WIKI link to page that explains how to import data -->

## How to contribute

We'd love to find out about any issues you encounter. We welcome code contributions -- please open an [issue](https://github.com/opencypher/cypher-for-apache-spark/issues) first to ensure there is no duplication of effort. <!-- TODO: Determine CLA and process -->

## License

The project is licensed under the Apache Software License, Version 2.0

## Copyright

© Copyright 2016-2017 Neo4j, Inc.

Apache Spark™, Spark, and Apache are registered trademarks of the [Apache Software Foundation](https://www.apache.org/).
