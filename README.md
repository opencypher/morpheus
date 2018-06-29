[![Build Status](https://travis-ci.org/opencypher/cypher-for-apache-spark.svg?branch=master)](https://travis-ci.org/opencypher/cypher-for-apache-spark)
[![Maven Central](https://img.shields.io/badge/Maven_Central-0.3.2-blue.svg?label=Maven%20Central)](http://search.maven.org/#search%7Cga%7C1%7Cspark-cypher)
# CAPS: Cypher for Apache Spark

CAPS extends [Apache Spark™](https://spark.apache.org) with [Cypher](https://neo4j.com/docs/developer-manual/current/cypher/), the industry's most widely used [property graph](https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc) query language defined and maintained by the [openCypher](http://www.opencypher.org) project.
It allows for the **integration** of many **data sources** and supports **multiple graph** querying.
It enables you to use your Spark cluster to run **analytical graph queries**.
Queries can also return graphs to create **processing pipelines**.

## Intended audience

CAPS allows you to develop complex processing pipelines orchestrated by a powerful and expressive high-level language.
In addition to **developers** and **big data integration specialists**, CAPS is also of practical use to **data scientists**, offering tools allowing for disparate data sources to be integrated into a single graph. From this graph, queries can extract subgraphs of interest into new result graphs, which can be conveniently exported for further processing.

CAPS builds on the Spark SQL DataFrame API, offering integration with standard Spark SQL processing and also allows
integration with GraphX. To learn more about this, please see our [examples](https://github.com/opencypher/cypher-for-apache-spark/tree/master/spark-cypher-examples).
 
<!-- TODO: WIKI How does it relate to GraphFrames -->
<!--- **Data Analysts**: -->
<!--  This example shows how to aggregate detailed sales data within a graph — in effect, performing a ‘roll-up’ — in order to obtain a high-level summarized view of the data, stored and returned in another graph, as well as returning an even higher-level view as an executive report. The summarized graph may be used to draw further high-level reports, but may also be used to undertake ‘drill-down’ actions by probing into the graph to extract more detailed information.-->

## Current status: Beta

The project is currently in a beta stage, which means that the code and the functionality is changing, but the APIs are stabilising.
We invite you to try it and welcome any feedback.

The first 1.0 release for the project is targeted for September 2018. 

## CAPS Features

CAPS is built on top of the Spark DataFrame API and uses features such as the Catalyst optimizer.
The Spark representations are accessible and can be converted to representations that integrate with other Spark libraries.

CAPS supports a subset of Cypher <!-- TODO: WIKI supported features --> and is the first implementation of [multiple graphs](https://github.com/boggle/openCypher/blob/CIP2017-06-18-multiple-graphs/cip/1.accepted/CIP2017-06-18-multiple-graphs.adoc) and graph query compositionality.

CAPS currently supports importing graphs from both Neo4j and from custom [CSV format](https://github.com/opencypher/cypher-for-apache-spark/tree/master/caps-core/src/test/resources/csv/sn) in HDFS and local file system.
CAPS has a data source API that allows you to plug in custom data importers for external graphs.

## CAPS Roadmap

CAPS is under rapid development and we are planning to offer support for:
- a large subset of the Cypher language
- new Cypher Multiple Graph features
- integration with Spark SQL
- injection of custom graph data sources

## Get started with CAPS
CAPS is currently easiest to use with Scala. Below we explain how you can import a simple graph and run a Cypher query on it.

### Building CAPS

CAPS is built using Maven

```
mvn clean install
```


#### Add the CAPS dependency to your project
In order to use CAPS add the following dependency:

Maven:

```
<dependency>
  <groupId>org.opencypher</groupId>
  <artifactId>spark-cypher</artifactId>
  <version>0.1.4</version>
</dependency>
```

sbt:
```
libraryDependencies += "org.opencypher" % "spark-cypher" % "0.1.4"
```

Remember to add `fork in run := true` in your `build.sbt` for scala projects; this is not CAPS
specific, but a quirk of spark execution that will help 
[prevent problems](https://stackoverflow.com/questions/44298847/why-do-we-need-to-add-fork-in-run-true-when-running-spark-sbt-application)

### Generating API documentation

```
mvn scala:doc
```

Documentation will be generated and placed under `[MODULE_DIRECTORY]/target/site/scaladocs/index.html`

### Hello CAPS

Cypher is based on the [property graph](https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc) data model, comprising labelled nodes and typed relationships, with a relationship either connecting two nodes, or forming a self-loop on a single node. 
Both nodes and relationships are uniquely identified by an ID (in CAPS this is of type `Long`), and contain a set of properties. 

The following example shows how to convert a social network represented as Scala case classes to a `PropertyGraph` representation. 
The `PropertyGraph` representation is internally transformed into Spark data frames. 
If you have existing data frames which you would like to treat as a graph, have a look at our [DataFrameInputExample](spark-cypher-examples/src/main/scala/org/opencypher/spark/examples/DataFrameInputExample.scala).   

Once the property graph is constructed, it supports Cypher queries via its `cypher` method.

```scala
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates basic usage of the CAPS API by loading an example network via Scala case classes and running a Cypher
  * query on it.
  */
object CaseClassExample extends ConsoleApp {

  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since
       |ORDER BY a.name""".stripMargin
  )

  // 4) Print result to console
  results.show
}

/**
  * Specify schema and data with case classes.
  */
object SocialNetworkData {

  case class Person(id: Long, name: String, age: Int) extends Node

  @RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

  val alice = Person(0, "Alice", 10)
  val bob = Person(1, "Bob", 20)
  val carol = Person(2, "Carol", 15)

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
```

The above program prints:
```
╔═════════╤═════════╤══════════════╗
║ a.name  │ b.name  │ r.since      ║
╠═════════╪═════════╪══════════════╣
║ 'Alice' │ 'Bob'   │ '23/01/1987' ║
║ 'Bob'   │ 'Carol' │ '12/12/2009' ║
╚═════════╧═════════╧══════════════╝
(2 rows)
```

More examples, including [multiple graph features](spark-cypher-examples/src/main/scala/org/opencypher/spark/examples/MultipleGraphExample.scala), can be found [in the examples module](spark-cypher-examples).

### Loading CSV Data

See the documentation in `org.opencypher.spark.impl.io.hdfs.CsvGraphLoader`, which specifies how to structure the
CSV and the schema mappings that describe the graph structure for the underlying data.

#### Next steps

- How to use CAPS in [Apache Zeppelin](https://github.com/opencypher/cypher-for-apache-spark/wiki/Use-CAPS-in-a-Zeppelin-notebook)
- Look at and contribute to the [Wiki](https://github.com/opencypher/cypher-for-apache-spark/wiki)
<!-- TODO: Steps needed to run the demo with toy data -->
<!-- TODO: WIKI article that demonstrates a more realistic use case with HDFS data source -->
<!-- TODO: WIKI link to page that explains how to import data -->

## How to contribute

We would love to find out about any [issues](https://github.com/opencypher/cypher-for-apache-spark/issues) you encounter and are happy to accept contributions following a Contributors License Agreement (CLA) signature as per the process outlined in our [contribution guidelines](CONTRIBUTING.adoc).

## License

The project is licensed under the Apache Software License, Version 2.0, with an extended attribution notice as described in [the license header](license-header.txt).

## Copyright

© Copyright 2016-2018 Neo4j, Inc.

Apache Spark™, Spark, and Apache are registered trademarks of the [Apache Software Foundation](https://www.apache.org/).
