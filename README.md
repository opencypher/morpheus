[![Maven Central](https://img.shields.io/badge/Maven_Central-0.3.0-blue.svg?label=Maven%20Central)](https://search.maven.org/#artifactdetails%7Corg.opencypher%7Cspark-cypher%7C0.3.0%7Cjar)
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

## Current status: Pre-release

The functionality and APIs are stabilizing but surface changes (e.g. to the Cypher syntax and semantics for multiple graph processing and graph projections/construction) are still likely to occur. 
We invite you to try out the project, and we welcome feedback and contributions.

If you are interested in contributing to the project we would love to hear from you; email us at `opencypher@neo4j.org` or just raise a PR. 
Please note that this is an openCypher project and contributions can only be accepted if you’ve agreed to the  [openCypher Contributors Agreement (oCCA)](CONTRIBUTING.adoc).

## Documentation

A preview of the documentation for Morpheus, which is a commercially supported version of CAPS, is [available from Neo4j](https://neo4j.com/docs/morpheus-user-guide/1.0-preview/).

## CAPS Features

CAPS is built on top of the Spark DataFrame API and uses features such as the Catalyst optimizer.
The Spark representations are accessible and can be converted to representations that integrate with other Spark libraries.

CAPS supports [a subset of Cypher](https://github.com/opencypher/cypher-for-apache-spark/blob/master/documentation/asciidoc/cypher-cypher9-features.adoc) and is the first implementation of [multiple graphs](https://github.com/boggle/openCypher/blob/CIP2017-06-18-multiple-graphs/cip/1.accepted/CIP2017-06-18-multiple-graphs.adoc) and graph query compositionality.

CAPS currently supports importing graphs from both Neo4j and from custom [CSV format](https://github.com/opencypher/cypher-for-apache-spark/tree/master/caps-core/src/test/resources/csv/sn) in HDFS and local file system.
CAPS has a data source API that allows you to plug in custom data importers for external graphs.

## CAPS Roadmap

CAPS is under rapid development and we are planning to offer support for:
- a large subset of the Cypher language
- new Cypher Multiple Graph features
- integration with Spark SQL
- injection of custom graph data sources

## Spark Project Improvement Proposal

Currently CAPS is a third-party add-on to the Spark ecosystem. We, however, believe that property graphs and graph processing
has the potential to be come a vital part of data analytics. We are thus working, in cooperation with 
*Databricks*, on making CAPS a core part of Spark. 
The first step on this road is the specification of a __PropertyGraph API__, similar to __SQL__ and __Dataframes__, along with porting
Cypher 9 features of CAPS to the core Spark project in a so called __Spark Project Improvement Proposal__ (SPIP).

We are currently in the second phase of this process, after having successfully [passed the vote for inclusion](http://apache-spark-developers-list.1001551.n3.nabble.com/VOTE-RESULT-SPIP-DataFrame-based-Property-Graphs-Cypher-Queries-and-Algorithms-td26401.html) into Apache Spark 3.0.
The SPIP describing the motivation and goals is published here
[SPARK-25994](https://issues.apache.org/jira/browse/SPARK-25994). 
Additionally [SPARK-26028](https://issues.apache.org/jira/browse/SPARK-26028) proposes an API design and implementation strategies. 

## Get started with CAPS
CAPS is currently easiest to use with Scala. Below we explain how you can import a simple graph and run a Cypher query on it.

### Building CAPS

CAPS is built using Gradle

```
./gradlew build
```


#### Add the CAPS dependency to your project
In order to use CAPS add the following dependency:

Maven:

```
<dependency>
  <groupId>org.opencypher</groupId>
  <artifactId>spark-cypher</artifactId>
  <version>0.3.0</version>
</dependency>
```

sbt:
```
libraryDependencies += "org.opencypher" % "spark-cypher" % "0.3.0"
```

Remember to add `fork in run := true` in your `build.sbt` for scala projects; this is not CAPS
specific, but a quirk of spark execution that will help 
[prevent problems](https://stackoverflow.com/questions/44298847/why-do-we-need-to-add-fork-in-run-true-when-running-spark-sbt-application).

### Hello CAPS

Cypher is based on the [property graph](https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc) data model, comprising labelled nodes and typed relationships, with a relationship either connecting two nodes, or forming a self-loop on a single node. 
Both nodes and relationships are uniquely identified by an ID (in CAPS this is of type `Long`), and contain a set of properties. 

The following example shows how to convert a social network represented as Scala case classes to a `PropertyGraph` representation. 
The `PropertyGraph` representation is internally transformed into Spark data frames. 
If you have existing data frames which you would like to treat as a graph, have a look at our [DataFrameInputExample](spark-cypher-examples/src/main/scala/org/opencypher/spark/examples/DataFrameInputExample.scala).   

Once the property graph is constructed, it supports Cypher queries via its `cypher` method.

```scala
import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates basic usage of the CAPS API by loading an example graph from [[DataFrame]]s.
  */
object DataFrameInputExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession

  import spark.sqlContext.implicits._

  // 2) Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF = spark.createDataset(Seq(
    (0L, "Alice", 42L),
    (1L, "Bob", 23L),
    (2L, "Eve", 84L)
  )).toDF("id", "name", "age")
  val relsDF = spark.createDataset(Seq(
    (0L, 0L, 1L, "23/01/1987"),
    (1L, 1L, 2L, "12/12/2009")
  )).toDF("id", "source", "target", "since")

  // 3) Generate node- and relationship tables that wrap the DataFrames. The mapping between graph entities and columns
  //    is derived using naming conventions for identifier columns.
  val personTable = CAPSNodeTable(Set("Person"), nodesDF)
  val friendsTable = CAPSRelationshipTable("KNOWS", relsDF)

  // 4) Create property graph from graph scans
  val graph = session.readFrom(personTable, friendsTable)

  // 5) Execute Cypher query and print results
  val result = graph.cypher("MATCH (n:Person) RETURN n.name")

  // 6) Collect results into string by selecting a specific column.
  //    This operation may be very expensive as it materializes results locally.
  val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("n_name")).toSet

  println(names)
}
```

The above program prints:
```
Set(Alice, Bob, Eve)
```

More examples, including [multiple graph features](spark-cypher-examples/src/main/scala/org/opencypher/spark/examples/MultipleGraphExample.scala), can be found [in the examples module](spark-cypher-examples).

### Run example Scala apps via command line

You can use Gradle to run a specific Scala application from command line. For example, to run the `CaseClassExample` 
within the `spark-cypher-examples` module, we just call:

```
./gradlew spark-cypher-examples:runApp -PmainClass=org.opencypher.spark.examples.CaseClassExample
```

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

The project is licensed under the Apache Software License, Version 2.0, with an extended attribution notice as described in [the license header](/etc/licenses/headers/NOTICE-header.txt).

## Copyright

© Copyright 2016-2019 Neo4j, Inc.

Apache Spark™, Spark, and Apache are registered trademarks of the [Apache Software Foundation](https://www.apache.org/).
