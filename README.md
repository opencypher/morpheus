# CAPS: Cypher for Apache Spark

CAPS extends [Apache Spark™](https://spark.apache.org) with [Cypher](http://www.opencypher.org), the industry's most widely used graph query language.
It allows for the **integration** of many **data sources** and supports **multiple graph** querying.
It enables you to use your Spark cluster to run **analytical graph queries**.
Queries can also return graphs to create **processing pipelines**.

## Intended audience

CAPS allows you to develop complex processing pipelines orchestrated by a powerful and expressive high-level language.
Beyond developers and big data integration specialist, CAPS is also of practical use for:

<!--- **Developer**: CAPS allows to conveniently *export result graphs*.-->
- **Data Scientists**: Extract the data items of interest into new result graphs. From there conveniently export the graphs for further processing or use Cypher and *[Neo4j graph algorithms](https://neo4j.com/blog/efficient-graph-algorithms-neo4j/)* to derive deeper insights from your data.
<!-- TODO: WIKI How does it relate to GraphX and GraphFrames -->
- **Data Analysts**: <!--  This example shows how to aggregate detailed sales data within a graph — in effect, performing a ‘roll-up’ — in order to obtain a high-level summarized view of the data, stored and returned in another graph, as well as returning an even higher-level view as an executive report. The summarized graph may be used to draw further high-level reports, but may also be used to undertake ‘drill-down’ actions by probing into the graph to extract more detailed information.-->

## Current status: Alpha

The project is currently in an alpha stage, which means that the code and the functionality are still changing. We haven't yet tested the system with large data sources and in many scenarios. We invite you to try it and welcome any feedback.

## CAPS Features & Roadmap

CAPS supports a subset of Cypher <!-- TODO: WIKI supported features --> and is the first implementation of multiple graphs and graph query compositionality <!-- TODO: WIKI openCypher and Cypher improvement proposal -->.

CAPS currently supports importing graphs from both Neo4j and from CSV+HDFS <!-- TODO: WIKI Clarify and maybe link to page that explains how to import data -->.

We are planning to support:
- Integration with existing Spark libraries via the DataFrame API
- Importing graphs from external sources and offer a pluggable data source interface
- Making it easy to use as a [Spark package](https://spark-packages.org)

## Getting started with CAPS

<!-- TODO: Publish to Maven Central -->
<!-- TODO: Steps needed to run the demo with toy data -->
<!-- TODO: Example in Notebook (Zeppelin?) -->
<!-- TODO: WIKI article that demonstrates a more realistic use case with HDFS data source -->
<!-- TODO: Post link to Cyper Chapter in Neo4j Developer Manual-->

## How to contribute

We'd love to find out about any issues you encounter. We welcome code contributions -- please open an [issue](https://github.com/neo-technology/cypher-for-apache-spark/issues) first to ensure there is no duplication of effort. <!-- TODO: Determine CLA and process -->

## License

The project is licensed under the Apache Software License 2.0.

## Copyright

© Copyright 2016-2017 Neo4j, Inc.

Apache Spark™, Spark, and Apache are registered trademarks of the [Apache Software Foundation](https://www.apache.org/).
