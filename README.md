# Cypher for Apache Spark

This project is an extension to [Apache Spark™](https://spark.apache.org), adding property graph querying support via the industry's most widely used property graph query language, Cypher.

## Current status

The project is in an alpha stage, which means that no guarantees are given as to the stability of APIs, semantics, correctness of results, or functionality.
The project is in rapid development, and will be published as a [Spark package](https://spark-packages.org).

## Roadmap

The overall goals for this project is to support the following high-level features:

- Cypher 3.2
- Multiple graphs Cypher
- Query compositionality
- Integration with existing Spark libraries via the DataFrame API
- Pluggable data source interface

Given the immutable nature of Spark DataFrames, support for Cypher will initially focus on the read-only parts of the language.

## Copyright

© Copyright 2016-2017 Neo4j, Inc.

Apache Spark™, Spark, and Apache are registered trademarks of the [Apache Software Foundation](https://www.apache.org/).

## License

The project is licensed under the Apache Software License, 2.0.
