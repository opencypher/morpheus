/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.opencypher.spark.examples

class JdbcSqlGraphSourceExampleTest extends ExampleTest {
  it("runs JdbcSqlGraphSourceExample") {
    validate(
      JdbcSqlGraphSourceExample.main(Array.empty),
      getClass.getResource("/example_outputs/JdbcSqlGraphSourceExample.out").toURI
    )
  }
}
