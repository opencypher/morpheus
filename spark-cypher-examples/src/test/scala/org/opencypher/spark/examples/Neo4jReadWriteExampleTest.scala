package org.opencypher.spark.examples

class Neo4jReadWriteExampleTest extends ExampleTest {

  it("should produce the correct output") {
    validate(Neo4jReadWriteExample.main(Array.empty),
      getClass.getResource("/example_outputs/Neo4jReadWriteExample.out").toURI)
  }

}
