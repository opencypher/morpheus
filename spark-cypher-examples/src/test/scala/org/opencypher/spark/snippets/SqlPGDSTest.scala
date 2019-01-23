package org.opencypher.spark.snippets

import org.opencypher.spark.examples.ExampleTest


class SqlPGDSTest extends ExampleTest  {

  it("should produce the correct output") {
    validate(
      SqlPGDS.main(Array.empty),
      emptyOutput
    )
  }
}
