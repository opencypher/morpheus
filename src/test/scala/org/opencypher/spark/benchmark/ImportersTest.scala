package org.opencypher.spark.benchmark

import org.opencypher.spark.StdTestSuite

class ImportersTest extends StdTestSuite {

  test("importing labels") {
    Importers.importMusicBrainz(10)
  }

}
