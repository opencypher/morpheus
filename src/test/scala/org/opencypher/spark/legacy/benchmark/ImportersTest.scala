package org.opencypher.spark.legacy.benchmark

import org.opencypher.spark.StdTestSuite

class ImportersTest extends StdTestSuite {

  test("importing labels") {
    Importers.importMusicBrainz(10)
  }

}
