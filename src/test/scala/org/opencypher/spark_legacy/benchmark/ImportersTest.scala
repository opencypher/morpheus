package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark.TestSuiteImpl

class ImportersTest extends TestSuiteImpl {

  ignore("importing labels") {
    Importers.importMusicBrainz(10)
  }

}
