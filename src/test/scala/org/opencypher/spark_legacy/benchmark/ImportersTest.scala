package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark.BaseTestSuite

class ImportersTest extends BaseTestSuite {

  ignore("importing labels") {
    Importers.importMusicBrainz(10)
  }

}
