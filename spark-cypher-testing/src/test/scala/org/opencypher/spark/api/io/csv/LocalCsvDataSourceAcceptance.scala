package org.opencypher.spark.api.io.csv

import java.nio.file.Paths

import org.opencypher.spark.api.io.fs.local.LocalDataSourceAcceptance
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource

class LocalCsvDataSourceAcceptance extends LocalDataSourceAcceptance {

  override protected def createDs(graph: CAPSGraph): CAPSPropertyGraphDataSource = {
    CsvDataSource("file://" + Paths.get(tempDir.getRoot.getAbsolutePath))
  }

}
