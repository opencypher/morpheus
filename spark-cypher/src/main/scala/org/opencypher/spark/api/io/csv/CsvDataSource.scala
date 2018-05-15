package org.opencypher.spark.api.io.csv

import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.FileBasedDataSource
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource

object CsvDataSource {

  def apply(rootPath: String)(implicit session: CAPSSession): CAPSPropertyGraphDataSource = {
    new FileBasedDataSource(rootPath, "csv", Some(10))
  }

}
