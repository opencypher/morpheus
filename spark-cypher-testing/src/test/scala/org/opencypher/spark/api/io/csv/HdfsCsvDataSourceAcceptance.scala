package org.opencypher.spark.api.io.csv

import org.opencypher.spark.api.io.fs.hdfs.HdfsDataSourceAcceptance
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource

class HdfsCsvDataSourceAcceptance extends HdfsDataSourceAcceptance {

  override protected def createDs(graph: CAPSGraph): CAPSPropertyGraphDataSource = {
    CsvDataSource("hdfs:///")
  }

}
