package org.opencypher.spark.api.io.csv

import org.apache.hadoop.fs.Path
import org.opencypher.spark.api.io.fs.hdfs.HdfsDataSourceAcceptance
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource

class HdfsCsvDataSourceAcceptance extends HdfsDataSourceAcceptance {

  override protected def createDs(graph: CAPSGraph): CAPSPropertyGraphDataSource = {
    HdfsCsvDataSource(new Path("/"), cluster.getFileSystem())
  }

}
