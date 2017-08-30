package org.opencypher.caps.api.io.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.CAPSGraph

case class HdfsCsvGraphSource(hadoopConfig: Configuration, path: String) extends GraphSource {

  override def get: CAPSGraph = new CsvGraphLoader(path, hadoopConfig).load
}

class HdfsCsvGraphSourceFactory extends GraphSourceFactory {
  override val protocol = "hdfs+csv"

  // TODO: update "fs.default.name" in sparkContext.hadoopConfiguration
  override def fromURI(uri: URI) = ???
}
