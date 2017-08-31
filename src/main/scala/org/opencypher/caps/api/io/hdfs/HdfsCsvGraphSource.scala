package org.opencypher.caps.api.io.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class HdfsCsvGraphSource(hadoopConfig: Configuration, path: String)
  extends GraphSource {

  override def handles(uri: URI): Boolean = {
    val hadoopURIString = Option(hadoopConfig.get("fs.defaultFS"))
      .getOrElse(Option(hadoopConfig.get("fs.default.name"))
      .getOrElse(Raise.invalidConnection("Neither fs.defaultFS nor fs.default.name found"))
    )

    val hadoopURI = URI.create(hadoopURIString)

    uri.getScheme == "hdfs+csv" && hadoopURI.getHost == uri.getHost && hadoopURI.getPort == uri.getPort
  }

  override def get(implicit capsSession: CAPSSession): CAPSGraph =
    new CsvGraphLoader(path, hadoopConfig).load
}

case class HdfsCsvGraphSourceFactory(hadoopConfiguration: Configuration)
  extends GraphSourceFactory {

  override val protocol = "hdfs+csv"

  override def fromURI(uri: URI): GraphSource = {
    val host = uri.getHost
    val port = if (uri.getPort == -1) "" else s":${uri.getPort}"
    val defaultName = s"hdfs://$host$port"

    val hadoopConf = new Configuration(hadoopConfiguration)
    hadoopConf.set("fs.default.name", defaultName)
    HdfsCsvGraphSource(hadoopConf, uri.getPath)
  }
}
