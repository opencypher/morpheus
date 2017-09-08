package org.opencypher.caps.api.io.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.io.{CreateOrFail, GraphSource, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class HdfsCsvGraphSource(override val canonicalURI: URI, hadoopConfig: Configuration, path: String)
  extends GraphSource {

  import org.opencypher.caps.api.io.hdfs.HdfsCsvGraphSourceFactory.supportedSchemes

  override def sourceForGraphAt(uri: URI): Boolean = {
    val hadoopURIString = Option(hadoopConfig.get("fs.defaultFS"))
      .getOrElse(Option(hadoopConfig.get("fs.default.name"))
      .getOrElse(Raise.invalidConnection("Neither fs.defaultFS nor fs.default.name found"))
    )
    val hadoopURI = URI.create(hadoopURIString)
    supportedSchemes.contains(uri.getScheme) && hadoopURI.getHost == uri.getHost && hadoopURI.getPort == uri.getPort
  }

  override def graph(implicit capsSession: CAPSSession): CAPSGraph =
    new CsvGraphLoader(path, hadoopConfig).load

  // TODO: Make better/cache?
  override def schema(implicit capsSession: CAPSSession): Option[Schema] = None

  override def create(implicit capsSession: CAPSSession): CAPSGraph =
    persist(CreateOrFail, CAPSGraph.empty)

  override def persist(mode: PersistMode, graph: CAPSGraph)(implicit capsSession: CAPSSession): CAPSGraph =
    ???

  override def delete(implicit capsSession: CAPSSession): Unit =
    ???
}
