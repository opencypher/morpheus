package org.opencypher.caps.impl.spark.io.hdfs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{GraphName, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema

/**
  * Data source for loading graphs from HDFS.
  *
  * @param hadoopConfig Hadoop configuration
  * @param rootPath     root path containing one ore more graphs
  */
class HdfsCsvPropertyGraphDataSource(
  hadoopConfig: Configuration,
  rootPath: String)(implicit val session: CAPSSession) extends PropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph =
    CsvGraphLoader(s"$rootPath${File.separator}$name", hadoopConfig).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
