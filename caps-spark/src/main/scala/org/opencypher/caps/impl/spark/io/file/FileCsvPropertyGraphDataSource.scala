package org.opencypher.caps.impl.spark.io.file

import java.io.File

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.GraphName
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.spark.io.CAPSPropertyGraphDataSource
import org.opencypher.caps.impl.spark.io.hdfs.CsvGraphLoader

class FileCsvPropertyGraphDataSource(rootPath: String)(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph =
    CsvGraphLoader(s"$rootPath${File.separator}$name").load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
