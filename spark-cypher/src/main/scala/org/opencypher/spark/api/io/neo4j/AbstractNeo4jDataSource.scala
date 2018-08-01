package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.neo4j.io.{Neo4jConfig, SchemaFromProcedure}
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._


abstract class AbstractNeo4jDataSource extends AbstractPropertyGraphDataSource {

  def config: Neo4jConfig

  def omitIncompatibleProperties = false

  override def tableStorageFormat: String = "neo4j"

  override protected def readSchema(graphName: GraphName): CAPSSchema =
    SchemaFromProcedure(config, omitIncompatibleProperties) match {
      case None =>
        // TODO: add link to procedure installation
        throw UnsupportedOperationException("Neo4j PGDS requires okapi-neo4j-procedures to be installed in Neo4j")

      case Some(schema) =>
        schema.asCaps
  }

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = ()
  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = CAPSGraphMetaData(tableStorageFormat)
  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = ()
}
