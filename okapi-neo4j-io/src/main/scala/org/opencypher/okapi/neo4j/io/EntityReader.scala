package org.opencypher.okapi.neo4j.io

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._

object EntityReader {

  def flatExactLabelQuery(labels: Set[String], schema: Schema, maybeMetaLabel: Option[String] = None): String ={
    val props = schema.nodeKeys(labels).propertyExtractorString
    val allLabels = labels ++ maybeMetaLabel
    val labelCount = allLabels.size

    s"""|MATCH ($entityVarName:${allLabels.mkString(":")})
        |WHERE LENGTH(LABELS($entityVarName)) = $labelCount
        |RETURN id($entityVarName) AS $idPropertyKey$props""".stripMargin
  }

  def flatRelTypeQuery(relType: String, schema: Schema, maybeMetaLabel: Option[String] = None): String ={
    val props = schema.relationshipKeys(relType).propertyExtractorString
    val metaLabel = maybeMetaLabel.map(m => s":$m").getOrElse("")

    s"""|MATCH (s$metaLabel)-[$entityVarName:$relType]->(t$metaLabel)
        |RETURN id($entityVarName) AS $idPropertyKey, id(s) AS $startIdPropertyKey, id(t) AS $endIdPropertyKey$props""".stripMargin
  }

  implicit class RichPropertyTypes(val properties: PropertyKeys) extends AnyVal {
    def propertyExtractorString: String = {
      val propertyStrings = properties
        .keys
        .toList
        .sorted
        .map(k => s"$entityVarName.$k")

      if (propertyStrings.isEmpty) ""
      else propertyStrings.mkString(", ", ", ", "")
    }
  }
}
