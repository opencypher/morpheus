package org.opencypher.morpheus.adapters

import org.apache.spark.graph.api.{NodeDataset, RelationshipDataset}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}

object MappingAdapter {

  implicit class RichNodeDataFrame(val nodeDf: NodeDataset) extends AnyVal {
    def toNodeMapping: ElementMapping = NodeMappingBuilder
      .on(nodeDf.idColumn)
      .withImpliedLabels(nodeDf.labelSet.toSeq: _*)
      .withPropertyKeyMappings(nodeDf.propertyColumns.toSeq:_*)
      .build
  }

  implicit class RichRelationshipDataFrame(val relDf: RelationshipDataset) extends AnyVal {
    def toRelationshipMapping: ElementMapping = RelationshipMappingBuilder
      .on(relDf.idColumn)
      .withSourceStartNodeKey(relDf.sourceIdColumn)
      .withSourceEndNodeKey(relDf.targetIdColumn)
      .withRelType(relDf.relationshipType)
      .withPropertyKeyMappings(relDf.propertyColumns.toSeq: _*)
      .build
  }
}
