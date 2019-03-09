package org.opencypher.graphddl

import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.impl.util.ScalaUtils._

// TODO: move close to SQL PGDS
object GraphDdlOps {

  implicit class SchemaOps(schema: Schema) {
    def asGraphType: GraphType = {
      ???
    }
  }

  implicit class GraphTypeOps(graphType: GraphType) {

    lazy val allPatterns: Set[SchemaPattern] =
      graphType.relTypes.map(edgeType => SchemaPattern(
        sourceLabelCombination = edgeType.startNodeType.labels,
        // TODO: validate there is only one rel type
        relType = edgeType.labels.head,
        targetLabelCombination = edgeType.endNodeType.labels
      ))

    // TODO move out of Graph DDL (maybe to CAPSSchema)
    def asOkapiSchema: Schema = Schema.empty
      .foldLeftOver(graphType.nodeTypes) { case (schema, nodeType) =>
        schema.withNodePropertyKeys(nodeType.labels, graphType.nodePropertyKeys(nodeType))
      }
      .foldLeftOver(graphType.nodeElementTypes) { case (schema, eType) =>
        eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
      }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relTypes) { case (schema, relType) =>
      schema.withRelationshipPropertyKeys(relType.labels.head, graphType.relationshipPropertyKeys(relType))
    }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relElementTypes) { case (schema, eType) =>
      eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
    }
      .withSchemaPatterns(allPatterns.toSeq: _*)
  }
}
