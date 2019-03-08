package org.opencypher.graphddl

import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.impl.util.ScalaUtils._

object GraphDdlOps {

  implicit class SchemaOps(schema: Schema) {
    def asGraphType: GraphType = {
      ???
    }
  }

  implicit class GraphTypeOps(graphType: GraphType) {

    lazy val allPatterns: Set[SchemaPattern] =
      graphType.relTypes.keySet.map(edgeType => SchemaPattern(
        sourceLabelCombination = edgeType.startNodeType.elementTypes,
        // TODO: validate there is only one rel type
        relType = edgeType.elementTypes.head,
        targetLabelCombination = edgeType.endNodeType.elementTypes
      ))

    // TODO move out of Graph DDL (maybe to CAPSSchema)
    def asOkapiSchema: Schema = Schema.empty
      .foldLeftOver(graphType.nodeTypes) { case (schema, (nodeTypeDefinition, properties)) =>
        schema.withNodePropertyKeys(nodeTypeDefinition.elementTypes, properties)
      }
      .foldLeftOver(graphType.nodeElementTypes) { case (schema, eType) =>
        eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
      }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relTypes) { case (schema, (relTypeDefinition, properties)) =>
      schema.withRelationshipPropertyKeys(relTypeDefinition.elementTypes.head, properties)
    }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relElementTypes) { case (schema, eType) =>
      eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
    }
      .withSchemaPatterns(allPatterns.toSeq: _*)
  }
}
