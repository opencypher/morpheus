package org.opencypher.spark.schema

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.{CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.impl.schema.SchemaUtils._
import org.opencypher.okapi.impl.schema.{ImpliedLabels, LabelCombinations}

object CAPSSchema {

  implicit class VerifiedSchema(schema: Schema) extends Schema {
    verify()

    // Verification makes sure that we will always know the exact type of a property when given at least one label
    // Another, more restrictive verification would be to guarantee that even without a label
    def verify(): Unit = {
      val combosByLabel = schema.foldAndProduce(Map.empty[String, Set[Set[String]]])(
        (set, combos, _) => set + combos,
        (combos, _) => Set(combos))

      combosByLabel.foreach {
        case (label, combos) =>
          val keysForAllCombosOfLabel = combos.map(schema.nodeKeys)
          for {
            keys1 <- keysForAllCombosOfLabel
            keys2 <- keysForAllCombosOfLabel
            if keys1 != keys2
          } yield {
            (keys1.keySet intersect keys2.keySet).foreach { k =>
              val t1 = keys1(k)
              val t2 = keys2(k)
              val join = t1.join(t2)
              if (join.material != t1.material && join.material != t2.material) {
                throw SchemaException(
                  s"The property types $t1 and $t2 (for property '$k' and label $label) can not be stored in the same Spark column"
                )
              }
            }
          }
      }
    }

    override def labels: Set[String] = schema.labels

    override def relationshipTypes: Set[String] = schema.relationshipTypes

    override def labelPropertyMap: LabelPropertyMap = schema.labelPropertyMap

    override def relTypePropertyMap: RelTypePropertyMap = schema.relTypePropertyMap

    override def impliedLabels: ImpliedLabels = schema.impliedLabels

    override def labelCombinations: LabelCombinations = schema.labelCombinations

    override def impliedLabels(knownLabels: Set[String]): Set[String] = schema.impliedLabels(knownLabels)

    override def nodeKeys(labels: Set[String]): PropertyKeys = schema.nodeKeys(labels)

    override def allNodeKeys: PropertyKeys = schema.allNodeKeys

    override def allLabelCombinations: Set[Set[String]] = schema.allLabelCombinations

    override def combinationsFor(knownLabels: Set[String]): Set[Set[String]] = schema.combinationsFor(knownLabels)

    override def nodeKeyType(labels: Set[String], key: String): Option[CypherType] = schema.nodeKeyType(labels, key)

    override def keysFor(labelCombinations: Set[Set[String]]): PropertyKeys = schema.keysFor(labelCombinations)

    override def relationshipKeyType(types: Set[String], key: String): Option[CypherType] = schema.relationshipKeyType(types, key)

    override def relationshipKeys(typ: String): PropertyKeys = schema.relationshipKeys(typ)

    override def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys): Schema = schema.withNodePropertyKeys(nodeLabels, keys)

    override def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema = schema.withRelationshipPropertyKeys(typ, keys)

    override def ++(other: Schema): Schema = schema ++ other

    override def fromNodeEntity(labels: Set[String]): Schema = schema.fromNodeEntity(labels)

    override def pretty: String = schema.pretty

    override def isEmpty: Boolean = schema.isEmpty

    override def forNodeScan(labelConstraints: Set[String]): Schema = schema.forNodeScan(labelConstraints)

    override def forRelationship(relType: CTRelationship): Schema = schema.forRelationship(relType)
  }

}
