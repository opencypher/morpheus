package org.opencypher.caps.api.spark
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.{PropertyKeyMap, Schema}
import org.opencypher.caps.api.spark.CAPSGraph.ScanGraph
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api.global.TokenRegistry
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.opencypher.caps.impl.spark.convert.toSparkType


class PatternGraph(private val baseTable: CAPSRecords, val schema: Schema, val tokens: CAPSRecordsTokens)
                  (implicit val session: CAPSSession) extends CAPSGraph {

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)

    val explicitLabels = nodeCypherType.labels.filter(_._2).keySet

    // TODO: Fix that no labels means all labels
    val nodeLabels: Set[String] = if (nodeCypherType.labels.isEmpty) {
      val allLabels = schema.labels
      allLabels
    } else {
      val possibleLabels = schema.labelCombinations.filterByLabels(explicitLabels)
      possibleLabels.combos.flatten
    }

    val nodeSchema = schema.copy(
      labels = nodeLabels,
      Set.empty,
      nodeKeyMap = schema.nodeKeyMap.filterByLabels(nodeLabels),
      relKeyMap = PropertyKeyMap.empty,
      impliedLabels = schema.impliedLabels.filterByLabels(nodeLabels),
      labelCombinations = schema.labelCombinations.filterByLabels(nodeLabels)
    )

    val nodeHeader: RecordHeader = RecordHeader.nodeFromSchema(node, nodeSchema, tokens.registry)

    val sourceHeader = baseTable.details.header

    val candidateNodes = sourceHeader.slots.collect {
      case RecordSlot(_, OpaqueField(v)) => v
    }.filter { v =>
      def isNode = v.cypherType.subTypeOf(CTNode).isTrue
      def hasAllRequiredLabels = {
        val labels = sourceHeader.labels(v).map(_.label.name).toSet
        explicitLabels.subsetOf(labels)
      }
      isNode && hasAllRequiredLabels
    }

    val slotsWithChildren: Map[Var, Seq[RecordSlot]] = candidateNodes.map { candidate =>
      candidate -> (sourceHeader.childSlots(candidate) :+ sourceHeader.slotFor(candidate))
    }.toMap

    val nodeColumnsLookupTables = slotsWithChildren.map {
      case (nodeVar, slotsForNode) =>
        nodeVar -> scanTableToBaseTableNameLookup(node, slotsForNode.map(_.content))
    }

    implicit val rowEncoder = rowEncoderFor(nodeHeader)

    val nodeDf = baseTable.details.toDF().flatMap(
      RowNodeExpansion(nodeHeader, node, slotsWithChildren, nodeColumnsLookupTables))

    CAPSRecords.create(nodeHeader, nodeDf)
  }


  def rowEncoderFor(nodeHeader: RecordHeader): ExpressionEncoder[Row] = {
    val schema = StructType(nodeHeader.slots.map(_.structField))
    RowEncoder(schema)
  }

  def scanTableToBaseTableNameLookup(scanTableVar: Var, slotContents: Seq[SlotContent]): Map[String,String] = {
    slotContents.map { baseTableSlotContent =>
      SparkColumnName.of(baseTableSlotContent.withOwner(scanTableVar)) -> SparkColumnName.of(baseTableSlotContent)
    }.toMap
  }



//    override def relationships(name: String, relCypherType: CTRelationship) = {
//      // (1) find all scans smaller than or equal to the given cypher type if any
//      val selectedScans = relEntityScans.scans(relCypherType)
//
//      // (2) rename scans consistently
//      val rel = Var(name)(relCypherType)
//      val tempSchema = selectedScans.map(_.schema).reduce(_ ++ _)
//      val selectedRecords = alignEntityVariable(selectedScans, rel)
//      val tempHeader = RecordHeader.relationshipFromSchema(rel, tempSchema, tokens.registry)
//
//      // (3) Update all non-nullable property types to nullable
//      val targetSchema = Schema(tempSchema.labels,
//        tempSchema.relationshipTypes,
//        tempSchema.nodeKeyMap,
//        PropertyKeyMap.asNullable(tempSchema.relKeyMap),
//        tempSchema.impliedLabels,
//        tempSchema.labelCombinations)
//      val targetHeader = RecordHeader.relationshipFromSchema(rel, targetSchema, tokens.registry)
//
//      // (4) Adjust individual scans to same header
//      val alignedRecords = alignRecords(selectedRecords, tempHeader, targetHeader)
//
//      // (5) Union all scan records based on final schema
//      val data = alignedRecords.map(_.details.toDF()).reduce(_ union _)
//      CAPSRecords.create(targetHeader, data)
//    }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = ???

  override def union(other: CAPSGraph): CAPSGraph = ???

  override protected def graph: CAPSGraph = this
}
