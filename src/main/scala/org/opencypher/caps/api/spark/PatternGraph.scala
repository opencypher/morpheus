package org.opencypher.caps.api.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.{PropertyKeyMap, Schema}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName


class PatternGraph(private val baseTable: CAPSRecords, val schema: Schema, val tokens: CAPSRecordsTokens)
                  (implicit val session: CAPSSession) extends CAPSGraph {

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val sourceHeader = baseTable.details.header

    val targetNode = Var(name)(nodeCypherType)
    val targetNodeSchema = schema.forNode(targetNode)
    val targetNodeHeader = RecordHeader.nodeFromSchema(targetNode, targetNodeSchema, tokens.registry)

    val extractionNodes = sourceHeader.nodesForType(nodeCypherType)
    val extractionSlots = extractionNodes.map { candidate =>
      candidate -> (sourceHeader.childSlots(candidate) :+ sourceHeader.slotFor(candidate))
    }.toMap

    val nodeColumnsLookupTables = extractionSlots.map {
      case (nodeVar, slotsForNode) =>
        nodeVar -> createScanToBaseTableLookup(targetNode, slotsForNode.map(_.content))
    }

    implicit val rowEncoder = rowEncoderFor(targetNodeHeader)

    val nodeDf = baseTable.details.toDF().flatMap(
      RowNodeExpansion(targetNodeHeader, targetNode, extractionSlots, nodeColumnsLookupTables))

    CAPSRecords.create(targetNodeHeader, nodeDf)
  }


  def rowEncoderFor(nodeHeader: RecordHeader): ExpressionEncoder[Row] = {
    val schema = StructType(nodeHeader.slots.map(_.structField))
    RowEncoder(schema)
  }

  private def createScanToBaseTableLookup(scanTableVar: Var, slotContents: Seq[SlotContent]): Map[String,String] = {
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
