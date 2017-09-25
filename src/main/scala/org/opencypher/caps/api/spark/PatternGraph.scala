package org.opencypher.caps.api.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName

class PatternGraph(private val baseTable: CAPSRecords, val schema: Schema, val tokens: CAPSRecordsTokens)
                  (implicit val session: CAPSSession) extends CAPSGraph {

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val sourceHeader = baseTable.details.header

    val targetNode = Var(name)(nodeCypherType)
    val targetNodeSchema = schema.forNode(nodeCypherType)
    val targetNodeHeader = RecordHeader.nodeFromSchema(targetNode, targetNodeSchema, tokens.registry)

    val extractionNodes = sourceHeader.nodesForType(nodeCypherType)
    val extractionSlots = extractionNodes.map { candidate =>
      candidate -> (sourceHeader.childSlots(candidate) :+ sourceHeader.slotFor(candidate))
    }.toMap

    val nodeColumnsLookupTables = extractionSlots.map {
      case (nodeVar, slotsForNode) =>
        nodeVar -> createScanToBaseTableLookup(targetNode, slotsForNode.map(_.content))
    }

    val nodeDf = baseTable.details.toDF().flatMap(
      RowExpansion(targetNodeHeader, targetNode, extractionSlots, nodeColumnsLookupTables))(rowEncoderFor(targetNodeHeader))

    CAPSRecords.create(targetNodeHeader, nodeDf)
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val sourceHeader = baseTable.details.header

    val targetRel = Var(name)(relCypherType)
    val targetRelSchema = schema.forRelationship(relCypherType)
    val targetRelHeader = RecordHeader.relationshipFromSchema(targetRel, targetRelSchema, tokens.registry)

    val extractionRels: Seq[Var] = sourceHeader.relationshipsForType(relCypherType)
    val extractionSlots = extractionRels.map { candidate =>
      candidate -> (sourceHeader.childSlots(candidate) :+ sourceHeader.slotFor(candidate))
    }.toMap

    val relColumnsLookupTables = extractionSlots.map {
      case (nodeVar, slotsForRel) =>
        nodeVar -> createScanToBaseTableLookup(targetRel, slotsForRel.map(_.content))
    }

    val relDf = baseTable.details.toDF().flatMap(
      RowExpansion(targetRelHeader, targetRel, extractionSlots, relColumnsLookupTables))(rowEncoderFor(targetRelHeader))

    CAPSRecords.create(targetRelHeader, relDf)
  }

  private def rowEncoderFor(nodeHeader: RecordHeader): ExpressionEncoder[Row] = {
    val schema = StructType(nodeHeader.slots.map(_.structField))
    RowEncoder(schema)
  }

  private def createScanToBaseTableLookup(scanTableVar: Var, slotContents: Seq[SlotContent]): Map[String,String] = {
    slotContents.map { baseTableSlotContent =>
      SparkColumnName.of(baseTableSlotContent.withOwner(scanTableVar)) -> SparkColumnName.of(baseTableSlotContent)
    }.toMap
  }

  override def union(other: CAPSGraph): CAPSGraph = ???

  override protected def graph: CAPSGraph = this
}
