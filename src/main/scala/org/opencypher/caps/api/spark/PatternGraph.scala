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

  private val header = baseTable.details.header

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val targetNode = Var(name)(nodeCypherType)
    val nodeSchema = schema.forNode(nodeCypherType)
    val targetNodeHeader = RecordHeader.nodeFromSchema(targetNode, nodeSchema, tokens.registry)
    val extractionNodes: Seq[Var] = header.nodesForType(nodeCypherType)

    extractRecordsFor(targetNode, targetNodeHeader, extractionNodes)
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val targetRel = Var(name)(relCypherType)
    val targetRelHeader = RecordHeader.relationshipFromSchema(targetRel, schema.forRelationship(relCypherType), tokens.registry)
    val extractionRels = header.relationshipsForType(relCypherType)

    extractRecordsFor(targetRel, targetRelHeader, extractionRels)
  }

  private def extractRecordsFor(targetVar: Var, targetHeader: RecordHeader, extractionVars: Seq[Var]): CAPSRecords = {
    val extractionSlots = extractionVars.map { candidate =>
      candidate -> (header.childSlots(candidate) :+ header.slotFor(candidate))
    }.toMap

    val relColumnsLookupTables = extractionSlots.map {
      case (relVar, slotsForRel) =>
        relVar -> createScanToBaseTableLookup(targetVar, slotsForRel.map(_.content))
    }

    val relDf = baseTable.details.toDF().flatMap(
      RowExpansion(targetHeader, targetVar, extractionSlots, relColumnsLookupTables))(rowEncoderFor(targetHeader))

    CAPSRecords.create(targetHeader, relDf)
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
