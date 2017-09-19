package org.opencypher.caps.api.spark
import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.{PropertyKeyMap, Schema}
import org.opencypher.caps.api.spark.CAPSGraph.ScanGraph
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.ir.api.global.TokenRegistry

class PatternGraph(private val baseTable: CAPSRecords, val schema: Schema, val tokens: CAPSRecordsTokens)
                  (implicit val session: CAPSSession) extends CAPSGraph {

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)

    val nodeLabels: Set[String] = nodeCypherType.labels.filter(_._2).keySet

    val nodeSchema = schema.copy(
      labels = nodeLabels,
      Set.empty,
      nodeKeyMap = schema.nodeKeyMap.filter({case (label, map)  => nodeLabels.contains(label)}),
      relKeyMap = PropertyKeyMap.empty,
      impliedLabels = schema.impliedLabels.filterByLabels(nodeLabels),
      labelCombinations = schema.labelCombinations.filterByLabels(nodeLabels)
    )

    val nodeHeader = RecordHeader.nodeFromSchema(node, nodeSchema, tokens.registry)


    val slots = baseTable.header.slots.collect {
      case slot@RecordSlot(_, OpaqueField(v)) if v.cypherType == nodeCypherType => slot -> v
    }

    val slotsWithChildren = slots.map(slot => baseTable.header.childSlots(slot._2) :+ slot._1)

    val columnNamesPerNode: IndexedSeq[Seq[String]] =
      slotsWithChildren.map(nodeSlots => nodeSlots.map(SparkColumnName.of))

    val nodeDf = baseTable.details.toDF().flatMap {row => {
      val columnIndicesPerNode: IndexedSeq[Seq[Int]] =
        columnNamesPerNode.map(nodeColumnNames => nodeColumnNames.map(row.fieldIndex))

      val nodeRows = columnIndicesPerNode.map(nodeIndices => {
        nodeIndices.map(row.get)
        Row.fromSeq(nodeIndices.map(row.get))
      })

      nodeRows
    }

    ???
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = ???

  override def union(other: CAPSGraph): CAPSGraph = ???

  override protected def graph: CAPSGraph = this
}
