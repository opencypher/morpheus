package org.opencypher.caps.api.spark
import org.apache.spark.sql.{Dataset, Row}
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

    val nodeHeader: RecordHeader = RecordHeader.nodeFromSchema(node, nodeSchema, tokens.registry)


    val slots = baseTable.header.slots.collect {
      case slot@RecordSlot(_, OpaqueField(v)) if v.cypherType == nodeCypherType => slot -> v
    }

    val slotsWithChildren: IndexedSeq[Seq[RecordSlot]] = slots.map(slot => baseTable.header.childSlots(slot._2) :+ slot._1)

    val columnNamesPerNode: IndexedSeq[Seq[String]] =
      slotsWithChildren.map(nodeSlots => nodeSlots.map(SparkColumnName.of))

    val nodeDf: Dataset[Nothing] = baseTable.details.toDF().flatMap {row => {
      val columnIndicesPerNode: IndexedSeq[Seq[Int]] =
        columnNamesPerNode.map(nodeColumnNames => nodeColumnNames.map(row.fieldIndex))

      val nodeRows: IndexedSeq[Row] = slotsWithChildren.map(slotsForNode => {
        val unorderedNodeSequence: Seq[Any] = slotsForNode.map(row.get)

        val orderedNodeSequence: Seq[Any] = nodeHeader.slots.foldLeft(Seq.empty){case (headersSoFar, nextSlot) =>
          val maybeSlot = slotsForNode.find(nextSlot)
            maybeSlot match {
              case
            }
        }
        }
        val newRow = Row.fromSeq(orderedSequence)
        ???
      })



      nodeRows
    }

    ???
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
