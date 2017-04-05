package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark.prototype.api.types.{CTList, CTRelationship}
import org.opencypher.spark.prototype.api.value.{CypherList, CypherRelationship}
import org.opencypher.spark.impl._

import scala.collection.mutable

object VarExpand extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(node: Symbol, lowerBound: Int, upperBound: Int)(relationships: Symbol)(implicit context: PlanningContext): StdCypherFrame[Product] = {
    val frames = new FrameProducer
    import frames._

    // Init relationships with their start id -- constant
    val rStart = allRelationships('RNEXT).asProduct.relationshipStartId('RNEXT)('START).asRow
    val startIdSlot = obtain(rStart.signature.slot)('START).sym

    val stepMap = new mutable.HashMap[Int, StdCypherFrame[Product]]()

    val firstRelationship = 'R1
    if (upperBound > 0) {
      // Initial input for stepping: all relationships
      stepMap(1) = allRelationships(firstRelationship).asProduct
    }

    (2 to upperBound).foreach { i =>
      val prevStep = stepMap(i - 1)
      val prevWithEndId = prevStep.relationshipEndId(Symbol(s"R${i - 1}"))(Symbol(s"END${i - 1}"))
      val endIdSlot = obtain(prevWithEndId.signature.slot)(prevWithEndId.projectedField.sym).sym
      val (_, stepSig) = prevWithEndId.signature.addField(Symbol(s"R$i") -> CTRelationship)(context)
      stepMap(i) = Step(prevWithEndId.asRow, endIdSlot)(rStart, startIdSlot)(stepSig).asProduct
    }

    // Combine phase
    val combines = (Math.max(lowerBound, 1) to upperBound).map { i =>
      val stepI = stepMap(i)
      val rFields = (1 to i).map { j => Symbol(s"R$j") }
      val rSlots = rFields.map { f => obtain(stepI.signature.slot)(f) }.map(_.ordinal)
      val (_ , sig) = stepI.signature.addField(relationships -> CTList(CTRelationship))(context)
      val combine = Combine(stepI, rSlots)(sig)
      val filter = FilterDuplicates(combine, rSlots)
      // Selection phase
      // Only keep relationship list column, and start id of first relationship (for joining)
      filter.selectFields(relationships, firstRelationship)
    }

    // Union phase
    // Union combinations into one table
    val union = combines.reduce[StdCypherFrame[Product]] {
      case (acc, next) => acc.unionAll(next)
    }

    // Join phase
    // Join with input start nodes

    val startNodeWithId = input.nodeId(node)('nodeSTART)
    val allRelsWithFirstStartId = union.relationshipStartId(firstRelationship)('FIRST).asRow

    val nonEmptyResults = startNodeWithId.asRow.join(allRelsWithFirstStartId).on('nodeSTART)('FIRST).asProduct.selectFields(node, relationships)

    val plan = if (lowerBound == 0) {
      // compute empty lists for all input nodes
      val (_, emptySig) = startNodeWithId.signature.addField(relationships -> CTList(CTRelationship))(context)
      val lists = Combine(startNodeWithId, IndexedSeq.empty)(emptySig).selectFields(node, relationships)
      nonEmptyResults.unionAll(lists)
    } else
      nonEmptyResults

    plan
  }

  private final case class FilterDuplicates(input: StdCypherFrame[Product], indices: IndexedSeq[Int])
    extends ProductFrame(input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      in.filter(areUnique(indices))
    }
  }

  private final case class areUnique(indices: IndexedSeq[Int]) extends (Product => Boolean) {
    import org.opencypher.spark.impl.util._

    override def apply(record: Product): Boolean = {
      val items = indices.map(record.getAs[CypherRelationship](_))
      items.distinct.size == indices.size
    }
  }

  private final case class Combine(input: StdCypherFrame[Product], indices: IndexedSeq[Int])(sig: StdFrameSignature) extends ProductFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      in.map(combineToList(indices))(context.productEncoder(slots))
    }
  }

  private final case class combineToList(indices: IndexedSeq[Int]) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    override def apply(record: Product): Product = {
      val items = indices.map(record.getAs[CypherRelationship](_))
      record :+ CypherList(items)
    }
  }

  private final case class Step(prev: StdCypherFrame[Row], prevEndId: Symbol)
                               (next: StdCypherFrame[Row], nextStartId: Symbol)
                               (sig: StdFrameSignature) extends RowFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhs = prev.run
      val rhs = next.run

      val joinExpr = functions.expr(s"${prevEndId.name} = ${nextStartId.name}")

      val joined = lhs.join(rhs, joinExpr, "inner")
      val out = joined.drop(nextStartId.name)

      out
    }
  }
}
