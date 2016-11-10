package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark.api.types.{CTList, CTRelationship}
import org.opencypher.spark.api.value.{CypherList, CypherRelationship}
import org.opencypher.spark.impl._

import scala.collection.mutable

object VarExpand extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(node: Symbol, lowerBound: Int, upperBound: Int)(relationships: Symbol)(implicit context: PlanningContext): StdCypherFrame[Product] = {
    val frames = new FrameProducer
    import frames._

    // Init relationships with their start id -- constant
    val rStart = allRelationships('RNEXT).asProduct.relationshipStartId('RNEXT)('START).asRow
    val startIdSlot = obtain(rStart.signature.slot)('START).sym

    // Initial input for stepping: all relationships

    val stepMap = new mutable.HashMap[Int, StdCypherFrame[Product]]()
    stepMap(0) = allRelationships('R0).asProduct

    (1 until upperBound).foreach { i =>
      val prevStep = stepMap(i - 1)
      val prevWithEndId = prevStep.relationshipEndId(Symbol(s"R${i - 1}"))(Symbol(s"END${i - 1}"))
      val endIdSlot = obtain(prevWithEndId.signature.slot)(prevWithEndId.projectedField.sym).sym
      val (_, stepSig) = prevWithEndId.signature.addField(Symbol(s"R$i") -> CTRelationship)(context)
      stepMap(i) = Step(prevWithEndId.asRow, endIdSlot)(rStart, startIdSlot)(stepSig).asProduct
    }

    // Combine phase
    val combines = (lowerBound - 1 until upperBound).map { i =>
      val stepI = stepMap(i)
      val rFields = (0 to i).map { j => Symbol(s"R$j") }
      val rSlots = rFields.map { f => obtain(stepI.signature.slot)(f) }
      val (_ , sig) = stepI.signature.addField(relationships -> CTList(CTRelationship))(context)
      Combine(stepI, rSlots.map(_.ordinal))(sig)
    }

    // Selection phase
    // Only keep relationship list column, and start id of first relationship (for joining)
    val selections = combines.map(_.selectFields(relationships, 'R0))

    // Union phase
    // Union combinations into one table
    val union = selections.reduce[StdCypherFrame[Product]] {
      case (acc, next) => acc.unionAll(next)
    }

    // Join phase
    // Join with input start nodes

    val startNodeWithId = input.nodeId(node)('nodeSTART).asRow
    val allRelsWithFirstStartId = union.relationshipStartId('R0)('FIRST).asRow

    startNodeWithId.join(allRelsWithFirstStartId).on('nodeSTART)('FIRST).asProduct.selectFields(node, relationships)
  }

  private final case class Combine(input: StdCypherFrame[Product], indices: Seq[Int])(sig: StdFrameSignature) extends ProductFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      in.map(combineToList(indices))(context.productEncoder(slots))
    }
  }

  private final case class combineToList(indices: Seq[Int]) extends (Product => Product) {

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

  /*
      f: rels_i, r_i+1, rels => rels_i+1

      rels2 = f(rels1, 'rels2, relationships)
      ...


      rels1 = node r1
      rels2 = node r1 r2
      rels3 = node r1 r2 r3
      ...

      rels1' = r = [r1]
      rels2' = r = [r1, r2]
      rels3' = r = [r1, r2, r3]

      result = UNION(rels1', rels2', ...)

   */

}
