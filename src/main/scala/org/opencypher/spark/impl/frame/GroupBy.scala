package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTInteger
import org.opencypher.spark.prototype.api.value.{CypherList, CypherValue}
import org.opencypher.spark.impl._

object GroupBy extends FrameCompanion {

  import org.opencypher.spark.impl.util._

  def apply(input: StdCypherFrame[Product])(groupingKey: Symbol*)(agg: AggregationFunction)(implicit context: PlanningContext): StdCypherFrame[Product] = {
    val inputSig = input.signature
    val aggType = agg.outType(inputSig)
    val computer = aggregationComputer(agg, inputSig)

    if (groupingKey.isEmpty) {
      val (_, keySig) = StdFrameSignature.empty.addField('AGG -> CTInteger)
      val (_, outSig) = StdFrameSignature.empty.addField(agg.outField -> aggType)
      GroupByEmptyKey(input)(keySig.slots, computer)(outSig)
    } else {
      val (_, keySig) = inputSig.selectFields(groupingKey: _*)
      val inputKeySlotIndices = groupingKey.map(inputSig.slot).map(_.get.ordinal).toVector
      val (_, outSig) = keySig.addField(agg.outField -> aggType)
      GroupByNonEmptyKey(input)(keySig.slots, inputKeySlotIndices, computer)(outSig)
    }
  }

  private final case class GroupByNonEmptyKey(input: StdCypherFrame[Product])(keySlots: Seq[StdSlot], keyIndices: Vector[Int], agg: AggregationComputer)(outSig: StdFrameSignature)
    extends ProductFrame(outSig) {

    override def execute(implicit context: RuntimeContext): Dataset[Product] = {
      val in = input.run(context)
      val group = in.groupByKey(extractGroupingKey(keyIndices))(context.productEncoder(keySlots))
      val out = group.flatMapGroups(agg)(context.productEncoder(outSig.slots))

      out
    }
  }

  private final case class extractGroupingKey(indices: Vector[Int]) extends (Product => Product) {
    override def apply(in: Product): Product = indices.map(in.get).asProduct
  }

  private final case class GroupByEmptyKey(input: StdCypherFrame[Product])(keySlots: Seq[StdSlot], agg: AggregationComputer)(outSig: StdFrameSignature)
    extends ProductFrame(outSig) {

    override def execute(implicit context: RuntimeContext): Dataset[Product] = {
      val in = input.run(context)
      val group = in.groupByKey(useEmptyGroupingKey)(context.productEncoder(keySlots))
      val reduced = group.flatMapGroups(dropKey(agg))(context.productEncoder(outSig.slots))

      reduced
    }
  }

  private case object useEmptyGroupingKey extends (Product => Product) {
    private val singleton = Tuple1(0L)

    override def apply(in: Product): Product = singleton
  }

  private object aggregationComputer extends ((AggregationFunction, StdFrameSignature) => AggregationComputer) {
    def apply(agg: AggregationFunction, sig: StdFrameSignature): AggregationComputer = agg match {
      case _: Collect => computeCollect(sig.slot(agg.inField).get.ordinal)
      case _: Count => computeCount
      case _ => ???
    }
  }

  private sealed trait AggregationComputer extends ((Product, Iterator[Product]) => TraversableOnce[Product]) {
    def compute(key: Product, partition: Iterator[Product]): Product

    override final def apply(key: Product, partition: Iterator[Product]): TraversableOnce[Product] =
      Seq(compute(key, partition))
  }

  private final case class dropKey(computer: AggregationComputer) extends AggregationComputer {
    override def compute(key: Product, partition: Iterator[Product]): Product =
      computer.compute(ZeroProduct, partition)
  }

  private case object computeCount extends AggregationComputer {
    override def compute(key: Product, partition: Iterator[Product]): Product =
      key :+ partition.size.toLong
  }

  private final case class computeCollect(index: Int) extends AggregationComputer {
    import org.opencypher.spark.impl.util._

    override def compute(key: Product, partition: Iterator[Product]): Product =
      key :+ CypherList(partition.map(_.getAs[CypherValue](index)).filter(_ != null).toIndexedSeq)
  }
}
