package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.spark.SparkColumnName

import scala.annotation.tailrec
import scala.util.Random

object FreshVariableNamer {
  val NAME_SIZE = 5
  val PREFIX = "  "

  @tailrec
  def generateUniqueName(header: RecordHeader): String = {
    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = SparkColumnName.from(Some(String.valueOf(chars.toArray)))

    if (header.slots.map(SparkColumnName.of).contains(name)) generateUniqueName(header)
    else name
  }

  def apply(seed: String, t: CypherType): Var = Var(s"$PREFIX$seed")(t)
}
