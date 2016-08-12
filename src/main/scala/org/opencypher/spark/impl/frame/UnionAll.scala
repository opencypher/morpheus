package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.{StdCypherFrame, StdRuntimeContext}

object UnionAll {

  def apply(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product]): StdCypherFrame[Product] = {
    new UnionAll(lhs, rhs)
  }

  private final class UnionAll(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product]) extends StdCypherFrame[Product](lhs.signature) {

    assert(lhs.signature.fields.equals(rhs.signature.fields), throw new IllegalArgumentException(
      s"""Fields must be equal in UNION
        |${lhs.signature.fields}
        |${rhs.signature.fields}
      """.stripMargin))

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val union = lhsIn.union(rhsIn)

      alias(union, context.productEncoder(slots))
    }
  }

}
