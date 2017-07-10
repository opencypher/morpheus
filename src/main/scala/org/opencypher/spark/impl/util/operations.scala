package org.opencypher.spark.impl.util

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.RecordSlot
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords}
import org.opencypher.spark.impl.spark.SparkCypherEngine

// TODO: Figure out where this should live in relationship to existing instances etc
object operations {

  implicit class RichSparkCypherGraph(graph: SparkCypherGraph)(implicit engine: SparkCypherEngine) {
    type Records = SparkCypherRecords

    def filter(subject: Records, expr: Expr): Records =
      ???

    def select(subject: Records, fields: IndexedSeq[Var]): Records =
      ???

    def project(subject: Records, expr: Expr): Records =
      ???

    def alias(subject: Records, alias: (Expr, Var)): Records =
      ???

    def join(subject: Records, other: Records)(lhs: RecordSlot, rhs: RecordSlot): Records =
      ???
  }
}
