package org.opencypher.spark.impl.typer

import cats.kernel.Semigroup
import org.neo4j.cypher.internal.frontend.v3_2.Ref
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.api.types.CypherType

import scala.annotation.tailrec

final case class TypeRecorder(recordedTypes: Seq[(Ref[Expression], CypherType)]) {

  def toMap: Map[Ref[Expression], CypherType] = toMap(Map.empty, recordedTypes)

  @tailrec
  private def toMap(m: Map[Ref[Expression], CypherType],
                    recorded: Seq[(Ref[Expression], CypherType)]): Map[Ref[Expression], CypherType] =
    recorded.headOption match {
      case Some((ref, t)) =>
        m.get(ref) match {
          case Some(t2) => toMap(m.updated(ref, t.join(t2)), recorded.tail)
          case None => toMap(m.updated(ref, t), recorded.tail)
        }
      case None =>
        m
    }
}

object TypeRecorder {
  implicit object recorderSemigroup extends Semigroup[TypeRecorder] {
    override def combine(x: TypeRecorder, y: TypeRecorder): TypeRecorder = {
      TypeRecorder(x.recordedTypes ++ y.recordedTypes)
    }
  }

}
