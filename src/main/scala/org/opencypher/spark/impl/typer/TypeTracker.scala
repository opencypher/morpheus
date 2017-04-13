package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.api.types.CypherType

import scala.annotation.tailrec

object TypeTracker {
  val empty = TypeTracker(List.empty)
}

case class TypeTracker(maps: List[Map[Expression, CypherType]]) {
  def get(e: Expression): Option[CypherType] = get(e, maps)

  @tailrec
  private def get(e: Expression, maps: List[Map[Expression, CypherType]]): Option[CypherType] = maps.headOption match {
    case None => None
    case Some(map) if map.contains(e) => map.get(e)
    case Some(_) => get(e, maps.tail)
  }

  def updated(e: Expression, t: CypherType): TypeTracker = copy(maps = head.updated(e, t) +: tail)
  def updated(entry: (Expression, CypherType)): TypeTracker = updated(entry._1, entry._2)
  def pushScope(): TypeTracker = copy(maps = Map.empty[Expression, CypherType] +: maps)
  def popScope(): Option[TypeTracker] = if (maps.isEmpty) None else Some(copy(maps = maps.tail))

  private def head: Map[Expression, CypherType] =
    maps.headOption.getOrElse(Map.empty[Expression, CypherType])
  private def tail: List[Map[Expression, CypherType]] =
    if (maps.isEmpty) List.empty else maps.tail
}
