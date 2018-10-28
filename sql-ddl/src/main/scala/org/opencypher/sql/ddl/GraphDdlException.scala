package org.opencypher.sql.ddl

abstract class GraphDdlException(msg: String, cause: Option[Exception] = None) extends RuntimeException(msg, cause.orNull) with Serializable {
  import GraphDdlException._
  def getFullMessage: String = causeChain(this).map(_.getMessage).mkString("\n")
}

private[ddl] object GraphDdlException {

  def unresolved(desc: String, reference: Any): Nothing = throw UnresolvedReferenceException(
    s"""$desc: $reference"""
  )

  def unresolved(desc: String, reference: Any, available: Traversable[Any]): Nothing = throw UnresolvedReferenceException(
    s"""$desc: $reference
       |Expected one of: ${available.mkString(", ")}""".stripMargin
  )

  def duplicate(desc: String, definition: Any): Nothing = throw DuplicateDefinitionException(
    s"""$desc: $definition"""
  )

  def incompatibleTypes(msg: String): Nothing = throw TypeException(msg)

  def contextualize[T](msg: String)(block: => T): T =
    try { block } catch { case e: Exception => throw ContextualizedException(msg, Some(e)) }

  def causeChain(e: Throwable): List[Throwable] = causeChain(e, Set.empty)
  def causeChain(e: Throwable, seen: Set[Throwable]): List[Throwable] = {
    val newSeen = seen + e
    e :: Option(e.getCause).filterNot(newSeen).toList.flatMap(causeChain(_, newSeen))
  }
}

case class UnresolvedReferenceException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class DuplicateDefinitionException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class TypeException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)

case class ContextualizedException(msg: String, cause: Option[Exception] = None) extends GraphDdlException(msg, cause)