package org.opencypher.spark.prototype.impl.record

import cats.data.State
import cats.data.State.{get, set}
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.spark.SparkColumnName
import org.opencypher.spark.prototype.impl.syntax.register._
import org.opencypher.spark.prototype.impl.syntax.expr._
import org.opencypher.spark.prototype.impl.util.RefCollection.AbstractRegister
import org.opencypher.spark.prototype.impl.util._

import scala.annotation.tailrec

// TODO: Prevent projection of expressions with unfulfilled dependencies
final case class InternalHeader protected[spark](
    private val slotContents: RefCollection[SlotContent],
    private val exprSlots: Map[Expr, Vector[Int]],
    private val cachedFields: Set[Var]
  )
  extends Serializable {

  self =>

  import InternalHeader.{addContent, recordSlotRegister}

  private lazy val cachedSlots = slotContents.contents.map(RecordSlot.from).toIndexedSeq
  private lazy val cachedColumns = slots.map(computeColumnName).toVector

  def ++(other: InternalHeader): InternalHeader =
    other.slotContents.elts.foldLeft(this) {
      case (acc, content) => acc + content
    }

  def slots = cachedSlots

  def fields = cachedFields

  def slotsFor(expr: Expr, cypherType: CypherType): Traversable[RecordSlot] =
    slotsFor(expr).filter(_.content.cypherType == cypherType)

  def slotsFor(expr: Expr): Traversable[RecordSlot] =
    exprSlots.getOrElse(expr, Vector.empty).flatMap(ref => slotContents.lookup(ref).map(RecordSlot(ref, _)))

  def +(addedContent: SlotContent): InternalHeader =
    addContent(addedContent).runS(self).value

  def columns = cachedColumns

  def column(slot: RecordSlot) = cachedColumns(slot.index)

  private def computeColumnName(slot: RecordSlot): String = {
    val content = slot.content
    val optExtraType = slotsFor(content.key, content.cypherType).slice(1, 2).headOption
    if (optExtraType.isEmpty)
      SparkColumnName.of(slot.content)
    else
      SparkColumnName.withType(content)
  }
}

object InternalHeader {
  val empty = new InternalHeader(RefCollection.empty, Map.empty, Set.empty)

  def apply(contents: SlotContent*) =
    from(contents)

  def from(contents: TraversableOnce[SlotContent]) =
    contents.foldLeft(empty) { case (header, slot) => header + slot }

  def addContent(addedContent: SlotContent): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    addedContent match {
      case (it: ProjectedExpr) => addProjectedExpr(it)
      case (it: OpaqueField) => addOpaqueField(it)
      case (it: ProjectedField) => addProjectedField(it)
    }

  private def addProjectedExpr(content: ProjectedExpr): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <- {
        val existingSlot =
          for (slot <- header.slotsFor(content.expr, content.cypherType).headOption)
          yield pureState[AdditiveUpdateResult[RecordSlot]](Found(slot))
        existingSlot.getOrElse {
            header.slotContents.insert(content) match {
              case Left(ref) => pureState(Found(slot(header, ref)))
              case Right((optNewSlots, ref)) => addSlotContent(optNewSlots, ref, content)
            }
          }
      }
    )
    yield result

  private def addOpaqueField(addedContent: OpaqueField): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    addField(addedContent)

  private def addProjectedField(addedContent: ProjectedField): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for(
      header <- get[InternalHeader];
      result <- {
        val existingSlot = header.slotsFor(addedContent.expr, addedContent.cypherType).headOption
        existingSlot.flatMap[State[InternalHeader, AdditiveUpdateResult[RecordSlot]]] {
          case RecordSlot(ref, _: ProjectedExpr) =>
            Some(header.slotContents.update(ref, addedContent) match {
              case Left(conflict) =>
                pureState(FailedToAdd(slot(header, conflict), Added(RecordSlot(ref, addedContent))))

              case Right(newSlots) =>
                addSlotContent(Some(newSlots), ref, addedContent).map(added => Replaced(slot(header, ref), added.it))
            })
          case _ =>
            None
        }
        .getOrElse { addField(addedContent) }
      }
    )
    yield result

  private def addField(addedContent: FieldSlotContent): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <- {
        header.slotContents.insert(addedContent) match {
          case Left(ref) => pureState(FailedToAdd(slot(header, ref), Added(RecordSlot(ref, addedContent))))
          case Right((optNewSlots, ref)) => addSlotContent(optNewSlots, ref, addedContent)
        }
      }
    )
    yield result


  private def addSlotContent(optNewSlots: Option[RefCollection[SlotContent]], ref: Int, addedContent: SlotContent)
  : State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <-
        optNewSlots match {
          case Some(newSlots) =>
            val newExprSlots = addedContent.support.foldLeft(header.exprSlots) {
              case (slots, expr) => addExprSlots(slots, expr, ref)
            }
            val newFields = addedContent.alias.map(header.cachedFields + _).getOrElse(header.cachedFields)
            val newHeader = InternalHeader(newSlots, newExprSlots, newFields)
            set[InternalHeader](newHeader).map(_ => Added(RecordSlot(ref, addedContent)))

          case None =>
            pureState(Found(slot(header, ref)))
        }
    )
    yield result

  private def addExprSlots(m: Map[Expr, Vector[Int]], key: Expr, value: Int): Map[Expr, Vector[Int]] =
    if (m.getOrElse(key, Vector.empty).contains(value)) m else m.updated(key, m.getOrElse(key, Vector.empty) :+ value)

  def removeContent(removedContent: SlotContent): State[InternalHeader, RemovingUpdateResult[SlotContent]] = {
    for (
      header <- get[InternalHeader]
    )
      yield
        header.slotContents.find(removedContent) match {
          case Some(ref) =>
            val slot = RecordSlot(ref, removedContent)
            val dependencies = removeDependencies(List(List(slot)), header.slots.toSet, Set.empty, Set.empty)
            // TODO: Actually make this happen
            Removed(removedContent, dependencies.map(_.content) - removedContent)

          case None =>
            NotFound(removedContent)
        }
  }

  @tailrec
  private def removeDependencies(
    drop: List[List[RecordSlot]], remaining: Set[RecordSlot], removedFields: Set[Var], removedSlots: Set[RecordSlot]
  ) : Set[RecordSlot] =
    drop match {
      case (hdList: List[RecordSlot]) :: (tlList: List[List[RecordSlot]]) =>
        hdList match {
          case hd :: tl if !removedSlots.contains(hd) =>
            hd.content match {
              case s: FieldSlotContent =>
                val newFields = removedFields + s.field
                val (nonDepending, depending) = remaining.partition {
                  case RecordSlot(_, c: ProjectedSlotContent) => (c.expr.dependencies intersect newFields).isEmpty
                  case _ => true
                }
                val newRemaining = nonDepending
                val newRemoved = depending.toList :: tlList
                removeDependencies(newRemoved, nonDepending, newFields, removedSlots + hd)
              case _ =>
                removeDependencies(tlList, remaining - hd, removedFields, removedSlots + hd)
            }
          case _ =>
            removeDependencies(tlList, remaining, removedFields, removedSlots)
        }
      case _ =>
        removedSlots
    }

  private def pureState[X](it: X) = State.pure[InternalHeader, X](it)

  private implicit def recordSlotRegister: AbstractRegister[Int, (Expr, CypherType), SlotContent] =
    new AbstractRegister[Int, (Expr, CypherType), SlotContent]() {
      override def key(defn: SlotContent): (Expr, CypherType) = defn.key -> defn.cypherType
      override protected def id(ref: Int): Int = ref
      override protected def ref(id: Int): Int = id
    }

  private def slot(header: InternalHeader, ref: Int) = RecordSlot(ref, header.slotContents.elts(ref))
}
