/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.types.{CTNode, _}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderOps
import org.opencypher.okapi.relational.impl.util.StringEncodingUtilities._

import scala.util.Random
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderSyntax.{addContent => implicitAddContent, addContents => implicitAddContents}

/**
  * A header for a CypherRecords.
  *
  * The header consists of a number of slots, each of which represents a Cypher expression.
  * The slots that represent variables (which is a kind of expression) are called <i>fields</i>.
  */
final case class RecordHeader(
  private[impl] val internalHeader: InternalHeader
) extends IRecordHeader {


  var newHeader: RecordHeaderNew =
    internalHeader.slots.map(_.content).foldLeft(RecordHeaderNew.empty)(addContentToNewHeader)


  override def generateUniqueName: String = {
    val NAME_SIZE = 5

    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = String.valueOf(chars.toArray).encodeSpecialCharacters

    if (slots.map(of).contains(name)) generateUniqueName
    else name
  }

  override def tempColName: String =
    ColumnNamer.tempColName

  override def of(slot: RecordSlot): String = {
    val cn = ColumnNamer.of(slot)
    assert(columns.contains(cn), s"the header did not contain $cn, had ${columns.mkString(", ")}")
    cn
  }

  override def of(slot: SlotContent): String = {
    val cn = ColumnNamer.of(slot)
    assert(columns.contains(cn), s"the header did not contain $cn, had ${columns.mkString(", ")}")
    cn
  }

  override def of(expr: Expr): String = {
    val cn = ColumnNamer.of(expr)
    assert(columns.contains(cn), s"the header did not contain $cn, had ${columns.mkString(", ")}")
    cn
  }

  override val columns: Seq[String] = internalHeader.slots.map(ColumnNamer.of)

  override def column(slot: RecordSlot): String = columns(slot.index)

  /**
    * Computes the concatenation of this header and another header.
    *
    * @param other the header with which to concatenate.
    * @return the concatenation of this and the argument header.
    */
  override def ++(other: IRecordHeader): IRecordHeader =
    copy(internalHeader ++ other.asInstanceOf[RecordHeader].internalHeader)

  /**
    * Removes the specified RecordSlot from the header
    *
    * @param toRemove record slot to remove
    * @return new IRecordHeader with removed slot
    */
  override def -(toRemove: RecordSlot): IRecordHeader =
    copy(internalHeader - toRemove)

  /**
    * Returns this record header with all fields of the other record header removed.
    *
    * @param other the header to remove
    * @return updated header
    */
  override def --(other: IRecordHeader): IRecordHeader =
    copy(internalHeader -- other.asInstanceOf[RecordHeader].internalHeader)

  /**
    * The ordered sequence of slots stored in this header.
    *
    * @return the slots in this header.
    */
  override def slots: IndexedSeq[RecordSlot] = internalHeader.slots

  override def contents: Set[SlotContent] = slots.map(_.content).toSet

  /**
    * The set of fields contained in this header.
    *
    * @return the fields in this header.
    */
  override def fields: Set[String] = fieldsAsVar.map(_.name)

  override def fieldsAsVar: Set[Var] = internalHeader.fields

  /**
    * The fields contained in this header, in the order they were defined.
    *
    * @return the ordered fields in this header.
    */
  override def fieldsInOrder: Seq[String] = slots.flatMap(_.content.alias.map(_.name))

  override def slotsFor(expr: Expr): Seq[RecordSlot] =
    internalHeader.slotsFor(expr)

  // TODO: Push error handling to API consumers

  override def slotFor(variable: Var): RecordSlot = slotsFor(variable).headOption.getOrElse(
    throw IllegalArgumentException(s"One of $fields", variable)
  )

  override def mandatory(slot: RecordSlot): Boolean =
    internalHeader.mandatory(slot)

  override def sourceNodeSlot(rel: Var): RecordSlot = slotsFor(StartNode(rel)()).headOption.getOrElse(
    throw IllegalArgumentException(s"One of $fields", rel)
  )

  override def targetNodeSlot(rel: Var): RecordSlot = slotsFor(EndNode(rel)()).headOption.getOrElse(
    throw IllegalArgumentException(s"One of $fields", rel)
  )

  override def typeSlot(rel: Expr): RecordSlot = slotsFor(Type(rel)()).headOption.getOrElse(
    throw IllegalArgumentException(s"One of $fields", rel)
  )

  override def labels(node: Var): Seq[HasLabel] = labelSlots(node).keys.toSeq

  override def properties(node: Var): Seq[Property] = propertySlots(node).keys.toSeq

  override def select(fields: Set[Var]): IRecordHeader = {
    fields.foldLeft(IRecordHeader.empty) {
      case (acc, next) =>
        val contents = childSlots(next).map(_.content)
        if (contents.nonEmpty) {
          acc.addContents(OpaqueField(next) +: contents)
        } else {
          acc
        }
    }
  }

  override def selfWithChildren(field: Var): Seq[RecordSlot] =
    slotFor(field) +: childSlots(field)

  override def childSlots(entity: Var): Seq[RecordSlot] = {
    slots.filter {
      case RecordSlot(_, OpaqueField(_)) => false
      case slot if slot.content.owner.contains(entity) => true
      case _ => false
    }
  }

  override def labelSlots(node: Var): Map[HasLabel, RecordSlot] = {
    slots.collect {
      case s@RecordSlot(_, ProjectedExpr(h: HasLabel)) if h.node == node => h -> s
      case s@RecordSlot(_, ProjectedField(_, h: HasLabel)) if h.node == node => h -> s
    }.toMap
  }

  override def propertySlots(entity: Var): Map[Property, RecordSlot] = {
    slots.collect {
      case s@RecordSlot(_, ProjectedExpr(p: Property)) if p.m == entity => p -> s
      case s@RecordSlot(_, ProjectedField(_, p: Property)) if p.m == entity => p -> s
    }.toMap
  }

  override def nodesForType(nodeType: CTNode): Seq[Var] = {
    slots.collect {
      case RecordSlot(_, OpaqueField(v)) => v
    }.filter { v =>
      v.cypherType match {
        case CTNode(labels, _) =>
          val allPossibleLabels = this.labels(v).map(_.label.name).toSet ++ labels
          nodeType.labels.subsetOf(allPossibleLabels)
        case _ => false
      }
    }
  }

  override def relationshipsForType(relType: CTRelationship): Seq[Var] = {
    val targetTypes = relType.types

    slots.collect {
      case RecordSlot(_, OpaqueField(v)) => v
    }.filter { v =>
      v.cypherType match {
        case t: CTRelationship if targetTypes.isEmpty || t.types.isEmpty => true
        case CTRelationship(types, _) =>
          types.exists(targetTypes.contains)
        case _ => false
      }
    }
  }

  override def toString: String = s"IRecordHeader with ${slots.size} slots"

  override def pretty: String = s"IRecordHeader with ${slots.size} slots: \n\t ${slots.map(slot => slot -> of(slot)).sortBy(_._2).mkString("\n\t")}"

  override def addContents(contents: Seq[SlotContent]): IRecordHeader = {

    newHeader = contents.foldLeft(newHeader)(addContentToNewHeader)

    val updatedOldHeader = new RecordHeaderOps(this).update(implicitAddContents(contents))._1
    validate(updatedOldHeader, newHeader)

    updatedOldHeader
  }

  override def addContent(content: SlotContent): IRecordHeader = {
    newHeader = addContentToNewHeader(newHeader, content)

    val updatedOldHeader = new RecordHeaderOps(this).update(implicitAddContent(content))._1

    validate(updatedOldHeader, newHeader)
    updatedOldHeader
  }

  private def addContentToNewHeader(header: RecordHeaderNew, content: SlotContent): RecordHeaderNew = {
    header.withExpr(content.key)
  }

  private def validate(oldHeader: RecordHeader, newHeader: RecordHeaderNew): Unit = {
    def printPretty =
      s"""
         |=== old header ===
         |${oldHeader.pretty}
         |=== new header ===
         |${newHeader.pretty}
       """.stripMargin

    // check number of physical columns
    val colsInOldHeader = oldHeader.slots.map(oldHeader.of).toSet
    val colsInNewHeader = newHeader.exprToColumn.values.toSet
    assert(colsInNewHeader == colsInOldHeader, s"different columns: \n$colsInOldHeader\n$colsInNewHeader\n$printPretty")

    // check column name equality
    oldHeader.slots.foreach { slot =>
      val exprInOldHeader = slot.content.key
      val colNameinOldHeader = oldHeader.of(slot)
      assert(newHeader.column(exprInOldHeader) == colNameinOldHeader, s"problem with expr in old header: $exprInOldHeader\n$printPretty")
    }
  }

  override def contains(content: SlotContent): Boolean = new RecordHeaderOps(this).update(implicitAddContent(content))._2 match {
    case _: Replaced[_] => true
    case _ => false
  }
}


