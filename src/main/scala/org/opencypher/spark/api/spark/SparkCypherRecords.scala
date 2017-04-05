package org.opencypher.spark.api.spark

import java.util.Collections

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.opencypher.spark.api.expr.{HasLabel, Property, Var}
import org.opencypher.spark.api.record._
import org.opencypher.spark.impl.spark.{SparkColumnName, SparkSchema, toSparkType}
import org.opencypher.spark.impl.syntax.header.{addContents, selectFields, _}

trait SparkCypherRecords extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords

  override def columns: IndexedSeq[String] =
    header.internalHeader.columns

  override def column(slot: RecordSlot): String =
    header.internalHeader.column(slot)

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): Data = data.cache()

  override def show() = data.show

  def compact = new SparkCypherRecords {
    private lazy val cachedHeader = self.header.update(compactFields)._1
    private lazy val cachedData = {
      val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
      self.data.select(columns: _*)
    }

    override def header = cachedHeader
    override def data = cachedData
  }

  // only keep slots with v as their owner
  def focus(v: Var): SparkCypherRecords = {
    val (newHeader, _) = self.header.update(selectFields(slot => slot.content.owner.contains(v)))
    val newColumns = newHeader.slots.collect {
      case RecordSlot(_, content: FieldSlotContent) => new Column(SparkColumnName.of(content))
    }
    new SparkCypherRecords {
      override def header = newHeader
      override def data = self.data.select(newColumns: _*)
    }
  }

  // alias oldVar to newVar, without guarding against shadowing
  def alias(oldVar: Var, newVar: Var): SparkCypherRecords = {
    val oldIndices: Map[SlotContent, Int] = self.header.slots.map { slot: RecordSlot =>
      slot.content match {
        case p: ProjectedSlotContent =>
          p.expr match {
            case HasLabel(`oldVar`, label, _) => ProjectedExpr(HasLabel(newVar, label), p.cypherType) -> slot.index
            case Property(`oldVar`, key, _) => ProjectedExpr(Property(newVar, key), p.cypherType)-> slot.index
            case _ => p -> slot.index
          }

        case OpaqueField(`oldVar`, cypherType) => OpaqueField(newVar, cypherType) -> slot.index
        case content => content -> slot.index
      }
    }.toMap

    // TODO: Check result for failure to add
    val (newHeader, _) = RecordHeader.empty.update(addContents(oldIndices.keySet.toSeq))
    val newIndices = newHeader.slots.map(slot => slot.content -> slot.index).toMap
    val indexMapping = oldIndices.map {
      case (content, oldIndex) => oldIndex -> newIndices(content)
    }.toSeq.sortBy(_._2)

    val columns = indexMapping.map {
      case (oldIndex, newIndex) =>
        val oldName = SparkColumnName.of(self.header.slots(oldIndex).content)
        val newName = SparkColumnName.of(newHeader.slots(newIndex).content)
        new Column(oldName).as(newName)
    }.toSeq

    val newData = self.data.select(columns: _*)

    new SparkCypherRecords {
      override def data = newData
      override def header = newHeader
    }
  }


  // union two record sets in their shared columns, dropping all non-shared columns
  // missing values, but discarding overlapping slots
  def union(other: SparkCypherRecords): SparkCypherRecords = {
    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))

    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }

    val selfData = self.data.select(newColumns: _*)
    val otherData = other.data.select(newColumns: _*)

    // TODO: Make distinct per entity fields
    val newData = selfData.union(otherData).distinct()
    new SparkCypherRecords {
      override def header = newHeader
      override def data = newData
    }
  }

  def intersect(other: SparkCypherRecords): SparkCypherRecords = {
    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))

    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }

    val selfData = self.data.select(newColumns: _*)
    val otherData = other.data.select(newColumns: _*)

    // TODO: Make distinct per entity fields
    val newData = selfData.intersect(otherData).distinct()

    new SparkCypherRecords {
      override def header = newHeader
      override def data = newData
    }
  }

  // concatenates two record sets, using a union of their columns and using null as as default for
  // missing values, but discarding overlapping slots
  def concat(other: SparkCypherRecords): SparkCypherRecords = {
    val duplicate = (self.header.slots intersect other.header.slots).map(_.content).toSet
    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => !duplicate(content)).distinct
    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))

    val selfColumns =
      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))} ++
      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) }
    val newSelfData = self.data.select(selfColumns: _*)

    val otherColumns =
      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) } ++
      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))}
    val newOtherData = other.data.select(otherColumns: _*)

    new SparkCypherRecords {
      override def header = newHeader
      override def data = newSelfData.union(newOtherData)
    }
  }

  def distinct: SparkCypherRecords =
    new SparkCypherRecords {
      override def header = self.header
      override def data = self.data.distinct()
    }
}

object SparkCypherRecords {
  def empty(session: SparkSession, emptyHeader: RecordHeader = RecordHeader.empty) = new SparkCypherRecords {
    override def data = session.createDataFrame(Collections.emptyList[Row](), SparkSchema.from(emptyHeader))
    override def header = emptyHeader
  }
}
