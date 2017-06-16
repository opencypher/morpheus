package org.opencypher.spark.api.spark

import java.util.Collections

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr.{Property, Var}
import org.opencypher.spark.api.record._
import org.opencypher.spark.impl.convert.{fromSparkType, toSparkType}
import org.opencypher.spark.impl.record.SparkCypherRecordHeader
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.header._

sealed abstract class SparkCypherRecords(tokens: SparkCypherTokens, initialHeader: RecordHeader, initialData: DataFrame)
                                        (implicit val space: SparkGraphSpace)
  extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords

  override def header = initialHeader
  override def data = initialData

  override def columns: IndexedSeq[String] =
    header.internalHeader.columns

  override def column(slot: RecordSlot): String =
    header.internalHeader.column(slot)

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): Data = data

  // TODO: Check that this does not change the caching of our data frame
  def cached = SparkCypherRecords.create(header, data.cache())

  override def show() = data.show

  def compact = {
    val cachedHeader = self.header.update(compactFields)._1
    val cachedData = {
      val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
      self.data.select(columns: _*)
    }

    SparkCypherRecords.create(cachedHeader, cachedData)
  }



  // only keep slots with v as their owner
  //  def focus(v: Var): SparkCypherRecords = {
  //    val (newHeader, _) = self.header.update(selectFields(slot => slot.content.owner.contains(v)))
  //    val newColumns = newHeader.slots.collect {
  //      case RecordSlot(_, content: FieldSlotContent) => new Column(SparkColumnName.of(content))
  //    }
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = self.data.select(newColumns: _*)
  //    }
  //  }

  //  // alias oldVar to newVar, without guarding against shadowing
  //  def alias(oldVar: Var, newVar: Var): SparkCypherRecords = {
  //    val oldIndices: Map[SlotContent, Int] = self.header.slots.map { slot: RecordSlot =>
  //      slot.content match {
  //        case p: ProjectedSlotContent =>
  //          p.expr match {
  //            case h@HasLabel(`oldVar`, label) => ProjectedExpr(HasLabel(newVar, label)(h.cypherType)) -> slot.index
  //            case p@Property(`oldVar`, key) => ProjectedExpr(Property(newVar, key)(p.cypherType))-> slot.index
  //            case _ => p -> slot.index
  //          }
  //
  //        case _: OpaqueField => OpaqueField(newVar) -> slot.index
  //        case content => content -> slot.index
  //      }
  //    }.toMap
  //
  //    // TODO: Check result for failure to add
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(oldIndices.keySet.toSeq))
  //    val newIndices = newHeader.slots.map(slot => slot.content -> slot.index).toMap
  //    val indexMapping = oldIndices.map {
  //      case (content, oldIndex) => oldIndex -> newIndices(content)
  //    }.toSeq.sortBy(_._2)
  //
  //    val columns = indexMapping.map {
  //      case (oldIndex, newIndex) =>
  //        val oldName = SparkColumnName.of(self.header.slots(oldIndex).content)
  //        val newName = SparkColumnName.of(newHeader.slots(newIndex).content)
  //        new Column(oldName).as(newName)
  //    }
  //
  //    val newData = self.data.select(columns: _*)
  //
  //    new SparkCypherRecords {
  //      override def data = newData
  //      override def header = newHeader
  //    }
  //  }
  //

  //  // union two record sets in their shared columns, dropping all non-shared columns
  //  // missing values, but discarding overlapping slots
  //  def union(other: SparkCypherRecords): SparkCypherRecords = {
  //    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }
  //
  //    val selfData = self.data.select(newColumns: _*)
  //    val otherData = other.data.select(newColumns: _*)
  //
  //    // TODO: Make distinct per entity fields
  //    val newData = selfData.union(otherData).distinct()
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newData
  //    }
  //  }

  //  def intersect(other: SparkCypherRecords): SparkCypherRecords = {
  //    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }
  //
  //    val selfData = self.data.select(newColumns: _*)
  //    val otherData = other.data.select(newColumns: _*)
  //
  //    // TODO: Make distinct per entity fields
  //    val newData = selfData.intersect(otherData).distinct()
  //
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newData
  //    }
  //  }

  //  // concatenates two record sets, using a union of their columns and using null as as default for
  //  // missing values, but discarding overlapping slots
  //  def concat(other: SparkCypherRecords): SparkCypherRecords = {
  //    val duplicate = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => !duplicate(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val selfColumns =
  //      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))} ++
  //      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) }
  //    val newSelfData = self.data.select(selfColumns: _*)
  //
  //    val otherColumns =
  //      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) } ++
  //      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))}
  //    val newOtherData = other.data.select(otherColumns: _*)
  //
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newSelfData.union(newOtherData)
  //    }
  //  }

  override def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): SparkCypherRecords = {
    val slotExprs = entity.slots
    val newSlots = header.slots.map {
      case slot@RecordSlot(idx, content: FieldSlotContent) =>
        slotExprs.get(content.field.name).map {
          case expr: Var      => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(content.cypherType))
          case expr           => ProjectedExpr(expr)
        }
        .getOrElse(slot.content)

      case slot =>
        slot.content
    }
    val newHeader = RecordHeader.from(newSlots: _*)
    SparkCypherRecords.create(newHeader, data)
  }

  def distinct: SparkCypherRecords = SparkCypherRecords.create(self.header, self.data.distinct())
}

object SparkCypherRecords {

  def create(initialDataFrame: DataFrame)(implicit graphSpace: SparkGraphSpace): SparkCypherRecords = {
    val initialHeader = SparkCypherRecordHeader.fromSparkStructType(initialDataFrame.schema)
    create(initialHeader, initialDataFrame)
  }

  def create(initialHeader: RecordHeader, initialData: DataFrame)(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords = {
    if (initialData.sparkSession == graphSpace.session) {

      // Ensure no duplicate columns in initialData
      val initialDataColumns = initialData.columns.toSeq
      if (initialDataColumns.size != initialDataColumns.distinct.size)
        throw new IllegalArgumentException("Cannot use data frames with duplicate column names")

      // Correctly name columns in initialData
      val columns = initialHeader.internalHeader.columns
      val data = if (columns == initialDataColumns) initialData else initialData.toDF(columns: _*)

      // Verify column types
      initialHeader.slots.foreach { slot =>
        val field = data.schema.fields(slot.index)
        val cypherType = fromSparkType(field.dataType, field.nullable)
        val headerType = slot.content.cypherType

        if (toSparkType(headerType) != toSparkType(cypherType))
          throw new IllegalArgumentException(s"Invalid data type for column ${field.name}. Expected at least $headerType but got conflicting $cypherType")
      }

      new SparkCypherRecords(graphSpace.tokens, initialHeader, data) {}
    }
    else {
      throw new IllegalArgumentException("Import of a data frame not created in the same session as the graph space")
    }
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords = {
    val initialSparkStructType = SparkCypherRecordHeader.asSparkStructType(initialHeader)
    val initialDataFrame = graphSpace.session.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    create(initialHeader, initialDataFrame)
  }
}
