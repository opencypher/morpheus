package org.opencypher.spark.api.io.json

import io.circe.Decoder.Result
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.spark.api.io.AbstractDataSource
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema

trait JsonSerialization {
  self: AbstractDataSource =>

  import CAPSSchema._
  import JsonSerialization._
  import io.circe.syntax._

  protected def readJsonSchema(graphName: GraphName): String

  protected def writeJsonSchema(graphName: GraphName, schema: String): Unit

  protected def readJsonCAPSGraphMetaData(graphName: GraphName): String

  protected def writeJsonCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: String): Unit


  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    parse[Schema](readJsonSchema(graphName)).asCaps
  }

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = {
    schema.schema.asJson.toString
  }

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = {
    parse[CAPSGraphMetaData](readJsonCAPSGraphMetaData(graphName))
  }

  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = {
    writeJsonCAPSGraphMetaData(graphName, capsGraphMetaData.asJson.toString)
  }

}

object JsonSerialization {

  import io.circe._
  import io.circe.generic.auto._
  import io.circe.generic.semiauto._

  def parse[A: Decoder](json: String): A = {
    parser.parse(json) match {
      case Right(parsedJson) => parsedJson.as[A].value
      case Left(f) => throw f // ParsingFailure
    }
  }

  implicit class DeserializationResult[A](r: Result[A]) {
    def value: A = {
      r match {
        case Right(value) => value
        case Left(f) => throw f // DecodingFailure
      }
    }
  }

  implicit val encodeLabelKeys: KeyEncoder[Set[String]] = new KeyEncoder[Set[String]] {
    override def apply(labels: Set[String]): String = labels.toSeq.sorted.mkString("_")
  }

  implicit val decodeLabelKeys: KeyDecoder[Set[String]] = new KeyDecoder[Set[String]] {
    override def apply(key: String): Option[Set[String]] = {
      if (key.isEmpty) Some(Set.empty) else Some(key.split("_").toSet)
    }
  }

  implicit val encodeSchema: Encoder[Schema] =
    Encoder.forProduct2("labelPropertyMap", "relTypePropertyMap")(s =>
      (s.labelPropertyMap.map, s.relTypePropertyMap.map)
    )

  implicit val decodeSchema: Decoder[Schema] =
    Decoder.forProduct2("labelPropertyMap", "relTypePropertyMap")(
      (lpm: Map[Set[String], PropertyKeys], rpm: Map[String, PropertyKeys]) =>
        SchemaImpl(LabelPropertyMap(lpm), RelTypePropertyMap(rpm)))

  implicit val encodeMetaData: Encoder[CAPSGraphMetaData] = deriveEncoder[CAPSGraphMetaData]
  implicit val decodeMetaData: Decoder[CAPSGraphMetaData] = deriveDecoder[CAPSGraphMetaData]

}
