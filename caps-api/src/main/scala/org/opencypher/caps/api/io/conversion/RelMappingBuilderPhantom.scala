package org.opencypher.caps.api.io.conversion

import org.opencypher.caps.api.io.conversion.RelMappingBuilder.RelMappingState._

object RelMapping {
  val empty: RelMappingBuilder[EmptyRelMapping] = RelMappingBuilder[EmptyRelMapping]()
}

sealed case class RelMapping(
  sourceIdKey: String,
  sourceStartNodeKey: String,
  sourceEndNodeKey: String,
  relTypeOrSourceRelTypeKey: Either[String, String],
  propertyMapping: Map[String, String] = Map.empty) {
}

sealed case class RelMappingBuilder[State <: RelMappingBuilder.RelMappingState](
  sourceIdKey: Option[String] = None,
  sourceStartNodeKey: Option[String] = None,
  sourceEndNodeKey: Option[String] = None,
  relTypeOrSourceRelTypeKey: Option[Either[String, String]] = None,
  propertyMapping: Map[String, String] = Map.empty) {

  def withSourceIdKey(sourceIdKey: String): RelMappingBuilder[State with SourceIdKeyPresent] =
    copy(sourceIdKey = Some(sourceIdKey))

  def withSourceStartNodeKey(sourceStartNodeKey: String): RelMappingBuilder[State with SourceStartNodeKeyPresent] =
    copy(sourceStartNodeKey = Some(sourceStartNodeKey))

  def withSourceEndNodeKey(sourceEndNodeKey: String): RelMappingBuilder[State with SourceEndNodeKeyPresent] =
    copy(sourceEndNodeKey = Some(sourceEndNodeKey))

  def withRelType(relType: String): RelMappingBuilder[State with RelTypePresent] =
    copy(relTypeOrSourceRelTypeKey = Some(Left(relType)))

  def withSourceRelTypeKey(sourceRelTypeKey: String): RelMappingBuilder[State with RelTypePresent] =
    copy(relTypeOrSourceRelTypeKey = Some(Right(sourceRelTypeKey)))

  def withPropertyKey(propertyKey: String): RelMappingBuilder[State] =
    copy(propertyMapping = propertyMapping.updated(propertyKey, propertyKey))

  def build(implicit ev: State =:= ValidRelMappingState): RelMapping =
    RelMapping(
      this.sourceIdKey.get,
      this.sourceStartNodeKey.get,
      this.sourceEndNodeKey.get,
      this.relTypeOrSourceRelTypeKey.get,
      this.propertyMapping)
}

object RelMappingBuilder {

  sealed trait RelMappingState {

    def sourceIdKey: String

    def sourceStartNodeKey: String

    def sourceEndNodeKey: String

    def relTypeOrSourceRelTypeKey: Either[String, String]

    def propertyMapping: Map[String, String]
  }

  object RelMappingState {

    sealed trait EmptyRelMapping extends RelMappingState

    sealed trait SourceIdKeyPresent extends RelMappingState

    sealed trait SourceStartNodeKeyPresent extends RelMappingState

    sealed trait SourceEndNodeKeyPresent extends RelMappingState

    sealed trait RelTypePresent extends RelMappingState

    type ValidRelMappingState = EmptyRelMapping
      with SourceIdKeyPresent
      with SourceStartNodeKeyPresent
      with SourceEndNodeKeyPresent
      with RelTypePresent
  }

}

object Foo extends App {

  val foo = RelMapping.empty
    .withSourceIdKey("id")
    .withSourceStartNodeKey("source")
    .withSourceEndNodeKey("target")
    .withRelType("FRIEND_OF")
    .withPropertyKey("since")
    .build
}
