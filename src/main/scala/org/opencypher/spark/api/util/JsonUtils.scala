package org.opencypher.spark.api.util

import io.circe.Decoder
import io.circe.parser.parse

object JsonUtils {
  def parseJson[T](jsonString: String)(implicit decoder: Decoder[T]): T = {
    parse(jsonString) match {
      case Left(failure) => throw new RuntimeException(s"Invalid json file: $failure")
      case Right(json) => json.hcursor.as[T] match {
        case Left(failure) => {
          val msg= s"Invalid JSON schema: Could not find mandatory element '${failure.history.head.productElement(0)}'"
          throw new RuntimeException(msg)
        }
        case Right(elem) => elem
      }
    }
  }
}
