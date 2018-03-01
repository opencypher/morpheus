package org.opencypher.okapi.impl.schema

import org.opencypher.okapi.api.schema.Schema

object SchemaUtils {

  implicit class RichSchema(schema: Schema) {
    // TODO: document the magic
    def foldAndProduce[A](
      zero: Map[String, A])(bound: (A, Set[String], String) => A, fresh: (Set[String], String) => A): Map[String, A] = {
      schema.labelPropertyMap.labelCombinations.foldLeft(zero) {
        case (map, labelCombos) =>
          labelCombos.foldLeft(map) {
            case (innerMap, label) =>
              innerMap.get(label) match {
                case Some(a) =>
                  innerMap.updated(label, bound(a, labelCombos, label))
                case None =>
                  innerMap.updated(label, fresh(labelCombos, label))
              }
          }
      }
    }
  }

}
