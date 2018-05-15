package org.opencypher.spark.api.io.metadata

case class CAPSGraphMetaData(
  tableStorageFormat: String,
  tags: Set[Int] = Set(0)
)
