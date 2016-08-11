package org.opencypher.spark.api

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with Ternary.Conversion
    with EntityId.Conversion
    with EntityData.Creation
