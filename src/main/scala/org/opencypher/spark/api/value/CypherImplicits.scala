package org.opencypher.spark.api.value

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with EntityData.Creation
