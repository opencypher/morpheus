package org.opencypher.spark.api

import org.opencypher.spark.api.value.{CypherValue, EntityData}

trait CypherImplicits
  extends CypherValue.Encoders
    with CypherValue.Conversion
    with EntityData.Creation
