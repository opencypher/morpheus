package org.opencypher.spark

import org.junit.runner.RunWith
import org.opencypher.spark_legacy.api.CypherImplicits
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
abstract class BaseTestSuite
  extends FunSuite
  with Matchers
  with CypherImplicits

