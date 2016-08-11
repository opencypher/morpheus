package org.opencypher.spark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class StdTestSuite extends FunSuite with Matchers with TestSessionSupport

