package org.opencypher.caps.test.fixture

trait TestDataFixture {

  def dataFixture: String

  def nbrNodes: Int
  def nbrRels: Int
}
