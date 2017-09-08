package org.opencypher.caps.api.io

class GraphSourceFactoryCompanion(val defaultScheme: String, additionalSchemes: String*) {
  val supportedSchemes: Set[String]= additionalSchemes.toSet + defaultScheme
}
