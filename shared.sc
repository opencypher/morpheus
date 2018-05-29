import $file.license
import ammonite.ops._
import coursier.core.Attributes
import mill._
import mill.scalalib._

trait OkapiModule extends SbtModule {

  def scalaVersion = "2.11.12"

  def frontendVersion = "2.0.0"
  def sparkVersion = "2.2.1"

  def cats = ivy"org.typelevel::cats-core:1.0.1"

  def frontend = ivy"org.opencypher:front-end-9.1:$frontendVersion"
  def eff = ivy"org.atnos::eff:5.0.0"
  def spark = ivy"org.apache.spark::spark-core:$sparkVersion"
  def sparkSql = ivy"org.apache.spark::spark-sql:$sparkVersion"
  def sparkCatalyst = ivy"org.apache.spark::spark-catalyst:$sparkVersion"
  def neo4jDriver = ivy"org.neo4j.driver:neo4j-java-driver:1.4.2"

  def scalaTest = ivy"org.scalatest::scalatest:3.0.5"
  def scalaCheck = ivy"org.scalacheck::scalacheck:1.13.5"
  def mockito = ivy"org.mockito:mockito-all:1.10.19"
  def neo4jHarness = ivy"org.neo4j.test:neo4j-harness:3.3.3"
  def cypherTesting = ivy"org.neo4j:neo4j-cypher-util-3.4:3.4.0".configure(Attributes("test-jar", "tests"))
  def astTesting = ivy"org.opencypher:ast-9.1:$frontendVersion".configure(Attributes("test-jar", "tests"))
  def frontendTesting = ivy"org.opencypher:util-9.1:$frontendVersion".configure(Attributes("test-jar", "tests"))

  override def ivyDeps = Agg(
    ivy"org.scala-lang:scala-reflect:${scalaVersion()}"
  )

  trait OkapiTests extends Tests {

    override def ivyDeps = Agg(scalaTest)

    def testFrameworks = Seq("org.scalatest.tools.Framework")
  }

  def checkLicenses = T {
    val invalid = allSourceFiles().map(_.path).filterNot(license.License.check(_))
    val numInvalid = invalid.size
    if (numInvalid == 0) {
      T.ctx().log.info(license.License.successMsg)
    } else if (numInvalid == 1) {
      T.ctx().log.info(s"One license was invalid in file `${invalid.head.segments.last}`.")
    } else {
      T.ctx().log.info(
        s"""$numInvalid licenses were incorrect:
           |${license.License.pathsAsListItems(invalid)}""".stripMargin)
    }
  }

  def addLicenses = T {
    val added = allSourceFiles().map(_.path).filter(license.License.add(_))
    val numAdded = added.size
    if (numAdded == 0) {
      T.ctx().log.info(license.License.successMsg)
    } else if (numAdded == 1) {
      T.ctx().log.info(s"One license was added to file `${added.head.segments.last}`.")
    } else {
      T.ctx().log.info(
        s"""$numAdded licenses were added to files:
           |${license.License.pathsAsListItems(added)}""".stripMargin)
    }
  }

  // Workaround for https://github.com/lihaoyi/mill/issues/166
  override def millSourcePath = super.millSourcePath / up / moduleName

  protected lazy val moduleName: String = hyphenateCamelCase(getClass.getSimpleName.dropRight(1))

  protected def hyphenateCamelCase(s: String): String = {
    s.replaceAll("([A-Z]+)([A-Z][a-z0-9])", "$1-$2")
      .replaceAll("([a-z])([A-Z0-9])", "$1-$2")
      .toLowerCase
  }

}
