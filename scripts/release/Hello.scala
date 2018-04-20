import io.circe.generic.auto._
import io.circe.syntax._
import org.http4s.client._
import org.http4s.client.dsl.io._
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.client.blaze._
import org.http4s.circe._
import cats.effect._
import io.circe.Json._
import io.circe.Json

object Hello extends App {

  val parser = new scopt.OptionParser[Config]("Github Release") {
    opt[String]('v',"releaseVersion")
      .required()
      .action((version, config) => config.copy(releaseVersion = version))
      .text("The version for which a GitHub release is created.")

    opt[String]('b',"branch")
      .action((branch, config) => config.copy(branch = branch))
      .text("The branch for which the tag is created. Default is `master`.")

    opt[String]('t',"githubToken")
      .required()
      .action((token, config) => config.copy(token = token))
      .text("GitHub API token.")

  }

  parser.parse(args, Config()) match {
    case Some(c) => runScript(c)
    case _ => ()
  }

  private def runScript(config: Config) = {
    val releaseVersion = config.releaseVersion
    val branch = config.branch
    val releaseName = s"CAPS Release Version $releaseVersion"
    val releaseBody =
      s"""
         |There are two released artifacts:
         |
      |- `spark-cypher-$releaseVersion`
         |Same artifact that is deployed to [Maven Central](https://search.maven.org/#artifactdetails%7Corg.opencypher%7Cspark-cypher%7C$releaseVersion%7Cjar)
         |
      |To use this release in a Maven project, add the following dependency to your pom:
         |```
         |<dependency>
         |  <groupId>org.opencypher</groupId>
         |  <artifactId>spark-cypher</artifactId>
         |  <version>$releaseVersion</version>
         |</dependency>
         |```
         |
      |and for SBT
         |```
         |libraryDependencies += "org.opencypher" % "spark-cypher" % "$releaseVersion"
         |```
         |
      |- spark-cypher-$releaseVersion-cluster
         |A fat jar contaning all dependencies except the Spark dependencies. This version can be used within existing
         |Spark infrastructure.
    """.stripMargin
    val draft = false
    val preRelease = releaseVersion.contains("-")

    require(branch != "master")

    val request = GithubReleaseCreationRequest(
      releaseVersion,
      branch,
      releaseName,
      releaseBody,
      draft,
      preRelease
    )

    val requestJson = request.asJson
    val requestUri = Uri.fromString(s"https://api.github.com/repos/opencypher/cypher-for-apache-spark/releases?access_token=${config.token}").right.get
    val req = POST(requestUri, requestJson.toString)

    val response = req.flatMap(_.as[Json]).unsafeRunSync

    println(response)
  }
}

case class Config(
  releaseVersion: String = "",
  branch: String = "master",
  token: String = ""
)

case class GithubReleaseCreationRequest(
  tag_name: String,
  target_commitish: String,
  name: String,
  body: String,
  draft: Boolean,
  prerelease: Boolean
)