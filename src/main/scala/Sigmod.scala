import java.io.FileWriter

import Types.JsonSchema.JSS
import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.jackson.JsonMethods._
import util.CMDLineParser

import scala.collection.mutable
import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

//{"payload": {"action": "created", "comment": {"body": "Thank you! /cc @marco-streng", "url": "https://api.github.com/repos/prantlf/timezone-support/issues/comments/500497874", "created_at": "2019-06-10T17:07:49Z", "author_association": "NONE", "html_url": "https://github.com/prantlf/timezone-support/pull/9#issuecomment-500497874", "updated_at": "2019-06-10T17:07:49Z", "node_id": "MDEyOklzc3VlQ29tbWVudDUwMDQ5Nzg3NA==", "user": {"following_url": "https://api.github.com/users/danielbayerlein/following{/other_user}", "events_url": "https://api.github.com/users/danielbayerlein/events{/privacy}", "avatar_url": "https://avatars2.githubusercontent.com/u/457834?v=4", "url": "https://api.github.com/users/danielbayerlein", "gists_url": "https://api.github.com/users/danielbayerlein/gists{/gist_id}", "html_url": "https://github.com/danielbayerlein", "subscriptions_url": "https://api.github.com/users/danielbayerlein/subscriptions", "node_id": "MDQ6VXNlcjQ1NzgzNA==", "repos_url": "https://api.github.com/users/danielbayerlein/repos", "received_events_url": "https://api.github.com/users/danielbayerlein/received_events", "gravatar_id": "", "starred_url": "https://api.github.com/users/danielbayerlein/starred{/owner}{/repo}", "site_admin": false, "login": "danielbayerlein", "type": "User", "id": 457834, "followers_url": "https://api.github.com/users/danielbayerlein/followers", "organizations_url": "https://api.github.com/users/danielbayerlein/orgs"}, "id": 500497874, "issue_url": "https://api.github.com/repos/prantlf/timezone-support/issues/9"}, "issue": {"labels": [], "number": 9, "assignee": null, "repository_url": "https://api.github.com/repos/prantlf/timezone-support", "closed_at": "2019-06-10T16:02:58Z", "id": 427185931, "title": "Make parser be aware of \"Z\" character as timezone offset", "pull_request": {"url": "https://api.github.com/repos/prantlf/timezone-support/pulls/9", "diff_url": "https://github.com/prantlf/timezone-support/pull/9.diff", "html_url": "https://github.com/prantlf/timezone-support/pull/9", "patch_url": "https://github.com/prantlf/timezone-support/pull/9.patch"}, "comments": 9, "state": "closed", "body": "", "events_url": "https://api.github.com/repos/prantlf/timezone-support/issues/9/events", "labels_url": "https://api.github.com/repos/prantlf/timezone-support/issues/9/labels{/name}", "author_association": "CONTRIBUTOR", "comments_url": "https://api.github.com/repos/prantlf/timezone-support/issues/9/comments", "html_url": "https://github.com/prantlf/timezone-support/pull/9", "updated_at": "2019-06-10T17:07:49Z", "node_id": "MDExOlB1bGxSZXF1ZXN0MjY1ODcxNzYy", "user": {"following_url": "https://api.github.com/users/lucifurtun/following{/other_user}", "events_url": "https://api.github.com/users/lucifurtun/events{/privacy}", "avatar_url": "https://avatars2.githubusercontent.com/u/3864555?v=4", "url": "https://api.github.com/users/lucifurtun", "gists_url": "https://api.github.com/users/lucifurtun/gists{/gist_id}", "html_url": "https://github.com/lucifurtun", "subscriptions_url": "https://api.github.com/users/lucifurtun/subscriptions", "node_id": "MDQ6VXNlcjM4NjQ1NTU=", "repos_url": "https://api.github.com/users/lucifurtun/repos", "received_events_url": "https://api.github.com/users/lucifurtun/received_events", "gravatar_id": "", "starred_url": "https://api.github.com/users/lucifurtun/starred{/owner}{/repo}", "site_admin": false, "login": "lucifurtun", "type": "User", "id": 3864555, "followers_url": "https://api.github.com/users/lucifurtun/followers", "organizations_url": "https://api.github.com/users/lucifurtun/orgs"}, "milestone": null, "locked": false, "url": "https://api.github.com/repos/prantlf/timezone-support/issues/9", "created_at": "2019-03-29T21:31:37Z", "assignees": []}}, "created_at": "2019-06-10T17:07:49Z", "actor": {"url": "https://api.github.com/users/danielbayerlein", "display_login": "danielbayerlein", "avatar_url": "https://avatars.githubusercontent.com/u/457834?", "gravatar_id": "", "login": "danielbayerlein", "id": 457834}, "id": "9792328339", "repo": {"url": "https://api.github.com/repos/prantlf/timezone-support", "id": 145323192, "name": "prantlf/timezone-support"}, "type": "IssueCommentEvent", "public": true}

//java -Xmx10g -Xss10m -jar JsonSchemaSigmod.jar yelpUSFixed.log yelp.conf
object Sigmod {
  def jsonToMap(jsonStr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.DEBUG)
//    Logger.getLogger("akka").setLevel(Level.DEBUG)
    Logger.getLogger(Metrics.Validation.getClass).setLevel(Level.OFF)

    // logFile
    val logIter = Source.fromFile(args(0)).getLines()
    val outputFile = new FileWriter(args(0)+".res",true)
    val configFile: String = args(1)

    var forcedInputFile: Option[String] = None
    if(args.size == 3){
      forcedInputFile = Some(args(2))
    }

    val spark = CMDLineParser.createSparkSession(Some(configFile))

    val h: Configuration = spark.sparkContext.hadoopConfiguration
    h.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    h.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    while(logIter.hasNext){
      val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
      val info: Map[String,String] = jsonToMap(logIter.next())
      val schema: JSS = JsonSchemaParser.jsFromString(logIter.next())

      val inputFile: String = forcedInputFile.getOrElse(info.get("inputFile").get)
      val totalTime: String = info.get("TotalTime").get
      val trainPrecent: Double = info.get("TrainPercent").get.toDouble
      val validationSize: Int = info.get("ValidationSize").get.toInt
      val seed: Int = info.get("Seed").get.toInt

      val (train,validation) = CMDLineParser.split(spark,inputFile,trainPrecent,validationSize,Some(seed))


      log += LogOutput("inputFile",inputFile,"inputFile: ")
      log += LogOutput("TotalTime",totalTime,"TotalTime: ")

      log += LogOutput("TrainPercent",trainPrecent.toString,"TrainPercent: ")
      log += LogOutput("TrainSizeActual",train.count().toString(),"TrainSizeActual: ")
      log += LogOutput("ValidationSize",validationSize.toString,"ValidationSize: ")
      log += LogOutput("ValidationSizeActual",validation.count().toString,"ValidationSizeActual: ")
      log += LogOutput("Seed",seed.toString,"Seed: ")

      log += LogOutput("Precision",Metrics.Precision.calculatePrecision(schema).toString(),"Precision: ")
      val validationInfo = Metrics.Validation.calculateValidation(schema,validation)
      log += LogOutput("Validation",(validationInfo._1/validationInfo._3).toString(),"Validation: ")
      log += LogOutput("Saturation","["+validationInfo._2.mkString(",")+"]","Saturation: ")
      log += LogOutput("Grouping",Metrics.Grouping.calculateGrouping(schema).toString(),"Grouping: ")

      outputFile.write("{" + log.map(_.toJson).mkString(",") + "}\n")
      println(log.map(_.toString).mkString("\n"))
    }

    outputFile.close()
  }

  case class LogOutput(label:String, value:String, printPrefix:String, printSuffix:String = ""){
    override def toString: String = s"""${printPrefix}${value}${printSuffix}"""
    def toJson: String = s""""${label}":"${value}""""
  }
}
