import java.io.FileWriter

import Types.JsonSchema.JSS
import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.jackson.JsonMethods._
import util.CMDLineParser

import scala.collection.mutable
import scala.io.Source

//java -Xmx10g -Xss10m -jar JsonSchemaSigmod.jar yelpUSFixed.log yelp.conf
object Sigmod {
  def jsonToMap(jsonStr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

  def main(args: Array[String]): Unit = {
    // logFile
    val logIter = Source.fromFile(args(0)).getLines()
    val outputFile = new FileWriter(args(0)+".res",true)
    val configFile: String = args(1)

    val spark = CMDLineParser.createSparkSession(Some(configFile))

    val h: Configuration = spark.sparkContext.hadoopConfiguration
    h.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    h.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    while(logIter.hasNext){
      val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
      val info: Map[String,String] = jsonToMap(logIter.next())
      val schema: JSS = JsonSchemaParser.jsFromString(logIter.next())

      val inputFile: String = info.get("inputFile").get
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
      log += LogOutput("Validation",Metrics.Validation.calculateValidation(schema,validation).toString(),"Validation: ")
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
