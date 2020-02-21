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


//java -Xmx10g -Xss10m -jar JsonSchemaSigmod.jar yelpUSFixed.log yelp.conf
object ExplorerMain {
  def jsonToMap(jsonStr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger(Metrics.Validation.getClass).setLevel(Level.OFF)

    // logFile
    val logIter = Source.fromFile(args(0)).getLines()
    val outputFile = new FileWriter(args(0)+".res",true)
    val configFile: String = args(1)

    var numberOfRows: Option[Int] = None
    if(args.size == 3){
      numberOfRows = Some(args(2).toInt)
    }

    var forcedInputFile: Option[String] = None
    if(args.size == 4){
      forcedInputFile = Some(args(3))
    }

    val spark = CMDLineParser.createSparkSession(Some(configFile))

//    val h: Configuration = spark.sparkContext.hadoopConfiguration
//    h.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
//    h.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    while(logIter.hasNext){
      val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
      val info: Map[String,String] = jsonToMap(logIter.next())
      val schema: JSS = JsonSchemaParser.jsFromString(logIter.next())

      val inputFile: String = forcedInputFile.getOrElse(info.get("inputFile").get)
      val totalTime: String = info.get("TotalTime").get
      val trainPrecent: Double = info.get("TrainPercent").get.toDouble
      val validationSize: Int = info.get("ValidationSize").get.toInt
      val seed: Option[Int] = info.get("Seed") match {
        case Some(s) => if(s.equals("None")) None else Some(s.toInt)
        case None => None
      }

      val (train,validation) = CMDLineParser.split(spark,inputFile,trainPrecent,validationSize,seed,numberOfRows)


      log += LogOutput("inputFile",inputFile,"inputFile: ")
      log += LogOutput("TotalTime",totalTime,"TotalTime: ")

      log += LogOutput("TrainPercent",trainPrecent.toString,"TrainPercent: ")
      log += LogOutput("ValidationSize",validationSize.toString,"ValidationSize: ")
      log += LogOutput("ValidationSizeActual",validation.count().toString,"ValidationSizeActual: ")
      log += LogOutput("Seed",seed.toString,"Seed: ")

      log += LogOutput("Precision",Metrics.Precision.calculatePrecision(schema).toString(),"Precision: ")
      val validationInfo = Metrics.Validation.calculateValidation(schema,validation)
      log += LogOutput("Validation",(validationInfo._1/validationInfo._2).toString(),"Validation: ")
//      log += LogOutput("Saturation","["+validationInfo._2.mkString(",")+"]","Saturation: ")
      log += LogOutput("Grouping",Metrics.Grouping.calculateGrouping(schema).toString(),"Grouping: ")
      log += LogOutput("BaseSchemaSize",Metrics.Grouping.calculateBaseSchemaSize(schema).toString(),"Base Schemas Size: ")

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
