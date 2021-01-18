import java.io.FileWriter

import JsonParser.JsonSchemaParser
import Types.JsonSchema.JSS
import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.jackson.JsonMethods._
import util.CMDLineParser

import scala.collection.mutable
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import util.Utils.jsonToMap


//java -Xmx10g -Xss10m -jar json-schema.jar yelpUSFixed.log yelp.conf
//../extractor/schemas/github.*.log ../extractor/schemas/*.groundtruth.log -d
object Main {

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger(Metrics.Validation.getClass).setLevel(Level.OFF)

    // logFile
    val configFile: Option[String] = if(args.filterNot(_.equals("-p")).size > 1) Some(args(1)) else None

    val validate: Boolean = !args.contains("-p")
    val distance: Boolean = args.contains("-d")

    if(distance) { // exits if true
      Metrics.Distance.calcDistance(args(0), args(1), if(!args(2).equals("-d")) args(2) else "symmetric")
      sys.exit(0)
    }

    val logIter = Source.fromFile(args(0)).getLines()
    val outputFile = new FileWriter(args(0)+".res",true)
    val spark = CMDLineParser.createSparkSession(configFile)

    while(logIter.hasNext){
      val log: mutable.ListBuffer[LogOutput] = mutable.ListBuffer[LogOutput]()
      val info: Map[String,String] = jsonToMap(logIter.next())
      val schema: JSS = JsonSchemaParser.jsFromString(logIter.next())
      info.get("TrainPercent") match {
        case Some(v) =>
          if(v.equals("1.0")){
            val inputFile: String = info.get("inputFile").get
            val totalTime: String = info.get("TotalTime").get
            val trainPrecent: Double = info.get("TrainPercent").get.toDouble
            val validationSize: Int = info.get("ValidationSize").get.toInt
            val seed: Option[Int] = info.get("Seed") match {
              case Some(s) => if(s.equals("None")) None else Some(s.toInt)
              case None => None
            }

            if(validate){

              val (_,validation): (RDD[String],RDD[String]) = CMDLineParser.split(spark,inputFile,trainPrecent,validationSize,seed,None)


              val validationInfo = Metrics.Validation.calculateValidation(schema,validation)
              log += LogOutput("ValidationSizeActual",validation.count().toString,"ValidationSizeActual: ")
              log += LogOutput("Validation",(validationInfo._1/validationInfo._2).toString(),"Validation: ")
              val totalWrong = validationInfo._2-validationInfo._1
              log += LogOutput("RequiredEdits",(validationInfo._3).toString(),"Required Edits: ")
              log += LogOutput("TotalWrong",(totalWrong).toString(),"Total Wrong Rows: ")
            }

            log += LogOutput("inputFile",inputFile,"inputFile: ")
            log += LogOutput("TotalTime",totalTime,"TotalTime: ")

            log += LogOutput("TrainPercent",trainPrecent.toString,"TrainPercent: ")
            log += LogOutput("ValidationSize",validationSize.toString,"ValidationSize: ")
            log += LogOutput("Seed",seed.toString,"Seed: ")

            log += LogOutput("Precision",Metrics.Precision.calculatePrecision(schema).toString(),"Precision: ")

            //      log += LogOutput("Saturation","["+validationInfo._2.mkString(",")+"]","Saturation: ")
            log += LogOutput("Grouping",Metrics.Grouping.calculateGrouping(schema).toString(),"Grouping: ")
            log += LogOutput("BaseSchemaSize",Metrics.Grouping.calculateBaseSchemaSize(schema).toString(),"Base Schemas Size: ")

            outputFile.write("{" + log.map(_.toJson).mkString(",") + "}\n")
            outputFile.flush()
            println(log.map(_.toString).mkString("\n"))
          }
        case None =>
      }
    }

    outputFile.close()
  }

  case class LogOutput(label:String, value:String, printPrefix:String, printSuffix:String = ""){
    override def toString: String = s"""${printPrefix}${value}${printSuffix}"""
    def toJson: String = s""""${label}":"${value}""""
  }
}
