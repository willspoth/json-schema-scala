import Types.JsonSchema.JSS
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import util.CMDLineParser
import util.CMDLineParser.{config, getClass}

import scala.io.Source

object SparkMain {

  def main(args: Array[String]): Unit = {

    val config = CMDLineParser.readArgs(args)

    val schema: JSS = JsonSchemaParser.jsFromFile(config.schemaFileName)

    println("Precision: "+ Metrics.Precision.calculatePrecision(schema).toString())
    println("Grouping: "+ Metrics.Grouping.calculateGrouping(schema).toString())
    //println("Validation: "+ Metrics.Validation.calculateValidation(schema,config.spark.sparkContext.textFile(config.validationFileName),config.outputBad).toString())
  }

  def readArgs(args: Array[String]): config = {
    if(args.size == 0) {
      println("No args provided, expected: filename\noptional args:\n\t-p only computes precision\n\t-f {filename} output to file\n\t-c {sparkconf} spark config file")
      System.exit(0)
    }

    val inputFile = args(0)
    val argMap = scala.collection.mutable.HashMap[String,Any]("-p"->true,"-f"->(inputFile+".log"))

    createSparkSession(argMap.get("-c").asInstanceOf[Option[String]])



  }

  def createSparkSession(confFile: Option[String]): SparkSession = {

    val spark: SparkSession =
      try {
        val spark_conf_file: String = confFile.getOrElse(scala.util.Properties.envOrElse("SPARK_CONF_FILE", getClass.getResource("/spark.conf").getFile))
        val lines = Source.fromFile(spark_conf_file).getLines.toList
        val conf = new SparkConf()
        val args = lines.map(_.split("#").head).filter(s => !s.contains('#') && s.length > 0).map(s => {
          (s.split("=").head.trim, s.split("=").last.trim)
        })
        conf.setAll(args)
        org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("json-schema").config(conf).getOrCreate()
      } catch {
        case _: java.lang.NullPointerException => org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("json-schema").getOrCreate()
      }

    return spark
  }

}
