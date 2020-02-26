import Types.JsonSchema.JSS
import util.CMDLineParser

object SparkMain {

  def main(args: Array[String]): Unit = {

    val config = CMDLineParser.readArgs(args)

    val schema: JSS = JsonSchemaParser.jsFromFile(config.schemaFileName)

    println("Precision: "+ Metrics.Precision.calculatePrecision(schema).toString())
    println("Grouping: "+ Metrics.Grouping.calculateGrouping(schema).toString())
    println("Validation: "+ Metrics.Validation.calculateValidation(schema,config.spark.sparkContext.textFile(config.validationFileName),config.outputBad).toString())
  }

}
