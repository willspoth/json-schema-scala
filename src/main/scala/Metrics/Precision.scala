package Metrics

import Types.JsonSchema.{Arr, Bool, JSS, JsonSchemaType, Null, Num, Obj, Str}

object Precision {

  // TODO for global precision first strip required fields whose lineage isn't also required
  def calculatePrecision(schema: JSS): BigInt = {
    val requiredSet: Set[String] = schema.required match {
      case Some(v) => v.value
      case None => Set[String]()
    }

    schema.anyOf match {
      case None =>
        schema.items match {
          case None =>
            schema.properties match {
              case None => return BigInt(1) // leaf
              case Some(leaf) =>
                leaf.value.map(x => {
                  calculatePrecision(x._2) * (if(requiredSet.contains(x._1)) BigInt(1) else BigInt(2))
                }).reduce(_*_)
            }
          case Some(item) => calculatePrecision(item.value)
        }
      case Some(anyOf) => anyOf.value.map(calculatePrecision(_)).reduce(_+_)
    }

  }

}
