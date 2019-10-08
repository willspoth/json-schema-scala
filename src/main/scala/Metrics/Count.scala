package Metrics

import Types.JsonSchema.JSS

object Count {

  def countAttributes(schema: JSS): BigInt = {
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
                //(schema.`type`.equals(Arr) && schema.maxItems.getOrElse(1.0) == 0.0) || (schema.`type`.equals(Obj) && schema.maxProperties.getOrElse(1.0) == 0.0)
                if(leaf.value.size == 0) // is an empty obj
                  return BigInt(1)
                leaf.value.map(x => {
                  countAttributes(x._2)
                }).reduce(_+_)
            }
          case Some(item) => countAttributes(item.value)
        }
      case Some(anyOf) => anyOf.value.map(countAttributes(_)).reduce(_+_)
    }

  }

}
