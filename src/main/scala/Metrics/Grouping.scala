package Metrics

import Types.JsonSchema.JSS

object Grouping {

  def calculateGrouping(schema: JSS, first: Boolean = true): Int = {

    val g = schema.anyOf match {
      case None =>
        schema.items match {
          case None =>
            schema.properties match {
              case None => return 0
              case Some(leaf) =>
                if(leaf.value.size == 0) // is an empty array or empty obj
                  return 0
                leaf.value.map(x => {
                  calculateGrouping(x._2,false)
                }).reduce(_+_)
            }
          case Some(item) => calculateGrouping(item.value,false)
        }
      case Some(anyOf) => anyOf.value.map(calculateGrouping(_,false)).reduce(_+_) + anyOf.value.size
    }
    if (first)
      return Math.max(1,g) // edge case where only one group exists
    else
      return g
  }

}
