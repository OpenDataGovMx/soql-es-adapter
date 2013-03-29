package com.socrata.es.facet.parse

import com.rojoma.json.io._
import com.socrata.soql.collection.OrderedMap
import com.rojoma.json.ast.{JNull, JNumber, JValue}
import com.rojoma.json.ast.JObject
import com.rojoma.json.io.EndOfArrayEvent
import com.socrata.es.soql.query.FacetName
import com.socrata.es.soql.parse.ESJsonBadParse


object TermsFacetResponseParser extends FacetResponseParser {

  import com.socrata.es.soql.parse.ESGroupingResultSet._

  override def parseEntries(
    reader: JsonReader,
    facetName: FacetName,
    groupCol: String,
    aggregateCol: String,
    acc: OrderedMap[JValue, JObject] = OrderedMap.empty,
    total: Option[Long])
    : OrderedMap[JValue, JObject] = {

    val lexer: JsonEventIterator = reader.lexer.asInstanceOf[JsonEventIterator]

    reader.lexer.head match {
      case _: EndOfArrayEvent =>
        reader.lexer.next() // consume ]
        acc
      case _ =>
        reader.read() match {
          case facet@JObject(fields) =>
            val groupKey = facet(facetGroupKey(facetName)) // group key is
            val aggFunction = facetName.groupKey.replaceFirst("_", "")
            val minOrMaxKey = s"${aggFunction}_${facetName.groupValue}"
            val minOrMaxValue = groupKey
            val groupRow = Map(
              minOrMaxKey -> minOrMaxValue,
              s"count_${facetName.groupValue}" -> JNumber(total.getOrElse(0L))
            )
            lexer.dropRestOfCompound()
            val acc2 = acc + ( JNull.asInstanceOf[JValue] -> JObject(groupRow))
            acc2
          case _ =>
            throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
        }
    }
  }
}
