package com.socrata.es.facet.parse

import com.rojoma.json.io._
import com.socrata.soql.collection.OrderedMap
import com.rojoma.json.ast.{JNull, JValue}
import com.rojoma.json.ast.JObject
import com.socrata.es.soql.query.FacetName
import com.socrata.es.soql.parse.ESGroupingResultSet


object StatisticalFacetResponseParser extends FacetResponseParser {

  override def parseFacet(reader: JsonReader, facet: FacetName, facetType: String, total: Option[Long])
    : Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    val aggregateCol = facet.groupValue
    reader.read() match {
      case jo@JObject(map) =>
        val groupRow = map.collect {
          case (k, v) if (ESGroupingResultSet.aggregateFnMap.contains(k)) =>
            (ESGroupingResultSet.aggregateFnMap(k) + "_" + aggregateCol, v)
        }
        val oneAndOnlyGroupRow = OrderedMap(JNull.asInstanceOf[JValue] -> JObject(groupRow))
        (Some(1L), oneAndOnlyGroupRow)
      case unk =>
        throw new Exception("cannot parse statistical facet")
    }
  }
}
