package com.socrata.es.facet.response

import com.rojoma.json.io._
import com.socrata.soql.adapter.elasticsearch.FacetName
import com.socrata.soql.collection.OrderedMap
import com.rojoma.json.ast.{JNull, JValue}
import com.socrata.soql.adapter.elasticsearch.ESGroupingResultSet._
import com.rojoma.json.ast.JObject


object StatisticalFacetResponseParser extends FacetResponseParser {

  override def parseFacet(reader: JsonReader, facet: FacetName, facetType: String, total: Option[Long])
    : Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    val aggregateCol = facet.groupValue
    reader.read() match {
      case jo@JObject(map) =>
        val groupRow = map.collect {
          case (k, v) if (aggregateFnMap.contains(k)) =>
            (aggregateFnMap(k) + "_" + aggregateCol, v)
        }
        val oneAndOnlyGroupRow = OrderedMap(JNull.asInstanceOf[JValue] -> JObject(groupRow))
        (Some(1L), oneAndOnlyGroupRow)
      case unk =>
        throw new Exception("cannot parse statistical facet")
    }
  }
}
