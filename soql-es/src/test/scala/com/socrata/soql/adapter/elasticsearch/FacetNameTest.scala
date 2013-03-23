package com.socrata.soql.adapter.elasticsearch

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.socrata.es.facet.StatisticalFacetType

class FacetNameTest extends FunSuite with MustMatchers {

  test("recognize system columns") {

    val groupCol = "name"
    val aggCol = ":id"
    val facetName = "fcs:name::id"

    val facet = FacetName(StatisticalFacetType, groupCol, aggCol)
    facet.toString() must equal(facetName)
    (facetName match {
      case FacetName(fc) => (fc.groupKey -> fc.groupValue)
      case _ => throw new Exception("fail")
    }) must equal( groupCol -> aggCol )
  }
}
