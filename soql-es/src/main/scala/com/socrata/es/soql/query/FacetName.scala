package com.socrata.es.soql.query

import com.socrata.es.facet.{ColumnsFacetType, StatisticalFacetType, FacetType}

class FacetName(val facetType: FacetType, val groupKey: String, val groupValue: String) {

  override def toString() = "fc%s:%s:%s".format(facetType.name, groupKey, groupValue)

  def isMultiColumn() = groupKey == ColumnsFacetType.groupKey

  def isStatistical() = groupKey == StatisticalFacetType.groupKey
}

object FacetName {

  private val rx = """fc([a-z]+):(:?[^:]+):(:?[^:]+)""".r // allow column to start with : for handling system column names.

  def apply(t: FacetType, k: String, v: String) = new FacetName(t, k, v)

  def unapply(composedName: String): Option[FacetName] = {
    composedName match {
      case rx(t, k, v) => Option(new FacetName(FacetType(t), k, v))
      case _ => None
    }
  }
}
