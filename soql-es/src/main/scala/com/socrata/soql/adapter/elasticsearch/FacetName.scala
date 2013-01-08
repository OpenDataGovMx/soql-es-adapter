package com.socrata.soql.adapter.elasticsearch

class FacetName(val groupKey: String, val groupValue: String) {

  override def toString() = "fc:%s:%s".format(groupKey, groupValue)

  def isMultiColumn() = groupKey == "_multi"
}

object FacetName {

  private val rx = """fc:(.+):(.+)""".r

  def apply(k: String, v: String) = new FacetName(k, v)

  def unapply(composedName: String): Option[FacetName] = {
    composedName match {
      case rx(k, v) => Option(new FacetName(k, v))
      case _ => None
    }
  }
}
