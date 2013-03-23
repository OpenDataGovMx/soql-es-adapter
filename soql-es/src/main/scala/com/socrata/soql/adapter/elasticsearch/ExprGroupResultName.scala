package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed.{CoreExpr, FunctionCall, ColumnRef}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.es.facet.StatisticalFacet

/**
 * Convert expression to naming style we use in simple grouping queries
 * that do not use scripted expressions.
 */
trait ExprGroupResultName[T] {

  def toName: String
}

object ExprGroupResultName {

  implicit def exprToGroupResultName(expr: CoreExpr[_]): ExprGroupResultName[_] = {
    expr match {
      case x: ColumnRef[_] => colRefToGroupResultName(x)
      case x: FunctionCall[_] => funCallToGroupResultName(x)
      case u =>
        throw new Exception(s"Simple group result name should not contain expr ${u}")
    }
  }

  implicit def colRefToGroupResultName(x: ColumnRef[_]) = new ExprGroupResultName[ColumnRef[_]] {
    def toName: String = x.column.name
  }

  implicit def funCallToGroupResultName(x: FunctionCall[_]) = new ExprGroupResultName[FunctionCall[_]] {
    def toName: String = {

      x.function.name match {
        case SoQLFunctions.CountStar.name =>
          s"${SoQLFunctions.Count.name.name}_${StatisticalFacet.CountStar}"
        case _ =>
          val arg: String = exprToGroupResultName(x.parameters.head).toName
          s"${x.function.name.name}_${arg}"
      }
    }
  }
}
