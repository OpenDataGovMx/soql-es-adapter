package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed._
import com.socrata.soql.functions.{SoQLFunctions, MonomorphicFunction}
import annotation.tailrec

/** *
  * Many times, stores can only handle expressions where lhs is column and rhs is literal.
  * If the child expressions are compound or column to column, a different coarse of action is needed.
  * This class helps recognize such simple usage pattern.
  */
class SimpleColumnLiteralExpression[T](val col: CoreExpr[T], val lit: CoreExpr[T], val lit2: Option[CoreExpr[T]], val lhsIsColumn: Boolean)

object SimpleColumnLiteralExpression {

  def unapply[T](exprs: Seq[CoreExpr[T]]): Option[SimpleColumnLiteralExpression[T]] = {

    val colsLitsCpounds = partitionExprs[T](exprs)
    if (isSimpleColumnLiteral(colsLitsCpounds)) {
      val lit2 = colsLitsCpounds._2 match {
        case h :: h2 :: _ => Some(h2)
        case _ => None
      }
      Some(new SimpleColumnLiteralExpression[T](
        colsLitsCpounds._1.head, colsLitsCpounds._2.head, lit2, exprs.head.isInstanceOf[ColumnRef[_]]))
    } else {
      None
    }
  }

  private def isSimpleColumnLiteral[T](exprs: Tuple3[Seq[CoreExpr[T]], Seq[CoreExpr[T]], Seq[CoreExpr[T]]]) = {
    exprs._1.size == 1 && exprs._2.size >= 1 && exprs._3.isEmpty
  }

  @tailrec
  private def considerLiteral[T](expr: CoreExpr[T]): Boolean = {
    expr match {
      case _ : TypedLiteral[_] => true
      case FunctionCall(MonomorphicFunction(SoQLFunctions.TextToFloatingTimestamp, _), arg :: Nil) =>
        considerLiteral(arg)
      case FunctionCall(MonomorphicFunction(SoQLFunctions.TextToFixedTimestamp, _), arg :: Nil) =>
        considerLiteral(arg)
      case _ => false
    }
  }

  /**
   * Partition expression into 3 types - columns, literal and compounds.
   */
  private def partitionExprs[T](exprs: Seq[CoreExpr[T]]) = {

    val (cols, nonCols) = exprs.partition(_.isInstanceOf[ColumnRef[_]])
    val (lits, cpounds) = nonCols.partition(considerLiteral(_))
    Tuple3(cols, lits, cpounds)
  }
}
