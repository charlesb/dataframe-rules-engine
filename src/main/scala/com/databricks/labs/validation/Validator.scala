package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.Result
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions.{col, sum, when}
import utils.Helpers._

import scala.collection.mutable.ArrayBuffer

class Validator(ruleSet: RuleSet) extends SparkSessionWrapper {

  import spark.implicits._

  private val boundaryRules = ruleSet.getRules.filter(_.ruleType == "bounds")
  private val numericRules = ruleSet.getRules.filter(_.ruleType == "validNumerics")
  private val stringRules = ruleSet.getRules.filter(_.ruleType == "validStrings")
  private val dateTimeRules = ruleSet.getRules.filter(_.ruleType == "dateTime")
  private val complexRules = ruleSet.getRules.filter(_.ruleType == "complex")
  private val rulesReport = ArrayBuffer[Result]()
  private val validationSummaryDF: DataFrame = Seq.empty[Result].toDF
  case class Selects(baseSelects: Column, topSelects: Column)

  //  private def genAggCol(rule: Rule): Column = {
  //    val funcRaw = rule.aggFunc.get.apply(rule.inputColumn).toString()
  //    val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
  //    rule.aggFunc.get(rule.inputColumn).cast("double")
  //      .alias(s"${getColumnName(rule.inputColumn)}_${funcName}")
  //    }

  // Add count of invalids by rule {rule.alias}_cnt
  private def getSelects(rules: Array[Rule]): Array[Selects] = {
    val w = Window.partitionBy(ruleSet.getGroupBys map col: _*)
    rules.map(rule => {
      val isAgg = rule.inputColumn.expr.prettyName == "aggregateexpression"
      val isGrouped = ruleSet.getGroupBys.nonEmpty

      val baseSelects = if (isAgg && !isGrouped) {
        rule.inputColumn.alias(s"${rule.ruleName}_agg_value")
      } else if (isAgg && isGrouped) {
        rule.inputColumn.over(w).alias(s"${rule.ruleName}_agg_value")
      } else if (!isAgg && isGrouped) {
        sum(when(rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper, 1)
          .otherwise(0)).over(w).alias(s"${rule.ruleName}_count")
      } else {
        sum(when(rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper, 1)
          .otherwise(0)).alias(s"${rule.ruleName}_count")
      }

      val topSelects = if (isAgg) {
        when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
          col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
          .otherwise(false).alias(s"${rule.ruleName}_containsInvalid")
      } else {
        col(s"${rule.ruleName}_count")
      }
      Selects(baseSelects, topSelects)
    })
  }

  private def boundsDetailDF(): DataFrame = {

    val selects = getSelects(boundaryRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.baseSelects): _*)
        .select(selects.map(_.topSelects): _*)
    } else {
      val byCols = ruleSet.getGroupBys map col
      ruleSet.getDf
        .select(byCols ++ selects.map(_.baseSelects): _*)
        .select(byCols ++ selects.map(_.topSelects): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }

    // TODO - Build Summart DF

    //    boundaryRules.filter(_.aggFunc.nonEmpty).foreach(rule => {
    //      val funcRaw = rule.aggFunc.get.apply(rule.inputColumn).toString()
    //      val funName = funcRaw.substring(0, funcRaw.indexOf("("))
    //      val actualValue = aggResults.getDouble(aggResults.fieldIndex(rule.alias))
    //      rulesReport.append(
    //        Result(rule.ruleName, rule.alias, funName, rule.boundaries, actualValue,)
    //      )
    //    })

  }

  //  case class Result(ruleName: String, colName: String, funcname: String,
  //                    boundaries: Bounds, actualVal: Double, failedCount: Int, passed: Boolean)

  private def validateNumericRules: Unit = ???

  private def validateStringRules: Unit = ???

  private def validatedateTimeRules: Unit = ???

  private def validateComplexRules: Unit = ???

  private[validation] def validate: DataFrame = {

    //    if (boundaryRules.nonEmpty) validateBoundaryRules()
    //    if (.nonEmpty)

    boundsDetailDF()

  }

}

object Validator {
  def apply(ruleSet: RuleSet): Validator = new Validator(ruleSet)
}