package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions.{array, col, collect_list, collect_set, explode, expr, lit, struct, sum, when}
import org.apache.spark.sql.types._
import utils.Helpers._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  import spark.implicits._

  private val boundaryRules = ruleSet.getRules.filter(_.ruleType == "bounds")
  private val categoricalRules = ruleSet.getRules.filter(rule => rule.ruleType == "validNumerics" ||
    rule.ruleType == "validStrings")
  private val numericRules = ruleSet.getRules.filter(_.ruleType == "validNumerics")
  private val stringRules = ruleSet.getRules.filter(_.ruleType == "validStrings")
  private val dateTimeRules = ruleSet.getRules.filter(_.ruleType == "dateTime")
  private val complexRules = ruleSet.getRules.filter(_.ruleType == "complex")
  //  private val rulesReport = ArrayBuffer[Result]()
  //  private val validationSummaryDF: DataFrame = Seq.empty[Result].toDF
  private val byCols = ruleSet.getGroupBys map col

  case class Selects(output: Column, select: Column*)

  private def buildValidationsByType(rule: Rule): Column = {
    val nulls = mutable.Map[String, Column](
      "bounds" -> lit(null).cast(ArrayType(DoubleType)).alias("bounds"),
      "validNumerics" -> lit(null).cast(ArrayType(DoubleType)).alias("validNumerics"),
      "validStrings" -> lit(null).cast(ArrayType(StringType)).alias("validStrings"),
      "validDate" -> lit(null).cast(LongType).alias("validDate")
    )
    val validationsByType = nulls.toMap.values.toSeq
    struct(
      validationsByType: _*
    ).alias("Validation_Values")
  }

  private def buildOutputStruct(rule: Rule, results: Seq[Column]): Column = {
      struct(
        lit(rule.ruleName).alias("Rule_Name"),
        lit(rule.ruleType).alias("Rule_Type"),
        buildValidationsByType(rule),
        struct(results: _*).alias("Results")
      ).alias("Validation")
  }

  private def simplifyReport(df: DataFrame): DataFrame = {
    val summaryCols = Seq(
      col("Validations.Rule_Name"),
      col("Validations.Rule_Type"),
      col("Validations.Validation_Values"),
      col("Validations.Results.Invalid_Count"),
      col("Validations.Results.Failed")
    )
    df.select(summaryCols: _*)
  }

  // Add count of invalids by rule {rule.alias}_cnt
  private def buildBaseSelects(rules: Array[Rule]): Array[Selects] = {
    // Build base selects
    val w = Window.partitionBy(ruleSet.getGroupBys map col: _*)
    val isGrouped = ruleSet.getGroupBys.nonEmpty

    rules.map(rule => {

      // V2
      // Results must have Invalid_Count & Failed
      rule.ruleType match {
        case "bounds" =>
          val invalid = rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper
          val failed = when(col(rule.ruleName) < rule.boundaries.lower ||
            col(rule.ruleName) > rule.boundaries.upper, true)
            .otherwise(false).alias("Failed")
          val first = if (!rule.isAgg) { // Not Agg
            sum(when(invalid, 1).otherwise(0)).alias(rule.ruleName)
          } else { // Is Agg
            rule.inputColumn.alias(rule.ruleName)
          }
          val results = Seq(col(rule.ruleName).cast(LongType).alias("Invalid_Count"), failed)
          Selects(buildOutputStruct(rule, results), first)
        case x if (x == "validNumerics" || x == "validStrings") =>
          val invalid = if (x == "validNumerics") {
            expr(s"size(array_except(${rule.ruleName}," +
              s"array(${rule.validNumerics.mkString("D,")}D)))")
          } else {
            expr(s"size(array_except(${rule.ruleName}," +
              s"array('${rule.validStrings.mkString("','")}')))")
          }
          val failed = when(invalid > 0, true).otherwise(false).alias("Failed")
          // TODO -- Cardinality check and WARNING
          val first = collect_set(rule.inputColumn).alias(rule.ruleName)
          val results = Seq(invalid.cast(LongType).alias("Invalid_Count"), failed)
          Selects(buildOutputStruct(rule, results), first)
      }


//      // Remove these
//      val isAgg = rule.inputColumn.expr.prettyName == "aggregateexpression"
//      //      val isGrouped = ruleSet.getGroupBys.nonEmpty
//
//      // TODO - Try creating rules as dataframe
//      // try using grouping and unioning vs windowing
//      // FINAL OUTPUT is mixing the data
//      rule.ruleType match {
//        case "bounds" =>
//          val invalid = rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper
//          // first
//          val first = if (isAgg && !isGrouped) {
//            rule.inputColumn.alias(s"${rule.ruleName}_agg_value")
//          } else if (isAgg && isGrouped) {
//            rule.inputColumn.over(w).alias(s"${rule.ruleName}_agg_value")
//          } else if (!isAgg && isGrouped) {
//            sum(when(invalid, 1)
//              .otherwise(0)).over(w).alias(s"${rule.ruleName}_count")
//          } else {
//            sum(when(invalid, 1)
//              .otherwise(0)).alias(s"${rule.ruleName}_count")
//          }
//          // second
//          val second = if (isAgg) {
//            when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
//              col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
//              .otherwise(false).alias(s"${rule.ruleName}_hasInvalids")
//          } else {
//            col(s"${rule.ruleName}_count").cast(LongType).alias(s"${rule.ruleName}_count")
//          }
//          val third = if (isAgg) {
//            struct(Seq(
//              lit(rule.ruleName).alias("RuleName"),
//              lit(rule.ruleType).alias("RuleType"),
//              when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
//                col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
//                .otherwise(false).alias("Failed")) ++ buildNulls(Seq("Failed")): _*)
//              .alias(rule.ruleName)
//          } else {
//            struct(Seq(
//              lit(rule.ruleName).alias("RuleName"),
//              lit(rule.ruleType).alias("RuleType"),
//              col(s"${rule.ruleName}_count").cast(LongType).alias("Failed_Count")) ++
//              buildNulls(Seq("Failed_Count")): _*)
//              .alias(rule.ruleName)
//          }
//          Selects(first, second)
//        case "validNumerics" => //validNumerics
//          // first
//          val first = if (!isGrouped) {
//            rule.calculatedColumn.alias(s"${rule.ruleName}_agg_value")
//          } else {
//            rule.calculatedColumn.over(w).alias(s"${rule.ruleName}_agg_value")
//          }
//          // second
//          val second = expr(s"array_except(${rule.ruleName}_agg_value," +
//            s"array(${rule.validNumerics.valid.mkString("D,")}D))").alias(s"Invalid_${rule.canonicalColName}s")
//          val third = struct(
//            lit(rule.ruleType).alias("RuleType"),
//            col(s"Invalid_${rule.canonicalColName}s").alias(s"Invalid_${rule.inputColumnName}s"),
//            expr(s"size(Invalid_${rule.canonicalColName}s) > 0").alias(s"${rule.ruleName}_Failed"),
//            expr(s"size(Invalid_${rule.canonicalColName}s)")
//              .cast(LongType).alias(s"${rule.ruleName}_Failed_Count")
//          ).alias(rule.ruleName)
//          Selects(first, second, third)
//      }
    })
  }

//  private def genSummaryDF(detailDF: DataFrame): DataFrame = {
//
//    val summaryCols = Array("RuleName", "RuleType", "Failed", "Failed_Count")
//    val dfCs = detailDF.columns.diff(ruleSet.getGroupBys)
//
//    val baseSummary = summaryCols.foldLeft(ArrayBuffer[Column]()) {
//      case (outs, summaryCol) =>
//        val colsByOut = dfCs.map(dfc => {
//          val bounds = col(s"$dfc.RuleType") === "bounds"
//          summaryCol match {
//            case "RuleName" => lit(dfc).alias("Rule")
//            case "RuleType" => col(s"$dfc.RuleType")
//            case "Failed" => when(bounds, col(s"$dfc.Failed")).alias("Failed")
//            case "Failed_Count" => when(bounds, col(s"$dfc.Failed_Count")).alias("Failed_Count")
//          }
//        })
//        outs += array(colsByOut: _*).alias(summaryCol)
//    }.toArray
//
//    summaryCols.foldLeft(detailDF.select(byCols ++ baseSummary: _*)) {
//      case (df, c) =>
//        df.withColumn(c, explode(col(c)))
//    }
//
//  }

  private def boundaries: DataFrame = {
    case class ColDetail(colName: String, c: Column)
    val selects = buildBaseSelects(boundaryRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.select.head): _*)
        .select(explode(array(
          selects.map(_.output): _*
        )).alias("Validations"))
    } else {
      ruleSet.getDf
        .select(byCols ++ selects.map(_.select.head): _*)
        .select(byCols ++ selects.map(_.select(1)): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }

    detailDF

  }

  private def categoricals: DataFrame = {
//    numericRules.foreach(rule => rule.setCalculatedColumn(collect_set(rule.inputColumn.cast("double"))))
    val selects = buildBaseSelects(categoricalRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.select.head): _*)
        .select(explode(array(
          selects.map(_.output): _*
        )).alias("Validations"))
    } else {
      ruleSet.getDf
        .select(byCols ++ selects.map(_.select.head): _*)
        .select(byCols ++ selects.map(_.select(1)): _*)
        .select(byCols ++ selects.map(_.select(2)): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }
    //
    //    val summaryStructs = getSummaryStructs(detailDF)
    //    val summaryDF = detailDF
    //      .withColumn("kvs", array(summaryStructs: _*))
    //      .select(ruleSet.getGroupBys.map(col) :+ explode(col("kvs")).alias("_kvs"): _*)
    //      .select(ruleSet.getGroupBys.map(col) ++
    //        Seq(col("_kvs.Rule")) ++
    //        summaryOutputsByType.values.map(colName => col(s"_kvs.$colName")): _*)
    //      .withColumn("Validation_Failed",
    //        when('Failed_Count > 0, true)
    //        .when('Failed_Count === 0, false).otherwise('Validation_Failed))

    //    val uniqueSets = numericRules
    //      .map(rule => {
    //        collect_set(rule.inputColumn.cast("double")).alias(s"${rule.canonicalColName}s")
    //      })

    //    val dfWSets = ruleSet.getDf.groupBy(ruleSet.getGroupBys map col: _*)
    //      .agg(uniqueSets.head, uniqueSets.tail: _*)
    //
    //    val detailDF = numericRules.foldLeft(dfWSets) {
    //      case (df, rule) =>
    //        df.withColumn("Rule", lit(rule.ruleName))
    //          .withColumn(s"invalid_${rule.inputColumnName}s",
    //                expr(s"array_except(${rule.canonicalColName}s," +
    //                  s"array(${rule.validNumerics.valid.mkString("D,")}D))"))
    //    }
    //    val metaDF = sc.parallelize(boundaryRules.map(rule =>
    //      (rule.ruleName,
    //        rule.ruleType,
    //        Array(rule.boundaries.lower, rule.boundaries.upper)
    //      )
    //    )).toDF("Rule", "Rule_Type", "Valid")
    //
    //    detailDF.withColumn("Rule", lit(rule.ruleName))
    //      .withColumn("Validation_Failed",
    //        expr(s"size(invalid_${rule.inputColumnName}s) > 0"))
    //      .withColumn("Failed_Count", expr(s"size(invalid_${rule.inputColumnName}s)"))
    //      .withColumn("Rule_Type", lit(rule.ruleType))
    //      .withColumn("Valid", lit(rule.validNumerics.valid))
    //
    detailDF //.select(col("Valid_Stores.Invalid_store_ids"))
  }

  private def validateStringRules: Unit = ???

  private def validatedateTimeRules: Unit = ???

  private def validateComplexRules: Unit = ???

  private def unifySummary: Unit = ???

  private[validation] def validate: DataFrame = {

    //    if (boundaryRules.nonEmpty) validateBoundaryRules()
    //    if (.nonEmpty)

//    if (ruleSet.getRules.exists(_.ruleType == "bounds")) boundaries
//    if (ruleSet.getRules.exists(_.ruleType == "validNumerics")) categoricals
    simplifyReport(boundaries.unionByName(categoricals))
//    else sc.parallelize(Seq((1, 2, 3))).toDF
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}