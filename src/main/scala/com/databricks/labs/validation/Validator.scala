package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.Result
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions.{
  col, sum, when, lit, struct,
  array, explode, collect_set, collect_list, expr
}
import org.apache.spark.sql.types._
import utils.Helpers._

import scala.collection.mutable.ArrayBuffer

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  import spark.implicits._

  private val boundaryRules = ruleSet.getRules.filter(_.ruleType == "bounds")
  private val numericRules = ruleSet.getRules.filter(_.ruleType == "validNumerics")
  private val stringRules = ruleSet.getRules.filter(_.ruleType == "validStrings")
  private val dateTimeRules = ruleSet.getRules.filter(_.ruleType == "dateTime")
  private val complexRules = ruleSet.getRules.filter(_.ruleType == "complex")
  private val rulesReport = ArrayBuffer[Result]()
  private val validationSummaryDF: DataFrame = Seq.empty[Result].toDF

  case class Selects(select: Seq[Column]*)

  //  private def genAggCol(rule: Rule): Column = {
  //    val funcRaw = rule.aggFunc.get.apply(rule.inputColumn).toString()
  //    val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
  //    rule.aggFunc.get(rule.inputColumn).cast("double")
  //      .alias(s"${getColumnName(rule.inputColumn)}_${funcName}")
  //    }

  private def buildNulls(columsUsed: Seq[String]): Seq[Column] = {
    val allNulls = Map[String, Column](
      "Failed" -> lit(null).cast(BooleanType).alias("Failed"),
      "Invalid_Count" -> lit(null).cast(LongType).alias("Invalid_Count")
    )
    allNulls.keys.toSeq.diff(columsUsed).map(k => allNulls(k))
  }

  // Add count of invalids by rule {rule.alias}_cnt
  private def buildBaseSelects(rules: Array[Rule]): Array[Selects] = {
    // Build base selects
    val w = Window.partitionBy(ruleSet.getGroupBys map col: _*)
    rules.map(rule => {
      val isAgg = rule.inputColumn.expr.prettyName == "aggregateexpression"
      val isGrouped = ruleSet.getGroupBys.nonEmpty

      rule.ruleType match {
        case "bounds" =>
          val invalid = rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper
          // first
          val first = if (isAgg && !isGrouped) {
            Seq(rule.inputColumn.alias(s"${rule.ruleName}_agg_value"))
          } else if (isAgg && isGrouped) {
            Seq(rule.inputColumn.over(w).alias(s"${rule.ruleName}_agg_value"))
          } else if (!isAgg && isGrouped) {
            Seq(sum(when(invalid, 1)
              .otherwise(0)).over(w).alias(s"${rule.ruleName}_count"))
          } else {
            Seq(sum(when(invalid, 1)
              .otherwise(0)).alias(s"${rule.ruleName}_count"))
          }
          // second
          val second = if (isAgg) {
            Seq(struct(Seq(
              lit(rule.ruleName).alias("RuleName"),
              lit(rule.ruleType).alias("RuleType"),
              when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
                col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
                .otherwise(false).alias("Failed")) ++ buildNulls(Seq("Failed")): _*)
              .alias(rule.ruleName))
          } else {
            Seq(struct(Seq(
              lit(rule.ruleName).alias("RuleName"),
              lit(rule.ruleType).alias("RuleType"),
              col(s"${rule.ruleName}_count").cast(LongType).alias("Invalid_Count")) ++
              buildNulls(Seq("Invalid_Count")): _*)
              .alias(rule.ruleName))
          }
          Selects(first, second)
        case "validNumerics" => //validNumerics
          // first
          val first = if (!isGrouped) {
            Seq(rule.calculatedColumn.alias(s"${rule.ruleName}_agg_value"))
          } else {
            Seq(rule.calculatedColumn.over(w).alias(s"${rule.ruleName}_agg_value"))
          }
          // second
          val second = Seq(
            expr(s"array_except(${rule.ruleName}_agg_value," +
              s"array(${rule.validNumerics.valid.mkString("D,")}D))").alias(s"Invalid_${rule.canonicalColName}s")
          )
          val third = Seq(struct(
            lit(rule.ruleType).alias("RuleType"),
            col(s"Invalid_${rule.canonicalColName}s").alias(s"Invalid_${rule.inputColumnName}s"),
            expr(s"size(Invalid_${rule.canonicalColName}s) > 0").alias(s"${rule.ruleName}_Failed"),
            expr(s"size(Invalid_${rule.canonicalColName}s)")
              .cast(LongType).alias(s"${rule.ruleName}_Failed_Count")
          ).alias(rule.ruleName))
          Selects(first, second, third)
      }
    })
  }

  private val summaryOutputsByType = Map(
    "BooleanType" -> "Validation_Failed",
    "LongType" -> "Failed_Count"
  )

  private def getSummaryStructs(detailDF: DataFrame): DataFrame = {
    val colsByType = detailDF.dtypes.filter { case (c, _) => !ruleSet.getGroupBys.contains(c) }
      .groupBy(_._2).map(typeMap => typeMap._1 -> typeMap._2.map(_._1))

    val castByType = Map(
      "BooleanType" -> BooleanType,
      "IntegerType" -> IntegerType,
      "LongType" -> LongType,
      "ArrayType(DoubleType,true)" -> ArrayType(DoubleType)
    )

    //    col("tmp")
    val summaryCols = Array("RuleName", "RuleType", "Failed", "Invalid_Count")
    val dfCs = detailDF.columns

    val baseSummary = summaryCols.foldLeft(ArrayBuffer[Column]()) {
      case (outs, summaryCol) =>
        val colsByOut = dfCs.map(dfc => {
          val bounds = col(s"$dfc.RuleType") === "bounds"
          summaryCol match {
            case "RuleName" => lit(dfc).alias("Rule")
            case "RuleType" => col(s"$dfc.RuleType")
            case "Failed" => when(bounds, col(s"$dfc.Failed")).alias("Failed")
            case "Invalid_Count" => when(bounds, col(s"$dfc.Invalid_Count")).alias("Failed_Count")
          }
        })
        outs += array(colsByOut: _*).alias(summaryCol)
    }.toArray

    summaryCols.foldLeft(detailDF.select(baseSummary: _*)) {
      case (df, c) =>
        df.withColumn(c, explode(col(c)))
          .dropDuplicates("RuleName")
    }

    //
    //    detailDF.columns.flatMap(c => {
    //      val bounds = col(s"$c.RuleType") === "bounds"
    //      val boundsBoolean = bounds && col(s"$c.ReturnType") === "boolean"
    //      val boundsCount = bounds && col(s"$c.ReturnType") === "boolean"
    //
    //
    //        lit(c).alias("Rule"),
    //        when(boundsBoolean, col(s"$c.Passed")).alias("Failed"),
    //        when(boundsCount, col(s"$c.Invalid_Count")).alias("Failed_Count"),
    //        col(s"$c.RuleType")
    //    })
    //
    //
    //    colsByType.flatMap(tc => {
    //      val colType = tc._1
    //      val colsForType = tc._2
    //      colsForType.map(c => { // col of type
    //        val outputCols = summaryOutputsByType.flatMap(output => { // for output type
    //          if (colType.startsWith(output._1)) { // if col type matches output type
    //            if (colType.startsWith("ArrayType")) {
    //              Seq(
    //                c match {
    //                  case c if c.startsWith("Invalid_") => col(c).cast(ArrayType(DoubleType)).alias("Invalid_Categoricals")
    //                }
    //              )
    //            } else Seq(
    //              col(c).cast(castByType(output._1)).alias(output._2)
    //            )
    //          } else { // Create null if diff type
    //            Seq(lit(null).cast(castByType(output._1)).alias(output._2))
    //          }
    //        }).toArray
    //        struct(
    //          lit(c).alias("Rule") +: outputCols: _*
    //        )
    //      })
    //    }).toArray
  }

  private def boundsSummaryDF: DataFrame = {
    case class ColDetail(colName: String, c: Column)
    val selects = buildBaseSelects(boundaryRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.flatMap(_.select.head): _*)
        .select(selects.flatMap(_.select(1)): _*)
    } else {
      val byCols = ruleSet.getGroupBys map col
      ruleSet.getDf
        .select(byCols ++ selects.flatMap(_.select.head): _*)
        .select(byCols ++ selects.flatMap(_.select(1)): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }

    //    detailDF.select(struct(array($"Reasonable_sku_counts.RuleName")).alias("Rule"))
    //    val metaDF = sc.parallelize(boundaryRules.map(rule =>
    //      (rule.ruleName,
    //        rule.ruleType,
    //        Array(rule.boundaries.lower, rule.boundaries.upper)
    //      )
    //    )).toDF("Rule", "Rule_Type", "Valid")
    //
//    val summaryStructs = getSummaryStructs(detailDF)
//    val summaryDF = detailDF.select(summaryStructs: _*)
//      .withColumn("RuleName", explode('RuleName))
//      .withColumn("RuleType", explode('RuleType))
//      .dropDuplicates("RuleName")
    //
    //    // Can convert to map if more filters necessary
    //    val detailFilter = 'Validation_Failed === true || 'Failed_Count > 0
    //    val summaryDF = detailDF
    //      .withColumn("kvs", array(summaryStructs: _*))
    //      .select(ruleSet.getGroupBys.map(col) :+ explode(col("kvs")).alias("_kvs"): _*)
    //      .select(ruleSet.getGroupBys.map(col) ++
    //        Seq(col("_kvs.Rule")) ++
    //        summaryOutputsByType.values.map(colName => col(s"_kvs.$colName")): _*)


    //    val filteredSummaryDF = if (detailLvl <= 1) summaryDF.filter(detailFilter) else summaryDF
    //    val orderBys = if (ruleSet.getGroupBys.nonEmpty) s"Rule, ${ruleSet.getGroupBys.mkString(",")}" else "Rule"
    //
    //    filteredSummaryDF
    //      .join(metaDF, "Rule") // Add UUID to each rule and join on that
    //      .orderBy(orderBys)

        getSummaryStructs(detailDF)
    //    detailDF
//    summaryDF
  }

  //  // SELECT array_except(array(1, 2, 3), array(1, 3, 5));
  //  val ar = Array(1001, 1002).mkString(",")
  //  val x = Seq.empty[Column]
  //  display(
  //    df
  //      .groupBy(x: _*)
  //      .agg(collect_set('store_id).alias("store_ids"))
  //      .withColumn("invalid_store_ids", expr(s"array_except(store_ids, array(${ar}))"))
  //  )

  private def validateNumericRules: DataFrame = {
    numericRules.foreach(rule => rule.setCalculatedColumn(collect_set(rule.inputColumn.cast("double"))))
    val selects = buildBaseSelects(numericRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.flatMap(_.select.head): _*)
        .select(selects.flatMap(_.select(1)): _*)
        .select(selects.flatMap(_.select(2)): _*)
    } else {
      val byCols = ruleSet.getGroupBys map col
      ruleSet.getDf
        .select(byCols ++ selects.flatMap(_.select.head): _*)
        .select(byCols ++ selects.flatMap(_.select(1)): _*)
        .select(byCols ++ selects.flatMap(_.select(2)): _*)
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

    if (ruleSet.getRules.exists(_.ruleType == "bounds")) boundsSummaryDF
    //    if (ruleSet.getRules.exists(_.ruleType == "validNumerics")) validateNumericRules
    else sc.parallelize(Seq((1, 2, 3))).toDF
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}