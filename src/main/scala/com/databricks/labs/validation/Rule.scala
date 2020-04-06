package com.databricks.labs.validation

import java.util.UUID
import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.Column

class Rule {

  private var _ruleName: String = _
  private var _canonicalCol: Column = _
  private var _canonicalColName: String = _
  private var _inputCol: Column = _
  private var _inputColName: String = _
  private var _calculatedColumn: Column = _
  private var _boundaries: Bounds = _
  private var _validNumerics: Array[Double] = _
  private var _validStrings: Array[String] = _
  private var _dateTimeLogic: Column = _
  private var _ruleType: String = _
  private var _isAgg: Boolean = _

  private def setRuleName(value: String): this.type = {
    _ruleName = value
    this
  }

  private def setColumn(value: Column): this.type = {
    _inputCol = value
    _inputColName = _inputCol.expr.toString().replace("'", "")
    val cleanUUID = UUID.randomUUID().toString.replaceAll("-", "")
    _canonicalColName = s"${_inputColName}_$cleanUUID"
    _canonicalCol = _inputCol.alias(_canonicalColName)
    _calculatedColumn = _inputCol
    this
  }

  private[validation] def setCalculatedColumn(value: Column): Unit = {
    _calculatedColumn = value
  }

  private def setBoundaries(value: Bounds): this.type = {
    _boundaries = value
    this
  }

  private def setValidNumerics(value: Array[Double]): this.type = {
    _validNumerics = value
    this
  }

  private def setValidStrings(value: Array[String]): this.type = {
    _validStrings = value
    this
  }

  private def setDateTimeLogic(value: Column): this.type = {
    _dateTimeLogic = value
    this
  }

  private def setRuleType(value: String): this.type = {
    _ruleType = value
    this
  }

  private def setIsAgg: this.type = {
    _isAgg = inputColumn.expr.prettyName == "aggregateexpression"
    this
  }

  def ruleName: String = _ruleName

  def inputColumn: Column = _inputCol

  def inputColumnName: String = _inputColName

  def canonicalCol: Column = _canonicalCol

  def canonicalColName: String = _canonicalColName

  private[validation] def calculatedColumn: Column = _calculatedColumn

  def boundaries: Bounds = _boundaries

  def validNumerics: Array[Double] = _validNumerics

  def validStrings: Array[String] = _validStrings

  def dateTimeLogic: Column = _dateTimeLogic

  def ruleType: String = _ruleType

  def isAgg: Boolean = _isAgg

}

object Rule {

  def apply(
             ruleName: String,
             column: Column,
             boundaries: Bounds
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBoundaries(boundaries)
      .setRuleType("bounds")
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics)
      .setRuleType("validNumerics")
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setRuleType("validNumerics")
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setRuleType("validNumerics")
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validStrings: Array[String]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidStrings(validStrings)
      .setRuleType("validStrings")
      .setIsAgg
  }
  // TODO -- Implement Date/Time Logic for:
  //     Column Type (i.e. current_timestamp and current_date)
  //     java.util.Date
  //     Validated strings compatible with Spark

  //  def apply(
  //             ruleName: String,
  //             column: Column,
  //             dateTimeLogic: ???,
  //           ): Rule = {
  //
  //    new Rule()
  //      .setRuleName(ruleName)
  //      .setColumn(column)
  //      .setAggFunc(aggFunc)
  //      .setAlias(alias)
  //      .setDateTimeLogic(dateTimeLogic)
  //      .setRuleType("dateTime")
  //      .setByCols(by)
  //  }

}
