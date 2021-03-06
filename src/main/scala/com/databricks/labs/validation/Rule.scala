package com.databricks.labs.validation

import java.util.UUID

import com.databricks.labs.validation.utils.Severity
import com.databricks.labs.validation.utils.Structures.{Bounds, DateBounds}
import org.apache.spark.sql.Column

/**
 * Definition of a rule
 */
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
  private var _ruleType: RuleType.Value = _
  private var _isAgg: Boolean = _
  private var _severity: String = _
  private var _blank: Boolean = _
  private var _dateBounds: DateBounds = _
  private val VALID_SEVERITY_LEVELS = Set("FATAL", "WARN")
  private val VALID_DATA_TYPES = Set("TEXT", "INTEGER", "DECIMAL")

  private def setRuleName(value: String): this.type = {
    _ruleName = value
    this
  }

  /**
   * Allows for use of canonical naming and rule identification. Not necessary as of version 0.1 but
   * can be used for future use cases
   *
   * @param value input column from user
   * @return Rule
   */
  private[validation] def setColumn(value: Column): this.type = {
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

  private def setBlank(value: Boolean): this.type = {
    _blank = value
    this
  }

  private def setDateTimeLogic(value: Column): this.type = {
    _dateTimeLogic = value
    this
  }

  private def setRuleType(value: RuleType.Value): this.type = {
    _ruleType = value
    this
  }

  private[validation] def setIsAgg: this.type = {
    _isAgg = inputColumn.expr.prettyName == "aggregateexpression" || inputColumn.expr.prettyName == "count"
    this
  }

  /** Control the severity level.
   * @param severityLevel The desired severity level as a string.
   * Valid log levels include: FATAL, WARN
   */
  private def setSeverity(severityLevel: String): this.type = {
    val upperCased = severityLevel.toUpperCase()
    require(VALID_SEVERITY_LEVELS.contains(upperCased),
      s"Supplied severity level $upperCased did not match one of:" +
        s" ${VALID_SEVERITY_LEVELS.mkString(",")}")
    _severity = severityLevel
    this
  }

  private def setDateBoundaries(value: DateBounds): this.type = {
    _dateBounds = value
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

  def ruleType: RuleType.Value = _ruleType

  private[validation] def isAgg: Boolean = _isAgg

  def severity: String = _severity

  def blank: Boolean = _blank

  def dateBounds: DateBounds = _dateBounds

}

object Rule {

  /**
   * Several apply methods have been created to handle various types of rules and instantiations from the user
   */

  def apply(
             ruleName: String,
             column: Column,
             boundaries: Bounds,
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBoundaries(boundaries)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateBounds)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             boundaries: Bounds
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBoundaries(boundaries)
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateBounds)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double],
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateNumerics)
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
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long],
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateNumerics)
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
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int],
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateNumerics)
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
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validStrings: Array[String],
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidStrings(validStrings)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateStrings)
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
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateStrings)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             blank: Boolean,
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBlank(blank)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateBlank)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             blank: Boolean
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBlank(blank)
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateBlank)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             dateBounds: DateBounds,
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setDateBoundaries(dateBounds)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateDateBounds)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             dateBounds: DateBounds
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setDateBoundaries(dateBounds)
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateDateBounds)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             severity: String
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setSeverity(severity)
      .setRuleType(RuleType.ValidateAdhoc)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setSeverity(Severity.fatal)
      .setRuleType(RuleType.ValidateAdhoc)
      .setIsAgg
  }

  /**
   * TODO -- Implement Date/Time Logic for:
   * Column Type (i.e. current_timestamp and current_date)
   * java.util.Date
   * Validated strings compatible with Spark
   *
   * Additional logic can be added to extend functionality
   */

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
