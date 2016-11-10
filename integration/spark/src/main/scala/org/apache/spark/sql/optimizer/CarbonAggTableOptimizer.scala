/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.optimizer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonMetastoreCatalog

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Carbon aggregate table Optimizer.
 */
object CarbonAggTableOptimizer {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  var sQLContext: SQLContext = _
  var aggTableList: List[String] = _
  var mainTable: CarbonTable = _
  def init(sQLContext: SQLContext): Unit = {
    this.sQLContext = sQLContext
  }

  def apply(plan: LogicalPlan): LogicalPlan = {

    val enableUseAggTable = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_USE_AGG_TABLE, "true").toBoolean
    if (enableUseAggTable && needToTransformtoAggTable(plan)) {
      var aggTableCanBeUsed: ArrayBuffer[CarbonTable] = ArrayBuffer()
      var aggTablePlan: LogicalPlan = plan
      aggTablePlan = plan transform  {
          case agg@Aggregate(groupbyExp, aggregateExp,
            proj@Project(namedExp,
            filter@Filter(filterCondition,
            rel@LogicalRelation(relation, attr))))
            if (relation.isInstanceOf[CarbonDatasourceRelation]) =>
            var convertAggPlan = agg
            // first need to collect main attributes to compare with aggtable
            val mainTableAttrs = rel.output.map { attr =>
              (attr.name -> attr)
            }.toMap
            // foreach aggtable list to change the original plan
            aggTableList.foreach { aggTableName =>
              val aggTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
                .getCarbonTable(mainTable.getDatabaseName
                  + CarbonCommonConstants.UNDERSCORE + aggTableName)
              // first need to collect the attributeReference of agg table
              val tableIdentifier = Seq(aggTable.getDatabaseName, aggTable.getFactTableName)
              val aggRelation = sQLContext.catalog.lookupRelation(tableIdentifier) match {
                case Subquery(_, logical: LogicalRelation) => logical
                case other => other
              }
              // collect aggtable attribute references.
              var aggTableAttributes: Map[String, AttributeReference] = Map()
              aggRelation.collect {
                case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceRelation] =>
                  aggTableAttributes = l.output.map { attribute =>
                    (attribute.name -> attribute)
                  }.toMap
              }
              // need to compose new aggregate attributes
              var aggAttrs: Seq[AttributeReference] = Seq()
              val baseRelation = aggRelation.asInstanceOf[LogicalRelation].relation
              baseRelation.schema.toAttributes.foreach { attr =>
                if (aggTableAttributes.get(attr.name).nonEmpty
                  && mainTableAttrs.get(attr.name).nonEmpty) {
                  aggAttrs :+= mainTableAttrs.get(attr.name).get
                } else {
                  aggAttrs :+= aggTableAttributes.get(attr.name).get
                }
              }
              baseRelation.schema.toAttributes
              val newAggRelation = LogicalRelation(baseRelation, Some(aggAttrs))
              val newAggTableAttrs = newAggRelation.output.map { attr =>
                attr.name -> attr
              }.toMap
              // extract the aggreagte Expression and groupby expression and filter,
              // to judge whether match
              val analyzedPlan = sQLContext
                .sql(aggTable.getQuerySqlOnFactTable).queryExecution.analyzed
              val (aggAggregateExp, aggGroupByExp,
              aggFilterExp, aggProjAttrName) = extractExpression(analyzedPlan)
              val (isFilterMatch, convertedFilter) = convFilterFromMainToAgg(aggFilterExp,
                Some(filterCondition), aggProjAttrName, newAggTableAttrs)
              if (isFilterMatch) {
                val (isAggMatch, convertAggExp) = convAggPartFomMaintoAgg(aggAggregateExp,
                  aggregateExp, aggProjAttrName, newAggTableAttrs)
                if (isAggMatch) {
                  val convertGroupBy = convertAttributeFromMainToAgg(groupbyExp, newAggTableAttrs)
                  val projectExp = makeAggProjectNameExps(convertGroupBy, convertAggExp,
                    convertedFilter, newAggTableAttrs)
                  convertAggPlan =
                    Aggregate(convertGroupBy, convertAggExp.map(_.asInstanceOf[NamedExpression]),
                      Project(projectExp,
                        Filter(convertedFilter.get,
                          newAggRelation)))
                  LOGGER.info(s"Match aggregate table ${aggTable.getDatabaseName}.${
                    aggTable.getFactTableName} for main table ${aggTable.getDatabaseName}" +
                    s".${aggTable.getMainTableName}, change to query on aggregate table")
                }
              }
            }
            convertAggPlan
          case other => other
        }
      aggTablePlan
    } else {
      plan
    }
  }


  def convAggPartFomMaintoAgg(aggAggregateExp: Seq[Expression],
    mainAggregateExp: Seq[Expression],
    aggProjAttrNames: Set[String],
    aggAttributes: Map[String, AttributeReference]): (Boolean, Seq[Expression]) = {
    var isMatch = false
    var arithmetcis = Seq("")
    var newAggregateExp: Seq[Expression] = Seq.empty
    var matchCount = 0
    // first need to make sure all the attribute referance is match
    val mainAttrNames = collectAttributeName(mainAggregateExp)
    if (mainAttrNames subsetOf aggProjAttrNames) {
      isMatch = checkMatch(aggAggregateExp, mainAggregateExp)
      if (isMatch) {
        newAggregateExp = mainAggregateExp.map { exp =>
          exp transform {
            // Sum => Sum(sum), Count => Sum(count), Max => Max(max), Min => Min(min)
            case sum@catalyst.expressions.Sum(_) =>
              val aggFieleName = CarbonMetastoreCatalog.convertFuncToExpression(sum.prettyString)
              catalyst.expressions.Sum(aggAttributes.get(aggFieleName.toLowerCase()).get)
            case count@catalyst.expressions.Count(_) =>
              val aggFieleName = CarbonMetastoreCatalog.convertFuncToExpression(count.prettyString)
              catalyst.expressions.Sum(aggAttributes.get(aggFieleName.toLowerCase).get)
            case max@catalyst.expressions.Max(_) =>
              val aggFieleName = CarbonMetastoreCatalog.convertFuncToExpression(max.prettyString)
              catalyst.expressions.Max(aggAttributes.get(aggFieleName.toLowerCase).get)
            case min@catalyst.expressions.Min(_) =>
              val aggFieleName = CarbonMetastoreCatalog.convertFuncToExpression(min.prettyString)
              catalyst.expressions.Min(aggAttributes.get(aggFieleName.toLowerCase).get)
            case AttributeReference(name, _, _, _) =>
              aggAttributes.get(name).get
            case other => other
          }
        }
      }
    }
    (isMatch, newAggregateExp)

  }

  /**
   *
   * @param aggFilterExp  Option(Filter in create agg table)
   * @param mainFilterExp Option(Filter in actual query)
   * @param aggProjAttr   all the attribute name in agg table
   * @param aggAttributes all the attributeReference in agg table
   * @return
   */
  def convFilterFromMainToAgg(aggFilterExp: Option[Expression],
    mainFilterExp: Option[Expression],
    aggProjAttr: Set[String],
    aggAttributes: Map[String, AttributeReference]): (Boolean, Option[Expression]) = {
    var isMatch = false
    var newFilterExp: Option[Expression] = None
    if (aggFilterExp.isEmpty && mainFilterExp.isEmpty) {
      isMatch = true
    } else if (aggFilterExp.isEmpty && mainFilterExp.nonEmpty) {
      val mainFilterAttr = collectAttributeName(Seq(mainFilterExp.get))
      isMatch = mainFilterAttr subsetOf aggProjAttr
      newFilterExp = if (isMatch) {
        Some(convertAttributeFromMainToAgg(Seq(mainFilterExp.get), aggAttributes)(0))
      } else {
        None
      }
    } else if (aggFilterExp.nonEmpty && mainFilterExp.isEmpty) {
      isMatch = false
      // agg Filter and main filter both not empty
    } else {
      val mainFilterAttrName = collectAttributeName(Seq(mainFilterExp.get))
      isMatch = mainFilterAttrName subsetOf aggProjAttr
      if (isMatch) {
        newFilterExp = pruneMainfilter(aggFilterExp, mainFilterExp, aggAttributes)
      }
    }
    (isMatch, newFilterExp)
  }

  /**
   * remove the duplicate part in agg filter and main filter
   *
   * @param aggFilterExp
   * @param mainFilterExp
   * @param aggAttributes
   */
  def pruneMainfilter(aggFilterExp: Option[Expression],
    mainFilterExp: Option[Expression],
    aggAttributes: Map[String, AttributeReference]): Option[Expression] = {
    var collectedExps: Seq[Expression] = Seq()
    breakable {
      aggFilterExp.get foreach {
        case Or(_, _) =>
          throw new UnsupportedOperationException ("OR filter is not supported in aggregate table")
        case And(leftExp, rightExp) =>
          if (!leftExp.isInstanceOf[And]) {
            collectedExps :+= leftExp
          }
          if (!rightExp.isInstanceOf[And]) {
            collectedExps :+= rightExp
          }
        case other =>
          collectedExps :+= other
          break()
      }
    }
    var hasOr = false
    val prunedMainFilter = mainFilterExp.get transform {
      case or@Or(_, _) =>
        hasOr = true
        or
      case and@And(leftExp, rightExp) =>
        if (collectedExps.contains(leftExp) && !collectedExps.contains(rightExp)) {
          rightExp
        } else if (!collectedExps.contains(leftExp) && collectedExps.contains(rightExp)) {
          leftExp
        } else if (collectedExps.contains(leftExp) && collectedExps.contains(rightExp)) {
          null
        } else {
          and
        }
      case other =>
        other
    }
    if (hasOr) {
      throw new UnsupportedOperationException("OR filter is not supported in aggregate table")
    }
    // convert to aggregate filter
    Some(convertAttributeFromMainToAgg(Seq(prunedMainFilter), aggAttributes)(0))
  }

  /**
   *  Convert attribute reference from main to Agg table.
   *
   * @param mainExps
   * @param aggAttributes
   * @return
   */
  def convertAttributeFromMainToAgg(mainExps: Seq[Expression],
    aggAttributes: Map[String, AttributeReference]): Seq[Expression] = {
    val aggTableExps = mainExps.map( exp =>
      exp transform {
        case AttributeReference(name, _, _, _) =>
          aggAttributes.get(name).get
        case other => other
      }
    )
    aggTableExps
  }

  def makeAggProjectNameExps(convertGroupBy: Seq[Expression],
    convertAggExp: Seq[Expression],
    convertedFilter: Option[Expression],
    aggAttributes: Map[String, AttributeReference]): Seq[NamedExpression] = {
    val attrNames = collectAttributeName(convertAggExp ++ convertGroupBy
      :+ convertedFilter.get).toSeq
    val attributes = attrNames.map { attr =>
      aggAttributes.get(attr).get
    }
    attributes
  }

  /**
   * foreach to get all the attr name
   *
   * @param expressions
   * @return
   */
  def collectAttributeName(expressions: Seq[Expression]): Set[String] = {
    var attributeSet: Set[String] = Set()
    expressions.map( exp =>
      exp foreach {
        case AttributeReference(name, _, _, _) =>
          attributeSet += name
        case other =>
      }
    )
    attributeSet
  }

  /**
   *  extract the aggExp, groupByExp, FilterExp and attributeReference in project
   * @param plan
   * @return
   */
  def extractExpression(plan: LogicalPlan):
  (Seq[Expression], Seq[Expression], Option[Expression], Set[String]) = {
    var aggregateExp: Seq[Expression] = Seq()
    var groupByExp: Seq[Expression] = Seq()
    var filterExp: Option[Expression] = None
    var projectAttr: Set[String] = Set()
    plan collect {
      case Aggregate(groupBy, aggregate, _) =>
        aggregateExp = aggregate
        groupByExp = groupBy
      case Filter(condition, _) =>
        filterExp = Some(condition)
      case _ =>
    }
    // collect all the attribute name in the query.
    projectAttr = collectAttributeName(aggregateExp ++ groupByExp :+ filterExp.get)
    (aggregateExp, groupByExp, filterExp, projectAttr)
  }

  def chooseBestMatchTable(aggTableList: Array[CarbonTable]): CarbonTable = {
    var index = 0
    val aggTableWithSize =
      aggTableList.map { aggTable =>
        var carbonTableIdentifier = aggTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
        // create dictionary folder if not exists
        val aggTablePath = CarbonStorePath.getCarbonTablePath(
          aggTable.getStorePath, carbonTableIdentifier)
        val tableFactpath = aggTablePath.getFactDir
        val fileType = FileFactory.getFileType(tableFactpath)
        val carbonFile = FileFactory.getCarbonFile(tableFactpath, fileType)
        (aggTable, countSize(carbonFile))
      }
    aggTableWithSize.sortBy(_._2).apply(0)._1
  }

  def countSize(carbonFile: CarbonFile): Long = {
    var size: Long = 0
    if (carbonFile.isDirectory) {
      carbonFile.listFiles().foreach {
        size += countSize(_)
      }
    } else {
      size = carbonFile.getSize
    }
    size
  }

  def checkMatch(aggTableExp: Seq[Expression], mainTableExp: Seq[Expression]): Boolean = {
    // when aggTable has no someone of maintable's exprssion, then not match.
    var matchCount: Int = mainTableExp.size
    breakable {
      mainTableExp.foreach { mainExp =>
        var isContain: Boolean = false
        // for CASE WHEN, need to check independently
        mainExp match {
          case Alias(CaseWhen(branches), _) =>
            branches foreach {branch =>
              branch foreach {
                case sum@catalyst.expressions.Sum(_) =>
                  if (!checkContains(sum, aggTableExp)) {
                    matchCount -= 1
                    break
                  }
                case count@catalyst.expressions.Count(_) =>
                  if (!checkContains(count, aggTableExp)) {
                    matchCount -= 1
                    break
                  }
                case max@catalyst.expressions.Max(_) =>
                  if (!checkContains(max, aggTableExp)) {
                    matchCount -= 1
                    break
                  }
                case min@catalyst.expressions.Min(_) =>
                  if (!checkContains(min, aggTableExp)) {
                    matchCount -= 1
                    break
                  }
                case avg@catalyst.expressions.Average(_) =>
                  throw new UnsupportedOperationException("Average is not supported temporaily")
                case other =>
              }
            }
          case other =>
            if (!checkContains(mainExp, aggTableExp)) {
              matchCount -= 1
              break
            }
        }
      }
    }
    matchCount == mainTableExp.length
  }

  /**
   *
   * @param mainExp
   * @param aggTableExps
   * @return
   */
  def checkContains(mainExp: Expression, aggTableExps: Seq[Expression]): Boolean = {
    var isEqual = false
    // foreach to check whether expression in maintable is in aggTable expression
    breakable {
      aggTableExps.foreach { aggExp =>
        isEqual = checkEquals(aggExp, mainExp)
        if (isEqual) break
      }
    }
    isEqual
  }

  /**
   *  first use semantic equal to chekc, other than, use pretty name match
   *
   * @param aggExp
   * @param mainExp
   * @return
   */
  def checkEquals(aggExp: Expression, mainExp: Expression): Boolean = {
    var isMatch = false
    if (aggExp.semanticEquals(mainExp)) {
      isMatch = true
    } else {
      if (aggExp.getClass.equals(mainExp.getClass)) {
        aggExp match {
          case AttributeReference(name, _, _, _) =>
            isMatch = name.equals(mainExp.asInstanceOf[AttributeReference].name)
          case Alias(child, _) =>
            isMatch = child.prettyString.split("AS")(0).trim
              .equals(mainExp.asInstanceOf[Alias].child.prettyString.split("AS")(0).trim)
        }
      } else if (aggExp.isInstanceOf[Alias]) {
        aggExp match {
          case Alias(child, _) =>
            isMatch = child.prettyString.split("AS")(0).trim.equals(
              mainExp.prettyString.split("AS")(0).trim)
          case other => other
        }
      } else if (mainExp.isInstanceOf[Alias]) {
        mainExp match {
          case Alias(child, _) =>
            isMatch = child.prettyString.split("AS")(0).trim.equals(
              aggExp.prettyString.split("AS")(0).trim)
          case other => other
        }
      }
    }
    isMatch
  }

  def needToTransformtoAggTable(plan: LogicalPlan): Boolean = {
    var aggTableExists = false
    var aggExprssionExists = false
    plan collect {
      case l: LogicalRelation if l.relation
        .isInstanceOf[CarbonDatasourceRelation] =>
        val tableIdentifier = l.relation.asInstanceOf[CarbonDatasourceRelation].tableIdentifier
        mainTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
          .getCarbonTable(tableIdentifier.database.get
            + CarbonCommonConstants.UNDERSCORE + tableIdentifier.table)
        if (!mainTable.getAggregateTablesName.isEmpty) {
          aggTableList = mainTable.getAggregateTablesName.asScala.toList
          aggTableExists = true
        }
      case agg: Aggregate =>
        aggExprssionExists = true
      case other =>
    }
    aggExprssionExists && aggTableExists
  }


}

