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

package org.apache.spark.sql.hive

import java.io._
import java.util
import java.util.{GregorianCalendar, UUID}

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AggregateTableAttributes, Partitioner}
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.CarbonTableIdentifier
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.apache.carbondata.core.reader.ThriftReader
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.lcm.locks.ZookeeperInit
import org.apache.carbondata.spark.{CarbonDataFrameWriter, CarbonOption}
import org.apache.carbondata.spark.util.CarbonScalaUtil.CarbonSparkUtil

case class MetaData(var tablesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String],
    msrs: Seq[String],
    carbonTable: CarbonTable,
    dictionaryMap: DictionaryMap)

case class TableMeta(carbonTableIdentifier: CarbonTableIdentifier, storePath: String,
    var carbonTable: CarbonTable, partitioner: Partitioner)

object CarbonMetastoreCatalog {

  def readSchemaFileToThriftTable(schemaFilePath: String): TableInfo = {
    val createTBase = new ThriftReader.TBaseCreator() {
      override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
        new TableInfo()
      }
    }
    val thriftReader = new ThriftReader(schemaFilePath, createTBase)
    var tableInfo: TableInfo = null
    try {
      thriftReader.open()
      tableInfo = thriftReader.read().asInstanceOf[TableInfo]
    } finally {
      thriftReader.close()
    }
    tableInfo
  }

  def writeThriftTableToSchemaFile(schemaFilePath: String, tableInfo: TableInfo): Unit = {
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    try {
      thriftWriter.open()
      thriftWriter.write(tableInfo);
    } finally {
      thriftWriter.close()
    }
  }


  /**
   * for example, sum(id) will transform to sum_id,
   * subString(id,1,3) will transform to subString_id_1_3
   * @param aggregateFun
   * @return expression
   */
  def convertFuncToExpression(aggregateFun: String): String = {
    val arithmetics = Seq("_ADD_", "_SUB_", "_MUL_", "_DIV_")
    val colName: StringBuilder = new StringBuilder()
    for(alpha <- aggregateFun.toCharArray) {
      val newAlpha: String =
        if ((alpha >= 'a'&& alpha <= 'z')
          || (alpha >= 'A'&& alpha <= 'Z')
          || (alpha >= '0'&& alpha <= '9')
          || alpha == '_') {
          alpha.toString
        } else if (alpha == '+') {
          arithmetics(0)
        } else if (alpha == '-') {
          arithmetics(1)
        } else if (alpha == '*') {
          arithmetics(2)
        } else if (alpha == '/') {
          arithmetics(3)
        } else {
          "_"
        }
      colName.append(newAlpha)
    }
    colName.toString
  }

  def createAggTableSql(schema: StructType,
    factTable: CarbonTable,
    dataBaseName: String,
    aggTableName: String,
    groupByExp: Seq[String],
    aggregateExp: Seq[String]): String = {
    var id: Int = -1
    val aggTableSchema = schema.map { field =>
      id += 1
      s"${convertFuncToExpression(aggregateExp(id))} ${ field.dataType match {
        case DecimalType.Fixed(precision, scale) => s"DECIMAL($precision, $scale)"
        case other => CarbonDataFrameWriter
        .convertToCarbonType(field.dataType) } }"
    }
    val aggregationTypes = Seq("SUM", "COUNT", "MAX", "MIN")
    var dictExcludeCols: Seq[String] = Seq()
    var dictIncludeCols: Seq[String] = Seq()
    aggTableSchema.map { aggExpressionWithType =>
      val schemaExp = aggExpressionWithType.split("\\s+")(0)
      val schemaType = aggExpressionWithType.split("\\s+")(1)
      val colSplits = schemaExp.split("_")
      // column format, that is, dimenison
      if (colSplits.length == 1) {
        val dimenison = factTable.getDimensionByName(factTable.getFactTableName,
          schemaExp)
        if (dimenison.hasEncoding(Encoding.DICTIONARY) &&
          !dimenison.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          dictIncludeCols :+= schemaExp
        } else if (!dimenison.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          dictExcludeCols :+= schemaExp
        }
      } else {
        // MAX(String) and MIN(String) should be no dictionary
        if ((colSplits(0).equalsIgnoreCase(aggregationTypes(2)) ||
          colSplits(0).equalsIgnoreCase(aggregationTypes(3))) &&
           schemaType == "STRING") {
          dictExcludeCols :+= schemaExp
        } else if (!aggregationTypes.contains(colSplits(0).toUpperCase)) {
          // the function is not aggregate functions, treate as no dictionary
          dictExcludeCols :+= schemaExp
        }
      }
    }
    val dictInclude = if (dictIncludeCols.isEmpty) ""
    else s"'DICTIONARY_INCLUDE'='${dictIncludeCols.mkString(",")}' "
    val dictExclude = if (dictExcludeCols.isEmpty) ""
    else s"'DICTIONARY_EXCLUDE'='${dictExcludeCols.mkString(",")}' "
    val createSql = s"""
          CREATE TABLE IF NOT EXISTS $dataBaseName.$aggTableName
          (${ aggTableSchema.mkString(", ") })
          STORED BY '${ CarbonContext.datasourceName }'
          TBLPROPERTIES($dictInclude
      ${if (dictInclude != "" && dictExclude != "") {","} else {""}} $dictExclude) """
    createSql
  }
}

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

class CarbonMetastoreCatalog(hiveContext: HiveContext, val storePath: String,
    client: ClientInterface, queryId: String)
  extends HiveMetastoreCatalog(client, hiveContext)
    with spark.Logging {

  @transient val LOGGER = LogServiceFactory
    .getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

  val metadata = loadMetadata(storePath)


  def getTableCreationTime(databaseName: String, tableName: String): Long = {
    val tableMeta = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(databaseName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
    val tableCreationTime = tableMeta.head.carbonTable.getTableLastUpdatedTime
    tableCreationTime
  }

  def lookupRelation1(dbName: Option[String],
      tableName: String)(sqlContext: SQLContext): LogicalPlan = {
    lookupRelation1(TableIdentifier(tableName, dbName))(sqlContext)
  }

  def lookupRelation1(tableIdentifier: TableIdentifier,
      alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tables = getTableFromMetadata(database, tableIdentifier.table)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableIdentifier.table,
          CarbonSparkUtil.createSparkMeta(tables.head.carbonTable), tables.head, alias)(sqlContext)
      case None =>
        LOGGER.audit(s"Table Not Found: ${tableIdentifier.table}")
        throw new NoSuchTableException
    }
  }

  /**
   * This method will search for a table in the catalog metadata
   *
   * @param database
   * @param tableName
   * @return
   */
  def getTableFromMetadata(database: String,
      tableName: String): Option[TableMeta] = {
    metadata.tablesMeta
      .find(c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
                 c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
  }

  def tableExists(tableIdentifier: TableIdentifier)(sqlContext: SQLContext): Boolean = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableIdentifier.table))
    tables.nonEmpty
  }

  def loadMetadata(metadataPath: String): MetaData = {
    val recorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val statistic = new QueryStatistic()
    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    val zookeeperUrl: String = hiveContext.getConf(CarbonCommonConstants.ZOOKEEPER_URL, null)
    if (zookeeperUrl != null) {
      CarbonProperties.getInstance.addProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperUrl)
      ZookeeperInit.getInstance(zookeeperUrl)
      LOGGER.info("Zookeeper url is configured. Taking the zookeeper as lock type.")
      var configuredLockType = CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.LOCK_TYPE)
      if (null == configuredLockType) {
        configuredLockType = CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER
        CarbonProperties.getInstance
          .addProperty(CarbonCommonConstants.LOCK_TYPE,
            configuredLockType)
      }
    }

    if (metadataPath == null) {
      return null
    }
    val fileType = FileFactory.getFileType(metadataPath)
    val metaDataBuffer = new ArrayBuffer[TableMeta]
    fillMetaData(metadataPath, fileType, metaDataBuffer)
    updateSchemasUpdatedTime(readSchemaFileSystemTime("", ""))
    statistic.addStatistics(QueryStatisticsConstants.LOAD_META,
      System.currentTimeMillis())
    recorder.recordStatisticsForDriver(statistic, queryId)
    MetaData(metaDataBuffer)
  }

  private def fillMetaData(basePath: String, fileType: FileType,
      metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
    val databasePath = basePath // + "/schemas"
    try {
      if (FileFactory.isFileExist(databasePath, fileType)) {
        val file = FileFactory.getCarbonFile(databasePath, fileType)
        val databaseFolders = file.listFiles()

        databaseFolders.foreach(databaseFolder => {
          if (databaseFolder.isDirectory) {
            val dbName = databaseFolder.getName
            val tableFolders = databaseFolder.listFiles()

            tableFolders.foreach(tableFolder => {
              if (tableFolder.isDirectory) {
                val carbonTableIdentifier = new CarbonTableIdentifier(databaseFolder.getName,
                  tableFolder.getName, UUID.randomUUID().toString)
                val carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                  carbonTableIdentifier)
                val tableMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
                  val tableName = tableFolder.getName
                  val tableUniqueName = databaseFolder.getName + "_" + tableFolder.getName


                  val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                      new TableInfo()
                    }
                  }
                  val thriftReader = new ThriftReader(tableMetadataFile, createTBase)
                  thriftReader.open()
                  val tableInfo: TableInfo = thriftReader.read().asInstanceOf[TableInfo]
                  thriftReader.close()

                  val schemaConverter = new ThriftWrapperSchemaConverterImpl
                  val wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, basePath)
                  val schemaFilePath = CarbonStorePath
                    .getCarbonTablePath(storePath, carbonTableIdentifier).getSchemaFilePath
                  wrapperTableInfo.setStorePath(storePath)
                  wrapperTableInfo
                    .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  val carbonTable =
                    org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
                      .getCarbonTable(tableUniqueName)
                  metaDataBuffer += TableMeta(
                    carbonTable.getCarbonTableIdentifier,
                    storePath,
                    carbonTable,
                    // TODO: Need to update Database thirft to hold partitioner
                    // information and reload when required.
                    Partitioner("org.apache.carbondata.spark.partition.api.impl." +
                                "SampleDataPartitionerImpl",
                      Array(""), 1, Array("")))
                }
              }
            })
          }
        })
      } else {
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
    }
  }

  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableinfo
   *
   */
  def createTableFromThrift(
      tableInfo: org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo,
      dbName: String, tableName: String, partitioner: Partitioner)
    (sqlContext: SQLContext): String = {
    if (tableExists(TableIdentifier(tableName, Some(dbName)))(sqlContext)) {
      sys.error(s"Table [$tableName] already exists under Database [$dbName]")
    }
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)

    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
      tableInfo.getFactTable.getTableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val tableMeta = TableMeta(
      carbonTableIdentifier,
      storePath,
      CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName),
      Partitioner("org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
        Array(""), 1, DistributionUtil.getNodeList(hiveContext.sparkContext)))

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    metadata.tablesMeta += tableMeta
    logInfo(s"Table $tableName for Database $dbName created successfully.")
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
    carbonTablePath.getPath
  }

  def updateAggTableSchema(dataBaseName: String,
      factTableName: String,
      aggTableName: String,
      querySqlonFact: String): Unit = {
    val aggTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dataBaseName + "_" + aggTableName)
    val factTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dataBaseName + "_" + factTableName)
    // need to copy the dict dimension column from fact table
    val dimensions = aggTable.getDimensionByTableName(aggTable.getFactTableName).asScala
    val measures = aggTable.getMeasureByTableName(aggTable.getFactTableName).asScala
    val dimColumns = dimensions.map { dimension =>
      var dimColumn = dimension.getColumnSchema
      if (dimension.getColumnSchema.hasEncoding(Encoding.DICTIONARY)) {
        dimColumn = factTable.getDimensionByName(factTable.getFactTableName,
          dimension.getColName).getColumnSchema
      }
      dimColumn
    }
    val measureColumns = measures.map(_.getColumnSchema)
    val allColumns = dimColumns ++ measureColumns

    val aggThriftTableInfo = CarbonMetastoreCatalog.readSchemaFileToThriftTable(
      aggTable.getMetaDataFilepath + File.separator + "schema")
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val aggWrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(aggThriftTableInfo, dataBaseName,
        aggTableName, aggTable.getStorePath)
    // reset it.
    aggWrapperTableInfo.getFactTable.setListOfColumns(allColumns.asJava)
    aggWrapperTableInfo.getFactTable.getTableProperties.put(
      CarbonCommonConstants.QUERYSQL_ON_FACTTABLE, querySqlonFact)
    aggWrapperTableInfo.getFactTable.getTableProperties.put(
      CarbonCommonConstants.MAIN_TABLE_NAME, factTableName)
    aggWrapperTableInfo.getFactTable.getTableProperties.put(
      CarbonCommonConstants.isAggTable, "true")
    aggWrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    val updatedAggThriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(
      aggWrapperTableInfo, dataBaseName, aggTableName)
    CarbonMetastoreCatalog.writeThriftTableToSchemaFile(aggTable.getMetaDataFilepath
      + File.separator + "schema", updatedAggThriftTableInfo)
    aggWrapperTableInfo.setMetaDataFilepath(aggTable.getMetaDataFilepath)
    CarbonMetadata.getInstance().loadTableMetadata(aggWrapperTableInfo)
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(dataBaseName, aggTableName))
  }

  def updateFactTableSchema(dataBaseName: String,
      factTableName: String,
      aggTableName: String): Unit = {
    // first to get fact tableInfo
    // then get aggregation tableInfo
    val factTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dataBaseName + "_" + factTableName)
    val factThriftTableInfo = CarbonMetastoreCatalog.readSchemaFileToThriftTable(
      factTable.getMetaDataFilepath + File.separator + "schema")
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val factWrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(factThriftTableInfo, dataBaseName,
        factTableName, factTable.getStorePath)
    val aggTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dataBaseName + "_" + aggTableName)
    val aggThriftTableInfo = CarbonMetastoreCatalog.readSchemaFileToThriftTable(
      aggTable.getMetaDataFilepath + File.separator + "schema")
    val aggWrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(aggThriftTableInfo, dataBaseName,
        aggTableName, aggTable.getStorePath)
    factWrapperTableInfo.getAggregateTableList.add(aggWrapperTableInfo.getFactTable)
    factWrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis)
    // update schema file information
    val updatedFactThriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(
      factWrapperTableInfo, dataBaseName, factTableName)
    CarbonMetastoreCatalog.writeThriftTableToSchemaFile(factTable.getMetaDataFilepath
      + File.separator + "schema", updatedFactThriftTableInfo)
    // update meta
    factWrapperTableInfo.setMetaDataFilepath(factTable.getMetaDataFilepath)
    CarbonMetadata.getInstance().loadTableMetadata(factWrapperTableInfo)
    LOGGER.info(s"table $dataBaseName.$factTableName's schema is updated successfully")
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(dataBaseName, factTableName))
  }

  private def updateMetadataByWrapperTable(
      wrapperTableInfo: org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo): Unit = {

    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      wrapperTableInfo.getTableUniqueName)
    for (i <- metadata.tablesMeta.indices) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.tablesMeta(i).carbonTableIdentifier.getTableUniqueName)) {
        metadata.tablesMeta(i).carbonTable = carbonTable
      }
    }
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, storePath: String): Unit = {

    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, storePath)
    wrapperTableInfo
      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
    wrapperTableInfo.setStorePath(storePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }


  def getDimensions(carbonTable: CarbonTable,
      aggregateAttributes: List[AggregateTableAttributes]): Array[String] = {
    var dimArray = Array[String]()
    aggregateAttributes.filter { agg => null == agg.aggType }.foreach { agg =>
      val colName = agg.colName
      if (null != carbonTable.getMeasureByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Measure must be provided along with aggregate function :: $colName")
      }
      if (null == carbonTable.getDimensionByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Invalid column name. Cannot create an aggregate table :: $colName")
      }
      if (dimArray.contains(colName)) {
        sys.error(s"Duplicate column name. Cannot create an aggregate table :: $colName")
      }
      dimArray :+= colName
    }
    dimArray
  }

  /**
   * Shows all schemas which has Database name like
   */
  def showDatabases(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
      schemaLike match {
        case Some(name) =>
          if (c.carbonTableIdentifier.getDatabaseName.contains(name)) {
            c.carbonTableIdentifier
              .getDatabaseName
          } else {
            null
          }
        case _ => c.carbonTableIdentifier.getDatabaseName
      }
    }.filter(f => f != null)
  }

  /**
   * Shows all tables for given schema.
   */
  def getTables(databaseName: Option[String])(sqlContext: SQLContext): Seq[(String, Boolean)] = {

    val dbName =
      databaseName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.filter { c =>
      c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(dbName)
    }.map { c => (c.carbonTableIdentifier.getTableName, false) }
  }

  /**
   * Shows all tables in all schemas.
   */
  def getAllTables()(sqlContext: SQLContext): Seq[TableIdentifier] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
      TableIdentifier(c.carbonTableIdentifier.getTableName,
        Some(c.carbonTableIdentifier.getDatabaseName))
    }
  }

  def isTablePathExists(tableIdentifier: TableIdentifier)(sqlContext: SQLContext): Boolean = {
    val dbName = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tableName = tableIdentifier.table

    val tablePath = CarbonStorePath.getCarbonTablePath(this.storePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getPath

    val fileType = FileFactory.getFileType(tablePath)
    FileFactory.isFileExist(tablePath, fileType)
  }

  def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sqlContext: SQLContext) {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table

    val metadataFilePath = CarbonStorePath.getCarbonTablePath(tableStorePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getMetadataDirectoryPath

    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTables
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      val metadataToBeRemoved: Option[TableMeta] = getTableFromMetadata(dbName,
        tableIdentifier.table)
      metadataToBeRemoved match {
        case Some(tableMeta) =>
          metadata.tablesMeta -= tableMeta
          org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
            .removeTable(dbName + "_" + tableName)
          org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
            .removeTable(dbName + "_" + tableName)
          updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
        case None =>
          logInfo(s"Metadata does not contain entry for table $tableName in database $dbName")
      }
      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sqlContext)
      // discard cached table info in cachedDataSourceTables
      sqlContext.catalog.refreshTable(tableIdentifier)
    }
  }

  private def getTimestampFileAndType(databaseName: String, tableName: String) = {
    val timestampFile = storePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  /**
   * This method will put the updated timestamp of schema file in the table modified time store map
   *
   * @param timeStamp
   */
  def updateSchemasUpdatedTime(timeStamp: Long) {
    tableModifiedTimeStore.put("default", timeStamp)
  }

  /**
   * This method will read the timestamp of empty schema file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  def readSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime
    } else {
      System.currentTimeMillis()
    }
  }

  /**
   * This method will check and create an empty schema timestamp file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  def touchSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $databaseName.$tableName")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }
    val systemTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(systemTime)
    systemTime
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        refreshCache()
      }
    }
  }

  def refreshCache() {
    metadata.tablesMeta = loadMetadata(storePath).tablesMeta
  }

  def getSchemaLastUpdatedTime(databaseName: String, tableName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      schemaLastUpdatedTime = FileFactory.getCarbonFile(timestampFile, timestampFileType)
        .getLastModifiedTime
    }
    schemaLastUpdatedTime
  }

  def readTableMetaDataFile(tableFolder: CarbonFile,
      fileType: FileFactory.FileType):
  (String, String, String, String, Partitioner, Long) = {
    val tableMetadataFile = tableFolder.getAbsolutePath + "/metadata"

    var schema: String = ""
    var databaseName: String = ""
    var tableName: String = ""
    var dataPath: String = ""
    var partitioner: Partitioner = null
    val cal = new GregorianCalendar(2011, 1, 1)
    var tableCreationTime = cal.getTime.getTime

    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      // load metadata
      val in = FileFactory.getDataInputStream(tableMetadataFile, fileType)
      var len = 0
      try {
        len = in.readInt()
      } catch {
        case others: EOFException => len = 0
      }

      while (len > 0) {
        val databaseNameBytes = new Array[Byte](len)
        in.readFully(databaseNameBytes)

        databaseName = new String(databaseNameBytes, "UTF8")
        val tableNameLen = in.readInt()
        val tableNameBytes = new Array[Byte](tableNameLen)
        in.readFully(tableNameBytes)
        tableName = new String(tableNameBytes, "UTF8")

        val dataPathLen = in.readInt()
        val dataPathBytes = new Array[Byte](dataPathLen)
        in.readFully(dataPathBytes)
        dataPath = new String(dataPathBytes, "UTF8")

        val versionLength = in.readInt()
        val versionBytes = new Array[Byte](versionLength)
        in.readFully(versionBytes)

        val schemaLen = in.readInt()
        val schemaBytes = new Array[Byte](schemaLen)
        in.readFully(schemaBytes)
        schema = new String(schemaBytes, "UTF8")

        val partitionLength = in.readInt()
        val partitionBytes = new Array[Byte](partitionLength)
        in.readFully(partitionBytes)
        val inStream = new ByteArrayInputStream(partitionBytes)
        val objStream = new ObjectInputStream(inStream)
        partitioner = objStream.readObject().asInstanceOf[Partitioner]
        objStream.close()

        try {
          tableCreationTime = in.readLong()
          len = in.readInt()
        } catch {
          case others: EOFException => len = 0
        }

      }
      in.close()
    }

    (databaseName, tableName, dataPath, schema, partitioner, tableCreationTime)
  }

  def createDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    FileFactory.mkdirs(databasePath, fileType)
  }

  def dropDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    if (FileFactory.isFileExist(databasePath, fileType)) {
      val dbPath = FileFactory.getCarbonFile(databasePath, fileType)
      CarbonUtil.deleteFoldersAndFiles(dbPath)
    }
  }

}


object CarbonMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ShortType |
    "short" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "long" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    fixedDecimalType |
    "decimal" ^^^ "decimal" ^^^ DecimalType(18, 2) |
    "varchar\\((\\d+)\\)".r ^^^ StringType |
    "timestamp" ^^^ TimestampType

  protected lazy val fixedDecimalType: Parser[DataType] =
    "decimal" ~> "(" ~> "^[1-9]\\d*".r ~ ("," ~> "^[0-9]\\d*".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField, ",") <~ ">" ^^ {
      case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = {
    parseAll(dataType, metastoreType) match {
      case Success(result, _) => result
      case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
    }
  }

  def toMetastoreType(dt: DataType): String = {
    dt match {
      case ArrayType(elementType, _) => s"array<${ toMetastoreType(elementType) }>"
      case StructType(fields) =>
        s"struct<${
          fields.map(f => s"${ f.name }:${ toMetastoreType(f.dataType) }")
            .mkString(",")
        }>"
      case StringType => "string"
      case FloatType => "float"
      case IntegerType => "int"
      case ShortType => "tinyint"
      case DoubleType => "double"
      case LongType => "bigint"
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DecimalType() => "decimal"
      case TimestampType => "timestamp"
    }
  }
}
