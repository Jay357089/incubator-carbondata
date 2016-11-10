package org.apache.spark.util

import scala.io.Source
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.CreateAggTable

/**
  * Created by l00357089 on 2016/12/1.
  */
object CreateAggTableAPI {

  def createsSark(storePath: String, appName: String): SparkSession = {
    SparkSession
      .builder
      .appName(appName)
      .master("local")
      .config(CarbonCommonConstants.STORE_LOCATION, storePath)
      .config("spark.sql.warehouse.dir", "file:///E:/carbondata/examples/spark2/spark-warehouse")
      .getOrCreate()
  }

  //args[0] storepath, args[1]: database name args[2]: agg table name, args[3]  query sql file.
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: query sql file is needed")
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val dbName = args(1)
    val aggTableName = args(2).toLowerCase
    val factTableName = aggTableName.split(CarbonCommonConstants.UNDERSCORE +
      CarbonCommonConstants.AGG_TABLE_FLAG)(0)
    val sparkSession = createsSark(storePath, "CreateAggTable:" + dbName + "." + aggTableName)
    val querySql = Source.fromFile(args(3)).mkString
    CreateAggTable(dbName, factTableName, aggTableName, querySql).run(sparkSession)

  }
}
