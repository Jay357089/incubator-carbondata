

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

package org.apache.spark.sql.examples

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.TableLoader

object testExample {

  def main(args: Array[String]): Unit = {
    val rootPath = "E:/carbondata"
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonExample")
      .enableHiveSupport()
      .config(CarbonCommonConstants.STORE_LOCATION,
        s"$rootPath/examples/spark2/target/store")
      .config("spark.sql.warehouse.dir", "file:///E:/carbondata/examples/spark2/spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //    val spark = SparkSession.builder.config(sc.getConf).enableHiveSupport().getOrCreate()
    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")

    // Create table
    //    spark.sql(
    //      s"""
    //         | CREATE TABLE carbon_table(
    //         |    shortField short,
    //         |    intField int,
    //         |    bigintField long,
    //         |    doubleField double,
    //         |    stringField string
    //         | )
    //         | USING org.apache.spark.sql.CarbonSource
    //       """.stripMargin)

    spark.sql(s"CREATE TABLE dwcjk (CJRQ String, CJXH string, " +
      s"ZQDH String, BXWDH String, BGDDM String, BHTXH String, BRZRQ String, " +
      s"BPCBZ String, SXWDH String, SGDDM String, SHTXH String, SRZRQ String, SPCBZ String, CJGS int, " +
      s"CJJG double, CJSJ int, YWLB String, MMLB String, FBBZ String, FILLER String) " +
      s"USING org.apache.spark.sql.CarbonSource Options('DICTIONARY_INCLUDE'='CJSJ')")
    //    spark.sql(s"create table dwcjk_agg USING org.apache.spark.sql.CarbonSource" +
    //      s" as select cjrq, cjsj, zqdh, bhtxh as ybdm, bxwdh as jydy, bgddm as gddm, mmlb, " +
    //      s"sum(cjgs) as cjgs, sum(cjgs*cjjg) as cjje " +
    //      s"from dwcjk where mmlb<>'c' group by cjrq, cjsj, zqdh, bxwdh, bgddm, bhtxh, mmlb ")


    val prop = s"$rootPath/conf/dataload.properties.template"
    val tableName = "dwcjk"
    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"
    val dwcjk = s"$rootPath/examples/spark2/src/main/resources/dwcjk.csv"
    TableLoader.main(Array[String](prop, tableName, dwcjk))


    //    spark.sql("desc formatted dwcjk").show()
    //    spark.sql(
    //      s"""
    //         | CREATE TABLE csv_table
    //         | (ID int,
    //         | date timestamp,
    //         | country string,
    //         | name string,
    //         | phonetype string,
    //         | serialname string,
    //         | salary int)
    //       """.stripMargin)
    //
    //    spark.sql(
    //      s"""
    //         | LOAD DATA LOCAL INPATH '$csvPath'
    //         | INTO TABLE csv_table
    //       """.stripMargin)

    //    spark.sql(
    //      s"""
    //         | INSERT INTO TABLE carbon_table
    //         | SELECT * FROM csv_table
    //       """.stripMargin)

    // Perform a query
    //    spark.sql("""
    //           SELECT country, count(salary) AS amount
    //           FROM carbon_table
    //           WHERE country IN ('china','france')
    //           GROUP BY country
    //           """).show()

    //    spark.sql("""
    //           SELECT sum(intField), stringField
    //           FROM carbon_table
    //           GROUP BY stringField
    //           """).show

    spark.sql(" select * from dwcjk").show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")
  }

}
