/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File
import java.math.BigDecimal

import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{CarbonHiveContext, QueryTest}
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ArrayBuffer

class TestLoadDataGeneral extends QueryTest with BeforeAndAfterAll {

  var currentDirectory: String = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS loadtest")
    sql(
      """
        | CREATE TABLE loadtest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
  }

  private def checkSegmentExists(
      segmentId: String,
      datbaseName: String,
      tableName: String): Boolean = {
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(datbaseName + "_" + tableName)
    val partitionPath = CarbonStorePath.getCarbonTablePath(
      CarbonHiveContext.hdfsCarbonBasePath,
      carbonTable.getCarbonTableIdentifier).getPartitionDir("0")
    val fileType: FileFactory.FileType = FileFactory.getFileType(partitionPath)
    val carbonFile = FileFactory.getCarbonFile(partitionPath, fileType)
    val segments: ArrayBuffer[String] = ArrayBuffer()
    carbonFile.listFiles.foreach { file =>
      segments += CarbonTablePath.DataPathUtil.getSegmentId(file.getAbsolutePath + "/dummy")
    }
    segments.contains(segmentId)
  }

  test("test data loading CSV file") {
    val testData = currentDirectory + "/src/test/resources/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading CSV file without extension name") {
    val testData = currentDirectory + "/src/test/resources/sample"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(8))
    )
  }

  test("test data loading GZIP compressed CSV file") {
    val testData = currentDirectory + "/src/test/resources/sample.csv.gz"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(12))
    )
  }

  test("test data loading BZIP2 compressed CSV file") {
    val testData = currentDirectory + "/src/test/resources/sample.csv.bz2"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(16))
    )
  }

  test("test data loading CSV file with delimiter char \\017") {
    val testData = currentDirectory + "/src/test/resources/sample_withDelimiter017.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest options ('delimiter'='\\017')")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(20))
    )
  }

  test("test data loading with invalid values for mesasures") {
    val testData = currentDirectory + "/src/test/resources/invalidMeasures.csv"
    sql("drop table if exists invalidMeasures")
    sql("CREATE TABLE invalidMeasures (country String, salary double, age decimal(10,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table invalidMeasures options('Fileheader'='country,salary,age')")
    checkAnswer(
      sql("SELECT * FROM invalidMeasures"),
      Seq(Row("India",null,new BigDecimal("22.44")), Row("Russia",null,null), Row("USA",234.43,null))
    )
  }

  test("test data loading into table whose name has '_'") {
    sql("DROP TABLE IF EXISTS load_test")
    sql(""" CREATE TABLE load_test(id int, name string, city string, age int)
        STORED BY 'org.apache.carbondata.format' """)
    val testData = currentDirectory + "/src/test/resources/sample.csv"
    try {
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table load_test")
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table load_test")
    } catch {
      case ex: Exception =>
        assert(false)
    }
    assert(checkSegmentExists("0", "default", "load_test"))
    assert(checkSegmentExists("1", "default", "load_test"))
    sql("DROP TABLE load_test")
  }

  override def afterAll {
    sql("DROP TABLE loadtest")
  }
}
