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

package org.apache.carbondata.spark.rdd

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.LoadMetadataDetails
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.aggregatetable.AggregateTableExecutor
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.util.Random


class CarbonAggTableRDD[K, V](
     rdd: RDD[Row],
     sc: SparkContext,
     carbonLoadModel: CarbonLoadModel,
     loadResult: DataLoadResult[K, V],
     segmentId: String,
     colCardinality: Array[Int])
  extends RDD[(K, V)](rdd) {

  carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val resultIterator = new Iterator[(K, V)] {
      // need to handle record table status
      val loadMetadataDetails = new LoadMetadataDetails()
      val uniqueLoadStatusId = carbonLoadModel.getAggTableName +
        CarbonCommonConstants.UNDERSCORE + theSplit.index
      loadMetadataDetails.setPartitionCount("0")
      loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
      carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
      // this property is used to determine whether temp location for carbon is inside
      // container temp dir or is yarn application directory.
      var tempStoreLocation: String = ""
      val carbonUseLocalDir = CarbonProperties.getInstance()
        .getProperty("carbon.use.local.dir", "false")
      if(carbonUseLocalDir.equalsIgnoreCase("true")) {
        val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != storeLocations && storeLocations.nonEmpty) {
          tempStoreLocation = storeLocations(Random.nextInt(storeLocations.length))
        }
        if (tempStoreLocation == null) {
          tempStoreLocation = System.getProperty("java.io.tmpdir")
        }
      }
      else {
        tempStoreLocation = System.getProperty("java.io.tmpdir")
      }
      tempStoreLocation = tempStoreLocation + '/' + System.nanoTime() + '/' + theSplit.index
      LOGGER.info(s"temp store location for aggregate table loading is $tempStoreLocation")
      carbonLoadModel.setPartitionId("0")
      carbonLoadModel.setSegmentId(segmentId)
      val aggregateTableExecutor = new AggregateTableExecutor(
        carbonLoadModel,
        colCardinality,
        segmentId,
        carbonLoadModel.getAggTableName)
      val queryIterator = firstParent[Row].iterator(theSplit, context)
      try {
        aggregateTableExecutor.processQueryResult(queryIterator.asJava)
      } catch {
        case ex: Exception =>
          throw ex
      }
      var finished = false

      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        loadResult.getKey(uniqueLoadStatusId, loadMetadataDetails)
      }
    }
    resultIterator
  }

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

}

