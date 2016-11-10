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
package org.apache.carbondata.spark.aggregatetable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;
import org.apache.carbondata.spark.load.CarbonLoaderUtil;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import org.pentaho.di.core.exception.KettleException;

/**
 * Executor class for executing the query on the selected segments to be loaded into agg table.
 * This will fire a select * query and get the raw result.
 */
public class AggregateTableExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AggregateTableExecutor.class.getName());

  private CarbonLoadModel carbonLoadModel;

  private SortDataRows sortDataRows;

  private int[] columnCardinality;

  private String segmentId;

  private String aggTableName;

  private String dataBaseName;

  private SegmentProperties segmentProperties;

  private CarbonTable aggTable;

  private int measureCount;

  private int dictDimensionCount;

  private int noDictionaryCount;

  private int complexDimensionCount;

  private boolean[] noDictionaryMapping;

  private String tempStoreLocation;

  private char[] aggType;

  private SingleThreadFinalSortFilesMerger finalMerger;

  private CarbonFactDataHandlerColumnar dataHandler;

  DirectDictionaryGenerator directDictionaryGenerator = null;

  public AggregateTableExecutor(CarbonLoadModel carbonLoadModel,
                                int[] columnCardinality,
                                String segmentId,
                                String aggTableName) {
    this.carbonLoadModel = carbonLoadModel;
    this.columnCardinality = columnCardinality;
    this.segmentId = segmentId;
    this.aggTableName = aggTableName;
    this.dataBaseName = carbonLoadModel.getDatabaseName();
    this.aggTable = CarbonMetadata.getInstance().getCarbonTable(
        this.dataBaseName + CarbonCommonConstants.UNDERSCORE + this.aggTableName);
    this.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(DataType.TIMESTAMP,
            CARBON_TIMESTAMP_DEFAULT_FORMAT);
    initSegmentProperties();
  }

  public void processQueryResult(Iterator<Row> iterator) {
    initSortDataRows();
    processResult(iterator);
    initFinalThreadMergerForMergeSort();
    initDataHandler();
    readAndLoadDataFromSortTempFiles();
  }

  private void initSortDataRows() {
    measureCount = aggTable.getMeasureByTableName(aggTableName).size();
    SortObserver sortObserver = new SortObserver();
    List<CarbonDimension> dimensions = aggTable.getDimensionByTableName(aggTableName);
    noDictionaryMapping = new boolean[dimensions.size()];
    int i = 0;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.hasEncoding(Encoding.DICTIONARY)) {
        i++;
        continue;
      }
      noDictionaryMapping[i++] = true;
      noDictionaryCount++;
    }
    dictDimensionCount = dimensions.size() - noDictionaryCount;
    SortParameters parameters =
        SortParameters.createSortParameters(dataBaseName, aggTableName,
            dictDimensionCount, complexDimensionCount, measureCount,
            sortObserver, noDictionaryCount, carbonLoadModel.getPartitionId(),
            segmentId + "", carbonLoadModel.getTaskNo(), noDictionaryMapping);
    SortIntermediateFileMerger intermediateFileMerger = new SortIntermediateFileMerger(parameters);
    this.sortDataRows = new SortDataRows(parameters, intermediateFileMerger);
    try {
      sortDataRows.initialize();
    } catch (CarbonSortKeyAndGroupByException ex) {
      LOGGER.error(ex);
    }
  }

  private void processResult(Iterator<Row> iterator) {
    try {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        sortDataRows.addRow(changeRowToObject(row));
      }
      sortDataRows.startSorting();
    } catch (CarbonSortKeyAndGroupByException ex) {
      LOGGER.error(ex);
    }
  }

  private Object[] changeRowToObject(Row row) {
    Object[] newArray = new Object[CarbonCommonConstants.ARRAYSIZE];
    ByteBuffer[] byteBufferArr = new ByteBuffer[noDictionaryCount + complexDimensionCount];
    int index = 0;
    int outIndex = 0;
    Object[] out = new Object[dictDimensionCount + measureCount];
    StructType types = row.schema();
    for (int i = 0; i < row.size(); i++) {
      Object value = row.get(i);
      if (i < dictDimensionCount + noDictionaryCount) {
        if (noDictionaryMapping[i]) {
          ByteBuffer buffer = ByteBuffer
              .wrap(value.toString().getBytes(Charset.forName(
                  CarbonCommonConstants.DEFAULT_CHARSET)));
          buffer.rewind();
          byteBufferArr[index++] = buffer;
        } else {
          if (value instanceof java.sql.Timestamp) {
            out[outIndex++] = Integer.valueOf(
                directDictionaryGenerator.generateDirectSurrogateKey(value.toString()));
          } else {
            out[outIndex++] = Integer.valueOf(value.toString());
          }
        }
      } else {
        out[outIndex++] = value;
      }
    }
    RemoveDictionaryUtil
        .prepareOut(newArray, byteBufferArr, out, dictDimensionCount);
    return newArray;
  }


  private void initFinalThreadMergerForMergeSort() {
    initTempStoreLocation();
    String sortTempFileLocation = tempStoreLocation + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    initAggType();
    finalMerger = new SingleThreadFinalSortFilesMerger(sortTempFileLocation, aggTableName,
        dictDimensionCount, complexDimensionCount, measureCount, noDictionaryCount, aggType,
        noDictionaryMapping, true);
  }

  private void initDataHandler() {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonLoaderUtil
        .getCarbonFactDataHandlerModel(carbonLoadModel,
            segmentProperties, dataBaseName, aggTableName,
            tempStoreLocation, carbonLoadModel.getStorePath(), aggType);
    carbonFactDataHandlerModel.setPrimitiveDimLens(segmentProperties.getDimColumnsCardinality());
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Integer.parseInt(carbonLoadModel.getTaskNo()),
            carbonLoadModel.getFactTimeStamp());
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    if (segmentProperties.getNumberOfNoDictionaryDimension() > 0 ||
        segmentProperties.getComplexDimensions().size() > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount);
    }
    carbonFactDataHandlerModel.setColCardinality(columnCardinality);
    carbonFactDataHandlerModel.setBlockSizeInMB(aggTable.getBlockSizeInMB());
    String carbonDataDirectoryPath =
        checkAndCreateCarbonStoreLocation(carbonLoadModel.getStorePath(), dataBaseName,
            aggTableName, carbonLoadModel.getPartitionId(), segmentId);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
    try {
      dataHandler.initialise();
    } catch (CarbonDataWriterException ex) {
      LOGGER.error(ex);
      throw ex;
    }

  }

  private String checkAndCreateCarbonStoreLocation(String factStoreLocation, String databaseName,
      String tableName, String partitionId, String segmentId) {
    String carbonStorePath = factStoreLocation;
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId);
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
    return carbonDataDirectoryPath;
  }

  private void readAndLoadDataFromSortTempFiles() {
    try {
      finalMerger.startFinalMerge();
      while (finalMerger.hasNext()) {
        Object[] row = finalMerger.next();
        Object[] outputRow = CarbonLoaderUtil.process(
            row, noDictionaryCount, complexDimensionCount, measureCount,
            aggType, segmentProperties);
        dataHandler.addDataToStore(outputRow);
      }
      dataHandler.finish();
    } catch (KettleException ex) {
      LOGGER.error(ex);
    }
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (CarbonDataWriterException ex) {
        LOGGER.error(ex);
        throw ex;
      }
    }
  }

  private void initTempStoreLocation() {
    tempStoreLocation = CarbonDataProcessorUtil.getLocalDataFolderLocation(dataBaseName,
        aggTableName, carbonLoadModel.getTaskNo(), carbonLoadModel.getPartitionId(),
        segmentId, false);
  }

  private void initSegmentProperties() {
    List<ColumnSchema> columnSchemaList = CarbonUtil
        .getColumnSchemaList(aggTable.getDimensionByTableName(aggTableName),
            aggTable.getMeasureByTableName(aggTableName));
    segmentProperties = new SegmentProperties(columnSchemaList, columnCardinality);
  }

  private void initAggType() {
    aggType = new char[measureCount];
    Arrays.fill(aggType, 'n');
    List<CarbonMeasure> measures = aggTable.getMeasureByTableName(aggTableName);
    for (int i = 0; i < measureCount; i++) {
      aggType[i] = DataTypeUtil.getAggType(measures.get(i).getDataType());
    }
  }
}
