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
package org.apache.carbondata.common.ext;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.service.PathService;

/**
 * Create helper to get path details
 */
public class PathFactory implements PathService {

  private static PathService pathService = new PathFactory();

  /**
   * @param storeLocation
   * @param tableIdentifier
   * @return store path related to tables
   */
  @Override public CarbonTablePath getCarbonTablePath(String storeLocation,
            CarbonTableIdentifier tableIdentifier) {
    return CarbonStorePath.getCarbonTablePath(storeLocation,
            getMainTableIdentifierInCaseOfAggTable(tableIdentifier));
  }

  /**
   *  add to identify aggregate table's main table, the can get dictionary file.
   * @param tableIdentifier
   * @return
     */
  private CarbonTableIdentifier getMainTableIdentifierInCaseOfAggTable(
          CarbonTableIdentifier tableIdentifier) {
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
            tableIdentifier.getTableUniqueName());
    if (carbonTable.isAggTable()) {
      CarbonTable mainTable = CarbonMetadata.getInstance().getCarbonTable(
              tableIdentifier.getDatabaseName() + CarbonCommonConstants.UNDERSCORE +
                      carbonTable.getMainTableName());
      tableIdentifier = mainTable.getCarbonTableIdentifier();
    }
    return tableIdentifier;
  }

  public static PathService getInstance() {
    return pathService;
  }
}
