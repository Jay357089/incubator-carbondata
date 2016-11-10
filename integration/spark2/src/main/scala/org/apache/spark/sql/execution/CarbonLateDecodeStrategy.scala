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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{CarbonDictionaryCatalystDecoder, CarbonDictionaryCatalystIntType, CarbonDictionaryDecoder, CarbonDictionaryIntToString}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Carbon strategy for late decode (convert dictionary key to value as late as possible), which
 * can improve the aggregation performance and reduce memory usage
 */
private[sql] class CarbonLateDecodeStrategy extends SparkStrategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
        CarbonDictionaryDecoder(relations,
          profile,
          aliasMap,
          planLater(child)
        ) :: Nil
      case CarbonDictionaryCatalystIntType(relations, profile, aliasMap, _, child) =>
        CarbonDictionaryIntToString(relations,
          profile,
          aliasMap,
          planLater(child)) :: Nil
      case _ => Nil
    }
  }

}
