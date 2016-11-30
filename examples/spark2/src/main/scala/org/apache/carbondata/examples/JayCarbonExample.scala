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

package org.apache.carbondata.examples

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
//import org.apache.carbondata.examples.util.ExampleUtils

object JayCarbonExample {
  def main(args: Array[String]) {
//    val cc = ExampleUtils.createCarbonContext("CarbonExample")
//    val dwcjk = ExampleUtils.currentPath + "/src/main/resources/dwcjk.csv"
//    val dwzqxx = ExampleUtils.currentPath + "/src/main/resources/dwzqxx.csv"
//    val swtnialk = ExampleUtils.currentPath + "/src/main/resources/swtnialk.csv"
//    val wwtnbrch = ExampleUtils.currentPath + "/src/main/resources/wwtnbrch.csv"
//
//    // Specify timestamp format based on raw data
//    CarbonProperties.getInstance()
//      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
//    CarbonProperties.getInstance()
//      .addProperty(CarbonCommonConstants.ENABLE_USE_AGG_TABLE, "true")
//    CarbonProperties.getInstance()
//      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")

    //    cc.sql("alter table dwcjk compact 'Minor'")

    //    cc.sql("DROP TABLE IF EXISTS dwcjk")
    //
//        cc.sql("""
//               CREATE TABLE IF NOT EXISTS dwcjk
//               (CJRQ timestamp, CJXH string, ZQDH String, BXWDH String, BGDDM String, BHTXH String, BRZRQ String,
//               BPCBZ String, SXWDH String, SGDDM String, SHTXH String, SRZRQ String, SPCBZ String, CJGS int,
//               CJJG decimal(9,3), CJSJ int, YWLB String, MMLB String, FBBZ String, FILLER String)
//               STORED BY 'carbondata'
//               TBLPROPERTIES('DICTIONARY_INCLUDE'='CJSJ')
//               """)
    ////    ////
    //        cc.sql("drop table if exists dwzqxx")
    //        cc.sql("create table dwzqxx (zqdh string) stored by 'carbondata'")
    //        cc.sql("drop table if exists wwtnbrch")
    //        cc.sql("create table wwtnbrch(brch_nm string, brchid_cd string, brch_cd string) stored by 'carbondata'")
    //        cc.sql("drop table if exists swtnialk")
    //        cc.sql("create table swtnialk(inv_cd string) stored by 'carbondata'")
    //    //////
//            for(i <- 0 until 4) {
//              cc.sql(s"""
//                   LOAD DATA LOCAL INPATH '$dwcjk' into table dwcjk
//                   """)
//            }
    //        for(i <- 0 until 3) {
    //          cc.sql(s"""
    //               LOAD DATA LOCAL INPATH '$dwzqxx' into table dwzqxx
    //               """)
    //        }
    //        for(i <- 0 until 3) {
    //          cc.sql(s"""
    //               LOAD DATA LOCAL INPATH '$wwtnbrch' into table wwtnbrch
    //               """)
    //        }
    //        for(i <- 0 until 3) {
    //          cc.sql(s"""
    //               LOAD DATA LOCAL INPATH '$swtnialk' into table swtnialk
    //                   """)
    //        }

    //    cc.sql("DROP table IF EXISTS dwcjk_agg_0")
    //
//        cc.sql("""
//               create table dwcjk_agg stored by 'carbondata'
//               as select cjrq, cjsj, zqdh, bhtxh as ybdm, bxwdh as jydy, bgddm as gddm, mmlb,
//               sum(cjgs) as cjgs, sum(cjgs*cjjg) as cjje
//               from dwcjk
//               where mmlb<>'c'
//               group by cjrq, cjsj, zqdh, bxwdh, bgddm, bhtxh, mmlb
//               """)
    //    cc.sql("SELECT  * from dwcjk").show()
    //    cc.sql("SELECT  * from dwcjk_agg_0").show()
    //        cc.sql("DROP table IF EXISTS dwcjk_agg_1")
    //
    //    cc.sql("""
    //           create table dwcjk_agg stored by 'carbondata'
    //           as select cjrq, cjsj, zqdh, shtxh as ybdm, sxwdh as jydy, sgddm as gddm, mmlb,
    //           sum(cjgs) as cjgs, sum(cjgs*cjjg) as cjje
    //           from dwcjk
    //           where mmlb<>'c'
    //           group by cjrq, cjsj, zqdh, sxwdh, sgddm, shtxh, mmlb
    //           """)
    ////    cc.sql(
    //      """
    //        show tables
    //      """.stripMargin).show()



    //    cc.sql(
    //      """
    //      select a.zqdh, a.ybdm, brch_nm, brchid_cd, a.jydy, a.gddm, a.mmlb, cjgs, a.cjje, cjjj
    //      from (
    //         select *
    //           from (select zqdh,
    //                    bhtxh as ybdm,
    //                    bxwdh as jydy,
    //                    bgddm as gddm,
    //                    mmlb as mmlb,
    //                   sum(cjgs) as cjgs,
    //                   sum(cjgs*cjjg) as cjje,
    //                   case when sum(cjgs)=0 then 0 else sum(cjgs*cjjg)/sum(cjgs) end as cjjj
    //                   from dwcjk
    //                   where cjsj>=1000000
    //                        and cjsj<=8000000
    //                       and mmlb<>'c'
    //                       and cjrq>='2016/07/01'
    //                       and cjrq<='2017/09/05'
    //                   group by zqdh, bxwdh, bgddm, bhtxh, mmlb
    //                   order by cjgs desc
    //                   limit 10000) cjk1
    //               union all
    //          select *
    //              from ( select zqdh,
    //                            shtxh as ybdm,
    //                            sxwdh as jydy,
    //                            sgddm as gddm,
    //                            mmlb as mmlb,
    //                           sum(cjgs) as cjgs, sum(cjgs*cjjg) as cjje,
    //                           case when sum(cjgs)=0 then 0 else sum(cjgs*cjjg)/sum(cjgs) end as cjjj
    //                           from dwcjk
    //                           where cjsj>=1000000
    //                               and cjsj<=8000000
    //                               and mmlb<>'c'
    //                               and cjrq>='2016/07/01'
    //                               and cjrq<='2017/09/05'
    //                           group by zqdh, sxwdh, sgddm, shtxh, mmlb
    //                           order by cjgs desc
    //                           limit 10000) cjk2
    //           ) a
    //           left join dwzqxx c on a.zqdh = c.zqdh
    //           left join wwtnbrch d on a.ybdm = d.brch_cd
    //           left join swtnialk e on a.gddm = e.inv_cd
    //      """).show()


  }
}
