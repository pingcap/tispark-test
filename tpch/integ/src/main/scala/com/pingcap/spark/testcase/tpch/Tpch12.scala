/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.spark.testcase.tpch

import java.util.Properties

import com.pingcap.spark.TestBase
import org.apache.spark.sql.SparkSession


class Tpch12(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
      |select
      |        	l_shipmode,
      |        	sum(case
      |        		when o_orderpriority = '1-URGENT'
      |        			or o_orderpriority = '2-HIGH'
      |        			then 1
      |        		else 0
      |        	end) as high_line_count,
      |        	sum(case
      |        		when o_orderpriority <> '1-URGENT'
      |        			and o_orderpriority <> '2-HIGH'
      |        			then 1
      |        		else 0
      |        	end) as low_line_count
      |        from
      |        	orders,
      |        	lineitem
      |        where
      |        	o_orderkey = l_orderkey
      |        	and l_shipmode in ('MAIL', 'SHIP')
      |        	and l_commitdate < l_receiptdate
      |        	and l_shipdate < l_commitdate
      |        	and l_receiptdate >= '1994-01-01'
      |        	and l_receiptdate < '1995-01-01'
      |        group by
      |        	l_shipmode
      |        order by
      |        	l_shipmode
    """.stripMargin
}
