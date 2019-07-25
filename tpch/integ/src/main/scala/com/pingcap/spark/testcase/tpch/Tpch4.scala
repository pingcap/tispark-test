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


class Tpch4(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
      select
 |        	o_orderpriority,
 |        	count(*) as order_count
 |        from
 |        	orders
 |        where
 |        	o_orderdate >= '1993-07-01'
 |        	and o_orderdate < '1993-10-01'
 |        	and exists (
 |        		select
 |        			*
 |        		from
 |        			lineitem
 |        		where
 |        			l_orderkey = o_orderkey
 |        			and l_commitdate < l_receiptdate
 |        	)
 |        group by
 |        	o_orderpriority
 |        order by
 |        	o_orderpriority
    """.stripMargin
}
