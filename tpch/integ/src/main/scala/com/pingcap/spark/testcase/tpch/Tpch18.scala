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

class Tpch18(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
 select
 |            	c_name,
 |            	c_custkey,
 |            	o_orderkey,
 |            	o_orderdate,
 |            	o_totalprice,
 |            	sum(l_quantity)
 |            from
 |            	customer,
 |            	orders,
 |            	lineitem
 |            where
 |            	o_orderkey in (
 |            		select
 |            			l_orderkey
 |            		from
 |            			lineitem
 |            		group by
 |            			l_orderkey having
 |            				sum(l_quantity) > 300
 |            	)
 |            	and c_custkey = o_custkey
 |            	and o_orderkey = l_orderkey
 |            group by
 |            	c_name,
 |            	c_custkey,
 |            	o_orderkey,
 |            	o_orderdate,
 |            	o_totalprice
 |            order by
 |            	o_totalprice desc,
 |            	o_orderdate
    """.stripMargin
}
