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


class Tpch14(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """

 |        select
 |        	100.00 * sum(case
 |        		when p_type like 'PROMO%'
 |        			then l_extendedprice * (1 - l_discount)
 |        		else 0
 |        	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 |        from
 |        	lineitem,
 |        	part
 |        where
 |        	l_partkey = p_partkey
 |        	and l_shipdate >= '1995-09-01'
 |        	and l_shipdate < '1995-10-01'
    """.stripMargin
}
