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


class Tpch20(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
                 select
 |        	s_name,
 |        	s_address
 |        from
 |        	supplier,
 |        	nation
 |        where
 |        	s_suppkey in (
 |        		select
 |        			ps_suppkey
 |        		from
 |        			partsupp
 |        		where
 |        			ps_partkey in (
 |        				select
 |        					p_partkey
 |        				from
 |        					part
 |        				where
 |        					p_name like 'forest%'
 |        			)
 |        			and ps_availqty > (
 |        				select
 |        					0.5 * sum(l_quantity)
 |        				from
 |        					lineitem
 |        				where
 |        					l_partkey = ps_partkey
 |        					and l_suppkey = ps_suppkey
 |        					and l_shipdate >= '1994-01-01'
 |        					and l_shipdate < '1995-01-01')
 |        	)
 |        	and s_nationkey = n_nationkey
 |        	and n_name = 'CANADA'
 |        order by
 |        	s_name
    """.stripMargin
}
