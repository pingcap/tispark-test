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


class Tpch19(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def sparkQuery =
    """
      |select
      |	sum(l_extendedprice* (1 - l_discount)) as revenue
      |from
      |	lineitem,
      |	part
      |where
      |	(
      |		p_partkey = l_partkey
      |		and p_brand = 'Brand#12'
      |		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      |		and l_quantity >= 1 and l_quantity <= 1 + 10
      |		and p_size between 1 and 5
      |		and l_shipmode in ('AIR', 'AIR REG')
      |		and l_shipinstruct = 'DELIVER IN PERSON'
      |	)
      |	or
      |	(
      |		p_partkey = l_partkey
      |		and p_brand = 'Brand#23'
      |		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      |		and l_quantity >= 10 and l_quantity <= 10 + 10
      |		and p_size between 1 and 10
      |		and l_shipmode in ('AIR', 'AIR REG')
      |		and l_shipinstruct = 'DELIVER IN PERSON'
      |	)
      |	or
      |	(
      |		p_partkey = l_partkey
      |		and p_brand = 'Brand#34'
      |		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      |		and l_quantity >= 20 and l_quantity <= 20 + 10
      |		and p_size between 1 and 15
      |		and l_shipmode in ('AIR', 'AIR REG')
      |		and l_shipinstruct = 'DELIVER IN PERSON'
      |	)
    """.stripMargin

  override def tidbQuery =
    """
         select
 |        	sum(l_extendedprice* (1 - l_discount)) as revenue
 |        from
 |        	lineitem,
 |        	part
 |        where
 |        	(
 |        		p_partkey = l_partkey
 |        		and p_brand = 'Brand#12'
 |        		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
 |        		and l_quantity >= 1 and l_quantity <= 1 + 10
 |        		and p_size between 1 and 5
 |        		and l_shipmode in ('AIR', 'AIR REG')
 |        		and l_shipinstruct = 'DELIVER IN PERSON'
 |        	)
 |        	or
 |        	(
 |        		p_partkey = l_partkey
 |        		and p_brand = 'Brand#23'
 |        		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
 |        		and l_quantity >= 10 and l_quantity <= 10 + 10
 |        		and p_size between 1 and 10
 |        		and l_shipmode in ('AIR', 'AIR REG')
 |        		and l_shipinstruct = 'DELIVER IN PERSON'
 |        	)
 |        	or
 |        	(
 |        		p_partkey = l_partkey
 |        		and p_brand = 'Brand#34'
 |        		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
 |        		and l_quantity >= 20 and l_quantity <= 20 + 10
 |        		and p_size between 1 and 15
 |        		and l_shipmode in ('AIR', 'AIR REG')
 |        		and l_shipinstruct = 'DELIVER IN PERSON'
 |        	);
    """.stripMargin
}
