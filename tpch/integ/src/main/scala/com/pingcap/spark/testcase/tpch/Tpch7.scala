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


class Tpch7(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {
  override def tidbQuery =
    """
      |select
      |	supp_nation,
      |	cust_nation,
      |	l_year,
      |	sum(volume) as revenue
      |from
      |	(
      |		select
      |			n1.n_name as supp_nation,
      |			n2.n_name as cust_nation,
      |			extract(year from l_shipdate) as l_year,
      |			l_extendedprice * (1 - l_discount) as volume
      |		from
      |			supplier,
      |			lineitem,
      |			orders,
      |			customer,
      |			nation n1,
      |			nation n2
      |		where
      |			s_suppkey = l_suppkey
      |			and o_orderkey = l_orderkey
      |			and c_custkey = o_custkey
      |			and s_nationkey = n1.n_nationkey
      |			and c_nationkey = n2.n_nationkey
      |			and (
      |				(n1.n_name = 'JAPAN' and n2.n_name = 'INDIA')
      |				or (n1.n_name = 'INDIA' and n2.n_name = 'JAPAN')
      |			)
      |			and l_shipdate between '1995-01-01' and '1996-12-31'
      |	) as shipping
      |group by
      |	supp_nation,
      |	cust_nation,
      |	l_year
      |order by
      |	supp_nation,
      |	cust_nation,
      |	l_year
    """.stripMargin
}
