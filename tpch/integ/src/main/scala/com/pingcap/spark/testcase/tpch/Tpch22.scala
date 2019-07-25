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


class Tpch22(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
       select
 |	cntrycode,
 |	count(*) as numcust,
 |	sum(c_acctbal) as totacctbal
 | from
 |	(
 |		select
 |			substring(c_phone, 1 , 2) as cntrycode,
 |			c_acctbal
 |		from
 |			customer
 |		where
 |			substring(c_phone , 1 , 2) in
 |				('20', '40', '22', '30', '39', '42', '21')
 |			and c_acctbal > (
 |				select
 |					avg(c_acctbal)
 |				from
 |					customer
 |				where
 |					c_acctbal > 0.00
 |					and substring(c_phone , 1 , 2) in
 |						('20', '40', '22', '30', '39', '42', '21')
 |			)
 |			and not exists (
 |				select
 |					*
 |				from
 |					orders
 |				where
 |					o_custkey = c_custkey
 |			)
 |	) as custsale
 | group by
 |	cntrycode
 | order by
 |	cntrycode
    """.stripMargin
}
