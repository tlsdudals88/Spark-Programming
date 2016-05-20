#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    
    ############### Question #################
    # Find the number of different stores in purchase table. For this part, you are required to use mapPartitions, but do not use Spark's distinct and counting functions including: distinct(), count(), and countByKey()
    # select count(distinct store) from purchase;
    
    def lenf(iterator):
        yield len(list(iterator))
    
    def aggr(x,y):
        return (x+y)

    sc = SparkContext(appName="Part-e")
    
    purchase = sc.textFile("purchase.txt").map(lambda x:x.split('\t'))
    # [[u'mary', u'john', u'Amazon', u'iphone 6'], [u'mary', u'david', u'Ebay', u'ms office 2013']]
    
    purchase_store = purchase.map(lambda x:x[2])
    # [u'Amazon', u'Ebay', u'Amazon', u'Ebay', u'Amazon']
    
    purchase_store_count = purchase_store.map(lambda x: (x, 1)).reduceByKey(add)
    # [(u'Amazon', 7), (u'Bestbuy', 6), (u'Ebay', 6)]


    #################### (1) using reduceByKey ###########################
    # purchase_store_distinct = purchase_store_count.map(lambda x:("same_key",1))
    # [('same_key', 1), ('same_key', 1), ('same_key', 1)]

    # purchase_store_distinct_total = purchase_store_distinct.map(lambda x: (x, 1)).reduceByKey(add)
    # [(('same_key', 1), 3)]

    # output = purchase_store_distinct_total.map(lambda x:x[1]).collect()
    # [3]

    # print(output)
    ######################################################################


    #################### (2) using mapPartitions #########################
    only_count = purchase_store_count.map(lambda x: x[1])
    # [7, 6, 6]

    mapPartitions_length = only_count.mapPartitions(lenf)
    # [1, 2] (default: 2 partition)
    
    output = mapPartitions_length.reduce(aggr)
    # 3
    
    print(output)
    ######################################################################

    sc.stop()
