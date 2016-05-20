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
    
    ##### Question #######
    # Find names of people who bought cell phones.
    # select distinct buyer from purchase, product where product=name and category='cell phone'; 
    
    sc = SparkContext(appName="Part-a")
    
    product = sc.textFile("product.txt").map(lambda x:x.split('\t'))
    # [[u'iphone 5', u'538.3', u'cell phone', u'apple'], [u'ms office 2010', u'300', u'software', u'microsoft']]
    
    product_cellphone = product.filter(lambda x: "cell phone" in x[2])
    # [[u'iphone 5', u'538.3', u'cell phone', u'apple'], [u'iphone 6', u'600.8', u'cell phone', u'apple']]
    
    product_cellphone_key = product_cellphone.map(lambda x:(x[0],x[1:]))
    # [(u'iphone 5', [u'538.3', u'cell phone', u'apple'])]
    
    
    purchase = sc.textFile("purchase.txt").map(lambda x:x.split('\t'))
    # [[u'mary', u'john', u'Amazon', u'iphone 6'], [u'mary', u'david', u'Ebay', u'ms office 2013']]
    
    purchase_key = purchase.map(lambda x:(x[3],x[0:3]))
    # [(u'iphone 6', [u'mary', u'john', u'Amazon'])]

    join_product_purchase = product_cellphone_key.join(purchase_key)
    # [(u'iphone 6', ([u'600.8', u'cell phone', u'apple'], [u'mary', u'john', u'Amazon']))]
    
    output = join_product_purchase.map(lambda x:x[1][1][0]).distinct().collect()
    # [u'steve', u'mary', u'bill', u'mark']
    
    print(output)

    sc.stop()
