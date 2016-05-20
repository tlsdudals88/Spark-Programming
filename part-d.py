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
    # Find the average price of products for each product category.
    # select category,avg(price) from product group by category;

    sc = SparkContext(appName="Part-d")

    product = sc.textFile("product.txt").map(lambda x:x.split('\t'))
    # [[u'iphone 5', u'538.3', u'cell phone', u'apple'], [u'ms office 2010', u'300', u'software', u'microsoft']]
    
    product_category_price = product.map(lambda x:(x[2],float(x[1])))
    # [(u'cell phone', 538.3), (u'cell phone', 600.8), (u'cell phone', 400.0)]
    
    product_sum_count = product_category_price.aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
    # [(u'cell phone', (2239.63, 4)), (u'laptop', (3400.8, 4)), (u'software', (384000.0, 6))]

    product_avg = product_sum_count.map(lambda(x,(y,z)):(x,float((y)/z))).collect()
    # [(u'cell phone', 559.9075), (u'laptop', 850.2), (u'software', 64000.0)]

    print(product_avg)

    sc.stop()
