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
    
    ############ Question ##############
    # Find the number of products sold for each product category and buyer city, e.g., cell phone, los angeles, 5; cell phone, san gabriel, 3; etc.;
    # select city, category, count(*) from Purchase, Product, Person where product=Product.name and buyer=Person.name group by category, city;
    
    
    sc = SparkContext(appName="Part-c")
    
    person = sc.textFile("person.txt").map(lambda x:x.split('\t'))
    # [[u'mary', u'(323)138-8588', u'los angeles'], [u'john', u'(626)123-4586', u'san gabriel']]
    
    person_key = person.map(lambda x:(x[0],x[1:]))
    # [(u'mary', [u'(323)138-8588', u'los angeles']), (u'john', [u'(626)123-4586', u'san gabriel'])]
    
    
    purchase = sc.textFile("purchase.txt").map(lambda x:x.split('\t'))
    # [[u'mary', u'john', u'Amazon', u'iphone 6'], [u'mary', u'david', u'Ebay', u'ms office 2013']]
    
    purchase_key = purchase.map(lambda x:(x[0],x[1:]))
    # [[(u'mary', [u'john', u'Amazon', u'iphone 6']), (u'mary', [u'david', u'Ebay', u'ms office 2013'])]
    
    join = person_key.join(purchase_key)
    # [(u'steve', ([u'(323)121-3838', u'alhambra'], [u'john', u'Bestbuy', u'iphone 6']))]
    
    output_product_city = join.map(lambda x:(x[1][1][2],x[1][0][1]))
    # [(u'iphone 5', u'alhambra'), (u'iphone 6', u'los angeles')]
    

    product = sc.textFile("product.txt").map(lambda x:x.split('\t'))
    # [[u'iphone 5', u'538.3', u'cell phone', u'apple'], [u'ms office 2010', u'300', u'software', u'microsoft']]
    
    product_key = product.map(lambda x:(x[0],x[1:]))
    # [(u'iphone 5', [u'538.3', u'cell phone', u'apple']), (u'iphone 6', [u'600.8', u'cell phone', u'apple'])]
    
    join2 = product_key.join(output_product_city)
    # [(u'windows 10', ([u'300', u'software', u'microsoft'], u'alhambra'))]
    
    output_city_category = join2.map(lambda x:(x[1][1],x[1][0][1]))
    # [(u'alhambra', u'software'), (u'los angeles', u'software'), (u'alhambra', u'cell phone')]
    
    output = output_city_category.map(lambda x:(x,1)).countByKey()
    # defaultdict(<type 'int'>, {(u'alhambra', u'cell phone'): 2, (u'los angeles', u'cell phone'): 3, (u'alhambra', u'software'): 2, (u'alhambra', u'laptop'): 1, (u'los angeles', u'laptop'): 3, (u'los angeles', u'software'): 8})

    print(output)

    sc.stop()
