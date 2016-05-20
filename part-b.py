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
    
    
    ######### Question ##########
    # Find names of people who bought US products but did not buy Chinese products.
    # select distinct buyer from purchase, product, company where product=name and maker=cname and country='us' and buyer not in (select distinct buyer from purchase, product, company where product=name and maker=cname and country='china');
    # select A.buyer from (select distinct buyer from purchase, product, company where product=name and maker=cname and country='us') as A left outer join (select distinct buyer from purchase, product, company where product=name and maker=cname and country='china') as B on A.buyer=B.buyer where B.buyer is null;
    
    sc = SparkContext(appName="Part-b")
    
    #################################### US #######################################
    company = sc.textFile("company.txt").map(lambda x:x.split('\t'))
    # [[u'ibm', u'125.2', u'us'], [u'apple', u'101.3', u'us'], [u'alibaba', u'72.78', u'china']]
    
    company_us = company.filter(lambda x: "us" in x)
    # [[u'ibm', u'125.2', u'us'], [u'apple', u'101.3', u'us']]
    
    company_us_key = company_us.map(lambda x:(x[0],x[1:]))
    # [(u'ibm', [u'125.2', u'us']), (u'apple', [u'101.3', u'us'])]
    
    
    product = sc.textFile("product.txt").map(lambda x:x.split('\t'))
    # [[u'iphone 5', u'538.3', u'cell phone', u'apple'], [u'ms office 2010', u'300', u'software', u'microsoft']]
    
    product_key = product.map(lambda x:(x[3],x[0:3]))
    # [(u'apple', [u'iphone 5', u'538.3', u'cell phone'])
    
    join1 = product_key.join(company_us_key)
    # [(u'apple', ([u'iphone 5', u'538.3', u'cell phone'], [u'101.3', u'us']))]
    
    key_category = join1.map(lambda x:(x[1][0][0],x[0]))
    # [(u'iphone 5', u'apple'), (u'iphone 6', u'apple')]
    
    
    
    purchase = sc.textFile("purchase.txt").map(lambda x:x.split('\t'))
    # [[u'mary', u'john', u'Amazon', u'iphone 6'], [u'mary', u'david', u'Ebay', u'ms office 2013']]
    
    purchase_key = purchase.map(lambda x:(x[3],x[0:3]))
    # [(u'iphone 6', [u'mary', u'john', u'Amazon']), (u'ms office 2013', [u'mary', u'david', u'Ebay'])]
    
    join1_1 = purchase_key.join(key_category)
    # [(u'windows 10', ([u'bill', u'david', u'Ebay'], u'microsoft'))]
    
    output_us = join1_1.map(lambda x:x[1][0][0]).distinct().collect()
    # [u'bill', u'mark', u'steve', u'mary']
    
    
    ################################### China #####################################
    
    company_china = company.filter(lambda x: "china" in x)
    company_china_key = company_china.map(lambda x:(x[0],x[1:]))
    join2 = product_key.join(company_china_key)
    key_category2 = join2.map(lambda x:(x[1][0][0], x[0]))
    join2_2 = purchase_key.join(key_category2)
    output_china = join2_2.map(lambda x:x[1][0][0]).distinct().collect()
    # [u'bill', u'steve', u'mary']
    
    
    output = list(set(output_us) - set(output_china))
    # [u'mark']
    
    print(output)

    sc.stop()
