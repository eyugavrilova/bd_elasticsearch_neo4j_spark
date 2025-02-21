# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row

es_client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])

def get_pacients():
    pacients = elastic_client.search(
        index='pacients',
        body={
            "size": 30, 
            "_source": ["pacient_id", "personal_data"],
        }    
    )["hits"]["hits"]
    res = []
    for pacient in pacients:
        res.append(
            {
                'pacient_id': pacient["_source"]["pacient_id"],
                'personal_data': pacient["_source"]["personal_data"]
            }
        )
    return res

def get_prescriptions():
    prescriptions = elastic_client.search(
        index='pacients',
        body={
            "size": 30, 
			"_source": ["pacient_id", "procedure_id"],
        }
    )["hits"]["hits"]

    res = []
    for prescription in prescriptions:
        for procedure in prescription["_source"]["procedure_id"]:
			res.append(
				{
					'pacient_id': prescription["_source"]["pacient_id"],
					'procedure_id': procedure
				}
			)
    return res

def get_procedures():
    procedures = elastic_client.search(
        index="procedures",
        body={
            "size": 30
        }
    )["hits"]["hits"]

    res = []
    for procedure in procedures:
        res.append(
            {
                "procedure_id": procedure["_id"],
                "procedure_info" : procedure["_source"]["procedure_info"],
                "cost" : procedure["_source"]["cost"],
            }
        )
    return res


spark_session = SparkSession.builder.appName('pacient_prescription_procedure').getOrCreate()

pacients = spark_session.createDataFrame(get_pacients())
pacients.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('hdfs://localhost:9000/pacients.csv')

prescriptions = spark_session.createDataFrame(get_prescriptions())
prescriptions.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('hdfs://localhost:9000/prescriptions.csv')

procedures = spark_session.createDataFrame(get_procedures())
procedures.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('hdfs://localhost:9000/procedures.csv')
