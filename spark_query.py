# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('pacient_prescription_procedure').getOrCreate()


pacients_dataframe = spark_session.read.load('hdfs://localhost:9000/pacients.csv', format='csv', inferSchema=True, header=True)
procedures_dataframe = spark_session.read.load('hdfs://localhost:9000/procedures.csv', format='csv', inferSchema=True, header=True)
prescriptions_dataframe = spark_session.read.load('hdfs://localhost:9000/prescriptions.csv', format='csv', inferSchema=True, header=True)

pacients_dataframe.registerTempTable('pacients')
procedures_dataframe.registerTempTable('procedures')
prescriptions_dataframe.registerTempTable('prescriptions')

spark_session.sql("""
    SELECT t1.procedure id, COUNT(*) AS proc_sum
	FROM procedures t1
	LEFT JOIN prescriptions t2
	ON t1.procedure_id=t2.procedure_id
	GROUP BY t1.procedure_id
	ORDER BY COUNT(*) DESC
""").show()

input('Ctrl C')
