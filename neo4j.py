# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

from py2neo import Graph, Node, Relationship



es_client = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])



neo_client = Graph(password = '123321')



neo_client.delete_all()

#получение даных о всех пациентах

all_pacients = es_client.search(

    index='pacients',

    body={

	"size": 30, 

	"aggs": {

		"product_id": {

		  "terms": {

			  "field": "_id"

		  }

		}

	}

    }

)



#стоимость процедуры по id

def get_cost_by_proc_id(proc_id):

    result = es_client.search(

        index='procedures',

        body={

		"_source": "cost",

		  "query": {

			  "term": {

				  "_id": {

					  "value": proc_id

				  }

			  }

		  }

        }

    )

    return result['hits']['hits'][0]['_source']['cost']



for pacient in all_pacients['hits']['hits']:

	pac_id = pacient['_source']['pacient_id']

	arr_date = pacient['_source']['arriving_date']

	pers_data = pacient['_source']['personal_data']



    	pacient_node = Node('Pacient', Pac_id=pac_id, Arr_date = arr_date, Pers_data = pers_data)

    	neo_client.create(pacient_node)

	

	proc_ids = pacient['_source']['procedure_id']

	



    	for proc_id in proc_ids:

		cost = get_cost_by_proc_id(proc_id)

        

		procedure_node = neo_client.nodes.match('Procedure', Procedure_id=proc_id).first()

		

		if procedure_node is None:

			procedure_node = Node('Procedure', Procedure_id=proc_id)

			neo_client.create(procedure_node)

	



		pacient_procedure_relationship = Relationship(pacient_node, 'Took', procedure_node, Cost=cost)

		neo_client.create(pacient_procedure_relationship)