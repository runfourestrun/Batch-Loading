import json
from collections import defaultdict
from connection import connection







def read_json_file(json_file):
    '''
    Reads Json File and yields a generator for a json object
    :param json_file:
    :return:
    '''
    with open(json_file,'r') as f:
        for line in f:
            json_object = json.loads(line)
            yield json_object




def generate_parameter_data(json_object,properties:list):
    '''
    creates a dictionary from a json object based on if the key is in the list
    :param json_object: a single json object
    :param properties: a list of strings that coorespond to keys in the dict/json
    :return:
    '''
    parameter = {key:value for key,value in json_object.items() if key in properties}
    yield parameter




def chunk_parameters(parameters:list):
    chunks = [parameters[x:x+100] for x in range(0, len(parameters),100)]
    print(chunks)




def generate_cypher(parameters):
    '''
    default_dict_parameters = defaultdict(list)
    default_dict_parameters['params'] = parameters
    dict_parameters = str(dict(default_dict_parameters))
    '''


    _parameters_key= 'params'
    _parameters = defaultdict(list)
    _parameters[_parameters_key] = parameters
    _parameters = str(dict(_parameters))

    unwind_string =  _parameters + '\n ' + 'UNWIND $params as param'





    base_query = "CREATE (u:User {{ {properties} }})"
    properties = ', '.join(('{0}: param.{0}'.format(v) for v in parameters[0]))
    final_templated_base_query = base_query.format(properties=properties)

    final_cypher_query = unwind_string + '\n'  + final_templated_base_query
    return final_cypher_query












if __name__ == '__main__':
    json_file =  '/Users/alexanderfournier/PycharmProjects/json_neo4j/input/nyt2.json'
    author_properties = ['author']
    author_parameter_list = []


    for json_object in read_json_file(json_file):
        for _dict in generate_parameter_data(json_object,author_properties):
            author_parameter_list.append(_dict)





    cypher_statement = generate_cypher(author_parameter_list)



    connection  = connection.Neo4j(url='bolt://localhost:7687',database='test',password='Reddit123!',username='neo4j')

    #connection.write(cypher_statement)












