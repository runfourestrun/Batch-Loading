import json
from connection import connection
from neo4j import GraphDatabase






def read_json_file(json_file_path):
    '''
    Reads Json File and yields a generator
    :param json_file:
    :return:
    '''
    with open(json_file_path,'r') as f:
        for line in f:
            json_object = json.loads(line)
            yield json_object




def generate_parameter_data(json_object,properties:list):
    '''
    creates a dictionary from a json object. based on if the key is in the list
    :param json_object: a single json object
    :param properties: a list of strings that coorespond to keys in the dict/json
    :return:
    '''
    parameter = {key:value for key,value in json_object.items() if key in properties}
    yield parameter



'''
todo: I feel like it's kind of confusing to call the functions inside this function... should I create a decorator/wrapper? 
'''
def generate_properties(acceptable_properties:list,json_file_path:str):
    '''
    acceptable properties
    :param acceptable_properties: list of properties that we want to extract from the json
    :return: list of parameters
    '''
    parameter_list = []
    for json_object in read_json_file(json_file_path):
        for _dict in generate_parameter_data(json_object, acceptable_properties):
            parameter_list.append(_dict)
    return parameter_list


def chunk_parameters(parameters:list,chunk_size:int):
    '''
    Chunks a list into smaller sublists. The idea here is to take create batches or chunks of parameters.
    :param parameters: input parameters
    :param chunk_size: size of sublists
    :return: list of lists. sublists contain a fixed number of elements (the last sublist will just contain the remainder)
    '''
    chunks = [parameters[x:x+chunk_size] for x in range(0, len(parameters),chunk_size)]
    return chunks




def generate_cypher(batches_of_paramters):
    '''
    DON'T USE THIS FUNCTION.
    I'm only keeping it for future reference but string interpolation isn't the move here.

    :param parameters: list of dicts representing parameters
    :return: List of Dictionaries. Dictionary Key is the batch index. Value is the Cypher string with Parameters


    :params {'params': [{'author': 'Dean R Koontz'}, {'author': 'Stephenie Meyer'}
    UNWIND $params as param
    WITH param
    CREATE (u:User {author:param.author})
    '''


    cypher_statements = []
    i = 0
    for batch in batches_of_parameters:
        while i < len(batches_of_paramters):
            i = i+1
            cypher = f'UNWIND ${str(batch)} as param CREATE (u:User {{author:param.author}}'
            cypher_statement = {i:cypher}
            cypher_statements.append(cypher_statement)
    return cypher_statements



def run_string_interpolation(cypher_statements:list,connection):
    '''
    DON'T USE THIS FUNCTION.
    I'm only keeping it for future reference but string interpolation isn't the move here.
    This uses the Connection module as well.

    :param cypher_statements: list of Cypher statements
    :param connection: Connection object
    :return:
    '''
    for cypher_dict in cypher_statements:
        for k,cypher_statement in cypher_dict.items():
            connection.execute(cypher_statement)


def create_authors(tx,batch):
    tx.run(f"UNWIND $batch as param CREATE (u:User {{author:param:author}})")









if __name__ == '__main__':
    json_file_path =  '/Users/alexanderfournier/PycharmProjects/json_neo4j/input/nyt2.json'
    acceptable_properties = ['author']
    uri = 'bolt://localhost:7687'
    driver = GraphDatabase.driver(uri, auth=('neo4j','Reddit123!'))



    parameter_list = generate_properties(acceptable_properties,json_file_path)
    batches_of_parameters = chunk_parameters(parameter_list,chunk_size=50)


    with driver.session() as session:
        for batch in batches_of_parameters:
            session.write_transaction(create_authors,batch)
































