import json
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




def individual_parameter_generator(json_object,properties:list):
    '''
    creates a dictionary from a json object. based on if the key is in the list
    :param json_object: a single json object
    :param properties: a list of strings that coorespond to keys in the dict/json
    :return:
    '''
    parameter = {key:value for key,value in json_object.items() if key in properties}
    yield parameter



'''
todo: I feel like it's kind of confusing to call the functions: individual_parameter_generator, read_json_file
inside this function... 
should I create a decorator/wrapper? 
maybe refactor this to be a Class and use static/class methods. 
'''
def generate_parameters(acceptable_properties:list,json_file_path:str):
    '''
    acceptable properties
    :param acceptable_properties: list of properties that we want to extract from the json
    :return: list of parameters
    '''
    parameter_list = []
    for json_object in read_json_file(json_file_path):
        for _dict in individual_parameter_generator(json_object, acceptable_properties):
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



def create_authors(tx,batch):
    '''
    :param tx:
    :param batch:
    :return:
    '''
    tx.run(f"UNWIND $batch as param CREATE (u:User {{author:param:author}})",kwparameters=batch)









if __name__ == '__main__':
    json_file_path =  '/Users/alexanderfournier/PycharmProjects/json_neo4j/input/nyt2.json'
    acceptable_properties = ['author']
    uri = 'bolt://localhost:7687'
    driver = GraphDatabase.driver(uri, auth=('neo4j','foo!'))



    parameter_list = generate_parameters(acceptable_properties,json_file_path)
    batches_of_parameters = chunk_parameters(parameter_list,chunk_size=50)


    with driver.session() as session:
        for batch in batches_of_parameters:
            session.write_transaction(create_authors, batch)
































