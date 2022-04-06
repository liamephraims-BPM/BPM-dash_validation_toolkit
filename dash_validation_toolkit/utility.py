# Creating client object class for creating client objects for capturing warnings and errors
class client:
    def __init__(self, client, regions, prod_databases):
        self.client = client
        self.regions = regions
        self.prod_databases = prod_databases

# creating utility function for easily creating the client set-up
def set_up_client(client_str, regions_list, prod_databases_list):
    # Creating a client object for keeping track of client validation:
    client_object = client(client=client_str, regions=regions_list, prod_databases=prod_databases_list)
    clients_dict = dict()
    clients_dict[client_str] = client_object
    return clients_dict

# Creating utility function for outputting all the client failures and warnings:
def output_client_validation_results(clients):
    for bpmclient in clients:
        print("------------------------------------------------------------Beginning Failures Output for {}-----------------------------------------------------\n".format(bpmclient))
        print("For client {}, the failures are:\n".format(bpmclient))
        for failed_tables in clients[bpmclient].failures:
            for failure in clients[bpmclient].failures[failed_tables].failures:
                print(clients[bpmclient].failures[failed_tables].failures[failure])
                for child in clients[bpmclient].failures[failed_tables].dependencies:
                    print("WARNING: Check {} failed for {} - this may affect the downstream table {}\n".format(failure, failed_tables, child))
        print("------------------------------------------------------------End of Failures output for {}-----------------------------------------------------\n\n".format(bpmclient))
    # add code here to add weeks data to a validation table in athena
