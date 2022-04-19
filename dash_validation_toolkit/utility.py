# Creating client object class for creating client objects for capturing warnings and errors
class client:
    def __init__(self, client, regions, prod_databases):
        self.client = client
        self.regions = regions
        self.prod_databases = prod_databases
        self.failures = dict()

# creating utility function for easily creating the client set-up
def set_up_client(client_str, regions_list, prod_databases_list):
    # Creating a client object for keeping track of client validation:
    client_object = client(client_str, regions_list, prod_databases_list)
    clients_dict = dict()
    clients_dict[client_str] = client_object
    return clients_dict

# Function to be an import that will allow for sending validation output to analytics slack channel:
import requests
import json
#send data to webhook
def send_data(text):
    WEBHOOK = "https://hooks.slack.com/services/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"
    data = {"text":text}
    headers = {'content-type': 'application/json'}
    res = requests.post(WEBHOOK, data=json.dumps(data), headers=headers)
    print(res.text)
    
# Creating utility function for outputting all the client failures and warnings:
def output_client_validation_results(clients, send_to_slack=True):
    """
        Print out the warnings/errors from the validation, and if send to slack parameter is not false, then also send to analytics slack channel
    """
    output_string = ""
    # For a particular client:
    for bpmclient in clients:
        output_string += "------------------------------------------------------------Beginning Failures Output for {}-----------------------------------------------------\n".format(bpmclient)
        output_string += "For client {}, the failures are:\n".format(bpmclient)
        for failed_tables in clients[bpmclient].failures:
            for failure in clients[bpmclient].failures[failed_tables].failures:
                output_string += clients[bpmclient].failures[failed_tables].failures[failure]
                for child in clients[bpmclient].failures[failed_tables].dependencies:
                    output_string +="WARNING: Check {} failed for {} - this may affect the downstream table {}\n".format(failure, failed_tables, child)
        output_string += "------------------------------------------------------------End of Failures output for {}-----------------------------------------------------\n\n".format(bpmclient)
    if send_to_slack == True:
        # first print out
        print(output_string)
        # then send to slack channel: 
        send_data(output_string)
    else:
        print(output_string)
