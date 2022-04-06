# The code base for defining functions for data validation of client dashboards

# import required packages
from pyathena import connect
import pandas as pd

## Defining node class for failures/warnings:
# Creating node object for passing warnings, dependencies & failures
class node:
    def __init__(self, table, client, database, dependencies, next=None):
        self.dependencies = dependencies
        self.table = table
        self.client = client
        self.database = database
        self.failures = dict()
# this structure would be helpful in passing warnings etc, along a directed graph format

## DEFINING STAGE ONE CHECK FUNCTIONS


############################# Check 1.1 definition - Check prod (source) against target (corresponding union table) for each region ###################################

def check_1_1(source, target, sourcedbs, targetdbs, region, connection):
    """ Utility function for undertaking check 1.1 for checking the count of regional prod data tables against their aggregated union tables 
	Inputs:
	  source - "<table-name>" string e.g.'fact_encounter"
	  target - "<table-name>" string e.g.'fact_encounter'
      sourcedbs - "<database-name>" string e.g. 'jj_prod' <- regional prod dbs pushed to uk
      targetdbs - "<database-name>" string e.g. 'jj_prod_union' <- corresponding union dbs in uk
      region - "<region>" string e.g. "ap-southeast-1" (singapore) <- the original region for the prod table before being pushed to uk
    Output:
	  bool - True/False - 1/0
    Comments:
    """    # Run query of count against the region prod database
    prod_query = """ 
                 SELECT COUNT(*) AS COUNT 
                 FROM {}.{} 
                            """.format(sourcedbs, source)

    prod_query = pd.read_sql(prod_query, connection)
    # Get the count for that regions prod
    region_count = list(prod_query["COUNT"])[0]
    
    # Run query of count against the region prod database
    union_table_query = """
                            SELECT COUNT(*) AS COUNT  
                            FROM {}.{} 
                            WHERE region = '{}'
                            """.format(targetdbs, target, region)

    union_table_query = pd.read_sql(union_table_query, connection)

    union_count = list(union_table_query["COUNT"])[0]

    print("Check 1.1 (same regional row count): ", union_count== region_count, union_count, target, targetdbs, region_count, source, sourcedbs)
    # Check to see if count is the same for that regional prod table within the union table for that region
    return union_count== region_count



######################### Check 1.2 definition - Check for union table, that there are no nulls in region column, so only regions to be tested   ######################
def check_1_2(target, targetdbs, connection):
    """ Utility function for undertaking check 1.2 for checking that there are no nulls in region columns of union table
	Inputs:
	  target - "<table-name>" string e.g.'fact_encounter'
      targetdbs - "<database-name>" string e.g. 'jj_prod_union' <- corresponding union dbs in uk
    Output:
	  bool - True/False - 1/0    
  Comments:
  Review Comments:
    """
#target=table, targetdbs=clients[validation_client].client + "_prod_union"
    # Run query of count of NULLS against region table
    union_table_query = """
                            SELECT COUNT(*) AS COUNT  
                            FROM {}.{} 
                            WHERE region IS NULL
                            """.format(targetdbs, target)

    union_table_query = pd.read_sql(union_table_query, connection)
    union_count = list(union_table_query["COUNT"])[0]

    print("Check 1.2 (null regions): ", union_count== 0, union_count)
    # there should be no nulls in region column for union table
    return union_count== 0



## DEFINING STAGE ONE CHECK FUNCTIONS


######################### Check 2.1 definition - Check for base table, that the count of primary key is the same as the count of union primary keys & simiarly that all PKs are in both tables   ######################

def check_2_1(target, targetdbs, target_PK, parent_query, prod_ids, connection):
    """ Utility function for undertaking check 2.1, checks that primary key or composite PK key count between union and base tables is the same 
	Inputs:
	  target -  string base table name  - e.g. fact_encounter
      	  targetdbs - string base table database name - e.g. jj_base_tables
          target_PK - the string name of the primary key column of table - e.g. encounter_id
                        note: this is derived from the index for the id/column to be counted in base table - col 0 (1) for jj_base_tables.fact_encounter
                              within the input section for each client          
          parent_query - the corresponding query for the corresponding prod union - jj_prod_union.menicon_encounters, col 0 (id)
	 Output:
	  bool - True/False - 1/0
     Comments:
          1. parent_query needs to provide the FROM section of query string only for prod table-if nested query then needs to be in format -> FROM ( <nested inner query>)
          with an id being needed in the output-as this will be counted
    """
    # for base tables where checking the count of the base table against the prod table does not make sense, will skip this check when requiried. e.g. dim_domain, dim-pathways
    # will return true so does not appear in errors/warnings.
    if parent_query == []:
        print(f"Check 2.1 skipped for {target}--not appropriate")
        return True
        
    # check if PK is a composite PK so a list, if so need to concat into correct format- otherwise leave as single column string:
    if type(target_PK) == list:
        # then need to concat into string of multiple PK columns:
        target_PK = ','.join(target_PK)
        
    # check if PK is a composite PK so a list, if so need to concat into correct format- otherwise leave as single column string:
    if type(prod_ids) == list:
        # then need to concat into string of multiple PK columns:
        prod_ids = ','.join(prod_ids)
        
    # Getting PK count query
    base_query = pd.read_sql("""
                            SELECT COUNT(DISTINCT {}) AS COUNT  FROM {}.{} 
                            """.format(target_PK, targetdbs, target ), connection)
    # actual count for base
    base_count = list(base_query["COUNT"])[0]      

    # Getting parent count athena query
    parent_query = pd.read_sql("""
                                    SELECT COUNT(DISTINCT {}) AS COUNT {}
    
                                """.format(prod_ids, parent_query), connection)

    # actual count for union parent table
    parent_count = list(parent_query["COUNT"])[0]     
    #print(union_count,base_count,table)   

    print("Check 2.1 (same PK count): ", parent_count== base_count, target, parent_count, base_count)
    # check result
    return parent_count== base_count

#check_dictionary["2.1"] = check_2_1

######################### Check 2.2 definition - Check for base table, that primary key of union primary keys & base primary keys are the same   ############################################################################

def check_2_2(target,targetdbs, target_PK, parent_query, prod_ids, connection):
    """ Utility function for undertaking check 2.2, checks that primary key or composite PK key of base are all in primary key of union table
	Inputs:
	      target -  string base table name  - e.g. fact_encounter
      	  targetdbs - string base table database name - e.g. jj_base_tables
          target_PK - the string name of the primary key column of table - e.g. encounter_id
                        note: this is derived from the index for the id/column to be counted in base table - col 0 (1) for jj_base_tables.fact_encounter
                              within the input section for each client
          parent_query - the corresponding query for the corresponding prod union - jj_prod_union.menicon_encounters, col 0 (id)
	 Output:
	  bool - True/False - 1/0
     Comments:
          1. parent_query needs to provide the FROM section of query string only for prod table-if nested query then needs to be in format -> FROM ( <nested inner query>)
          with an id being needed in the output-as this will be counted
    """
    # for base tables where checking the count of the base table against the prod table does not make sense, will skip this check when requiried. e.g. dim_domain, dim-pathways
    # will return true so does not appear in errors/warnings.
    if parent_query == []:
        print(f"Check 2.2 skipped for {target}-not appropriate")
        return True

        # check if PK is a composite PK so a list, if so need to concat into correct format- otherwise leave as single column string:
    if type(target_PK) == list:
        # then need to concat into string of multiple PK columns:
        target_PK = ','.join(target_PK)
        
    # check if PK is a composite PK so a list, if so need to concat into correct format- otherwise leave as single column string:
    if type(prod_ids) == list:
        # then need to concat into string of multiple PK columns:
        prod_ids = ','.join(prod_ids)
        
    # Getting PK count query
    base_query = pd.read_sql("""
                            SELECT {} AS PK_base  FROM {}.{} 
                            """.format(target_PK, targetdbs, target ), connection)
    # actual count for base
    base_set = set(base_query["PK_base"])   
        
    # Getting union count query
    parent_query = pd.read_sql("""
                                    SELECT DISTINCT CONCAT('', {}) AS union_PK {}
    
                                """.format(prod_ids, parent_query), connection)

    # actual count for union source table
    union_set = set(parent_query["union_PK"]) 
    
    print("Check 2.2 (same distinct PKs): ", (len(union_set.intersection(base_set)) == len(union_set)) and (len(base_set.intersection(union_set)) == len(base_set)), target,   )
    # check result: Logic - the intersection between base primary keys and union primary keys are the same - i.e. same primary keys in both base & union
    return (len(union_set.intersection(base_set)) == len(union_set)) and (len(base_set.intersection(union_set)) == len(base_set))  

#check_dictionary["2.2"] = check_2_2

######################### Check 2.3 definition - Check that every primary key is unique within base tables ############################################################################

def check_2_3(target, targetdbs, target_PK, connection):
    """ Utility function for undertaking check 2.3, checks that every primary key is unique - for base tables this is only the primary id and not the update time	
     Inputs:
	      target -  string base table name  - e.g. fact_encounter
      	  targetdbs - string base table database name - e.g. jj_base_tables
          target_PK - the list or string of the primary key columns of table - e.g. encounter_id or if composite (pathway_name, region) as list
                        note: this is derived from the index for the id/column to be counted in base table - col 0 (1) for jj_base_tables.fact_encounter
                              within the input section for each client
	 Output:
	       bool - True/False - 1/0
     Comments:

    """
        # check if PK is a composite PK so a list, if so need to concat into correct format- otherwise leave as single column string:
    if type(target_PK) == list:
        # then need to concat into string of multiple PK columns:
        target_PK = ','.join(target_PK)
        
    # Getting PK count query
    base_query = pd.read_sql("""
                                SELECT 
                                    {} AS PK_base, COUNT(*) AS COUNTER 
                                FROM {}.{} 
                                GROUP BY {}
                                HAVING COUNT(*) > 1
                            """.format(target_PK, targetdbs, target, target_PK), connection)

    # check if there are any primary keys within this table - indicating duplicate primary keys - count > 1 so length of column turned to set > 0
    base_set = set(base_query["PK_base"])   

    dup_keys = len(base_set)
    print("Check 2.3 (unique PKs): ",dup_keys ==0, target, dup_keys)
    
    # return whether this test is true - passed - or false - failed for not containing any duplicate primary keys/primary keys are unique
    return dup_keys ==0

#check_dictionary["2.3"] = check_2_3


######################### Check 2.4 definition - Check that for a base table which has a column which has an assumption based on a definition, that definition is up-to-date with all definition values - e.g. ecp roles   ############################################################################

def check_2_4(look_up_database, look_up_table, look_up_column, prod_query, prod_column, exclusion_set, connection):
        """ Utility function for check 2.4, checking that defintion look-up tables are up-to-date - ie no new values in prod which are not in exclusion set & in look-up table
                Inputs:
                    look_up_database = database name string for look up table - e.g. jj_sandbox
                    look_up_table = table name string for look-up table - e.g. ecp_user_roles_LOOK_UP
                    look_up_column = column name string of look-up table for value set being maintained - e.g. name
                    prod_query = the athena query string for creating the look-up table dataset-
                                e.g. SELECT DISTINCT name FROM jj_prod_union.roles
                    prod_column = the column name for the look-up value set of interest- e.g. name for roles
                    exclusion_set = the set of look-up values that have already been confirmed will not need to be added to look-up table 
                Output:
                    bool - True/False - 1/0
                Comments: these look-up tables & exclusion criteria are likely out of date, probs need a better way to maintain these if we see the need to maintain              
        """

        # Getting the definition values for base look-up table:
        base_query = pd.read_sql("""
                                    SELECT DISTINCT {}
                                    FROM {}.{}
                                """.format(look_up_column, look_up_database, look_up_table), connection)

        # Getting the set of distinct values of base look-up table:
        base_set = set(base_query[look_up_column])

        # Getting the definition values for prod look-up table:
        prod_query = pd.read_sql(prod_query, connection)

        # Getting the set of distinct values of defintion values off prod:
        prod_set = set(prod_query[prod_column])

        # removing from the prod_set any definition values that have already been deemed needing exclusion
        for definition_value in exclusion_set:
            if definition_value in prod_set:
                prod_set.remove(definition_value)

        print("Check 2.4 (no missed look-up values): ", bool(prod_set == base_set), look_up_table, prod_set.difference(base_set))

        # Checking that after excluding all definition values that do not fit the definition, that there are no news one which have been missed from the base defintion look-up table
        return prod_set == base_set


## DEFINING STAGE THREE FUNCTIONS:
######################### Check 3.1 definition - Check 1 for stage 3 Dashboard checks - check that if multiple pathways for a dashboard statistic, 
#                                   then make sure select all == sum of individual pathways in dashboard table alone - e.g. menicon ############################################################################

def check_3_1(pathway_set, pathways, dashboard_stat, connection):
    """ Utility function for undertaking check 3.2, checks that select all == individual pathway sums"""

    # get the select all total:
    select_all_total = list(pathways[pathways["pathway_name"] == "Select all"]["{}".format(dashboard_stat)])[0]

    # sum variable for capturing aggregated sum of pathways
    pathway_total = 0

    # remove select all from pathway_set, so only non-select all pathways:
    pathway_set.remove("Select all")

    # getting sum of specific pathways (other than select all):
    for pathway in pathway_set:
        pathway_sum = list(pathways[pathways["pathway_name"] == pathway]["{}".format(dashboard_stat)])[0]
        # Incrementing pathway total
        pathway_total = pathway_total + pathway_sum

    # confirming that select all == (all pathway sums added together)
    print('Check 3.1 (select all == pathway sum):', select_all_total == pathway_total, dashboard_stat)
    
    return select_all_total == pathway_total


######################### Check 3.2 definition - Check 2 for stage 3 Dashboard checks - checking NON-cumulative & NON-onboard user dashboard statistics & check 3.1  ############################################################################

# in future, should add in method for inference of schema from each script for prod, union, base table, end-point tables
# and matching of primary keys together

def check_3_2(dashboard_statistic, base_statistic_query, dash_table, dash_database, connection):
    """ Utility function for undertaking check 3.2, checks that a dashboard statistic in end-point dashboard table has the same sum as when calculated 
    directly off the relevant base tables and checks that check 3.1 is true"""
    # Calculating the sum for the dashboard statistic
    dash_statistic_query = """
                                    SELECT SUM({}) 
                                    FROM {}.{}
                                """.format(dashboard_statistic,dash_database,dash_table)
    
    #  getting all pathways for this dashboard statistic:

    # creating defaiult bool for check1
    check3_1 = True

    # checking if pathway_name column:
    pathway_check = pd.read_sql(
    """
        SELECT *
        FROM {}.{} 
    """.format(dash_database, dash_table) ,connection).columns

    if "pathway_name" in set(pathway_check):

        pathways = pd.read_sql("""
            SELECT 
                pathway_name, SUM({}) as {} 
            FROM {}.{} 
            GROUP BY pathway_name
        """.format(dashboard_statistic, dashboard_statistic, dash_database, dash_table) ,connection)

        # getting all distinct pathway name values
        pathway_set = set(pathways["pathway_name"])

        # setting a default boolean for check 3.1 (that a select all == sum of individual pathways if multiple pathways) with default true - in case not multiple pathways 
        
        # checking if there is a select all (ie. multiple pathways):
        if "Select all" in pathway_set:
            # then will need to compare select all against overall base table statistic - because sum over dashboard statistic would be wrong for check 3.1
            dash_statistic_query = dash_statistic_query + " WHERE pathway_name = 'Select all'"

            # furthermore, knowing that this is a multiple pathway, need to check that the pathways underlying the select all are also correct, so completeing a nested check for this
            check3_1 = check_3_1(pathway_set, pathways, dashboard_statistic) 

    dash_statistic = pd.read_sql(dash_statistic_query, connection)
    dash_statistic = list(dash_statistic["_col0"])[0]

    # Calculating the sum for base table query for the same statistic
    base_statistic = pd.read_sql(base_statistic_query, connection)
    base_statistic = list(base_statistic["_col0"])[0]

    # check logic: both sums are equal
    print("Check 3.2 (base to dash same sum): ", base_statistic==dash_statistic, base_statistic,dash_statistic, dash_table, dashboard_statistic)

    # check that the base statistic query is equal to the sumed base dashboard statistic (check 3.2) 
    #    AND if is a multiple pathway that select all == sum of individual pathways in dashboard table alone (check 3.1)
    return (base_statistic==dash_statistic) and (check3_1 == True)

    ######################### Check 3.3 definition - Check 3 for stage 3 Dashboard checks - checking cumulative  statistics   ############################################################################
# needs to generalise over numerous regions & numerous pathways
def check_3_3(cumulative_dash_query, base_dash_query, regions, cumulative_dash_statistic, connection):
    """ Utility function to test that for the cumulative table, if select all than this is equal the overall base statistic total    """
    # read in the cumulative data table from athena for all regions, maxing for last cumulative total for each regions + pathway
    cumulative_dash_query = pd.read_sql(cumulative_dash_query, connection)

    # read in the base data table from athena for the overall count of the dash statistic over all regions
    base_dash_read = pd.read_sql(base_dash_query, connection)

    check_bool = 0

    # integer for storing all regional select alls rolling counts:
    select_all_regional_set = 0

    if "pathway_name" in set(cumulative_dash_query.columns):

        # getting all possible pathways for dash table stat
        pathways = set(cumulative_dash_query["pathway_name"])

        # completing select all comparison
        if "Select all" in pathways:
            for region in regions:
                # then checking select all is correct against the base query for all pathways:
                select_all_regional_set += list(cumulative_dash_query[(cumulative_dash_query["region"] == region) & (cumulative_dash_query["pathway_name"] == "Select all") ][cumulative_dash_statistic])[0]
            # now checking that this total for select all over all regions is the same as the overall total from the base query:
            if not select_all_regional_set == base_dash_read["count"][0]:
                # if the select all cumulative total is not equal to the base query total for statistic, then add as a failure to the check bool
                check_bool += 1
        else:
            if len(pathways) == 1:
                #then only one pathway - check for this single pathway for all regions:
                for pathway in pathways:
                    for region in regions:
                        # then checking select all is correct against the base query for all pathways:
                        select_all_regional_set += list(cumulative_dash_query[(cumulative_dash_query["region"] == region) & (cumulative_dash_query["pathway_name"] == pathway) ][cumulative_dash_statistic])[0]
                    # now checking that this total for select all over all regions is the same as the overall total from the base query:
                    if not select_all_regional_set == base_dash_read["count"][0]:
                        # if the select all cumulative total is not equal to the base query total for statistic, then add as a failure to the check bool
                        check_bool += 1     

    # completing check for 3.3, if any failures in pathway+region cumulative totals not adding up  or the overall select all not adding up to base query then inconsistency in cumualtive:
    print("Check 3.3 (select all == cumulative total): ", check_bool == 0, cumulative_dash_statistic)
    return check_bool == 0
    

    ######################### Check 3.4 definition - Check 3 for stage 3 Dashboard checks - checking onboard (single-level pathway)  statistics  -where sum would not work over multiple pathways ############################################################################
def check_3_4(dashboard_statistic, dash_database, dash_table, base_statistic_query, connection):
    """ Utility function to test check 3.4, which computes if a dashboard statistic, which cannot be summed across pathways as same for all - e.g. onboarded users is the same as base    """
    #  getting all pathways for  dashboard statistic:
    pathways = pd.read_sql("""
        SELECT 
            pathway_name, SUM({}) as {} 
        FROM {}.{} 
        GROUP BY pathway_name
    """.format(dashboard_statistic, dashboard_statistic, dash_database, dash_table) ,connection)

    # getting all distinct pathway name values
    pathway_set = set(pathways["pathway_name"])


    pathway_sum_list = list()
    # for all pathways add the sum to a list, then sum this list, then divide by number of pathways - this should end up at the same number if all are equal as they are supposed to be
    for pathway in pathway_set:
        path_total = list(pathways[pathways["pathway_name"] == pathway]["{}".format(dashboard_statistic)])[0]
        pathway_sum_list.append(path_total)
    # now summing that list for sum across all pathways (including select all if present)
    pathway_total_sum = sum(pathway_sum_list)
    # no. of pathways
    no_pathways = len(pathway_set)

    # count for a randomly selected pathway:
    random_pathway_count = list(pathways[pathways["pathway_name"] == pathway_set.pop()]["{}".format(dashboard_statistic)])[0]
    # bool for checking if all pathways are equal value across pathways, including select all
    paths_equal_bool = pathway_total_sum / no_pathways == random_pathway_count
    # now getting the base query total to make sure this is also the same 
    base_statistic = pd.read_sql(base_statistic_query, connection)
    base_statistic = list(base_statistic["_col0"])[0]
    
    print("Check 3.4 (onboard stat dash->base same): ", (base_statistic == random_pathway_count and paths_equal_bool == True), dashboard_statistic)
    
    # Now having confirmed that all pathway counts are the same for this statistic, making sure that one of them is equal to the base table query for this statistic
    return base_statistic == random_pathway_count and paths_equal_bool == True

    ######################### Check 3.5 definition - Check 3 for stage 3 Dashboard checks - checking onboard (single-level pathway)  statistics  -where sum would not work over multiple pathways ############################################################################

def check_3_5(business_logic_query, base_stat_query,business_logic_name, connection):
    """     General utility function to compare if two queries are the same, generalised for testing business logic queries against base table queries  or could also check between dashboard queries   
               NOTE: Needs to be queries resulting in single counts/ count comparison  """
    
    # running the result for business logic
    business_logic_query = pd.read_sql(business_logic_query, connection)
    business_logic_query_result = list(business_logic_query["_col0"])[0]
    
    # running the result for base query comparison
    base_stat_query = pd.read_sql(base_stat_query, connection)
    base_stat_query_result = list(base_stat_query["_col0"])[0]

    # outputting result of comparison
    print(f"Check 3.5 (business logic-{business_logic_name}): ", (base_stat_query_result == business_logic_query_result), base_stat_query_result, business_logic_query_result)

    # returning whether business logic check was passed:
    return base_stat_query_result == business_logic_query_result

    ######################### Check 3.6 definition - essentially same as above - Check 3 for stage 3 Dashboard checks - Checking between dashboard figures or business logic totals within the dashboard

def check_3_6(dash_query_1, dash_query_2,dash_test_name, logical_comparion_operator, connection):
    """     General utility function to compare if two queries are the same, used in this case for two generic dashboard figures or queries  """
    
    # running the result for dash query 1 comparison
    dash_query_1 = pd.read_sql(dash_query_1, connection)
    #print(dash_query_1)
    dash1_query_result = list(dash_query_1["_col0"])[0]
    
    # running the result for dash query 2 comparison
    dash_query_2 = pd.read_sql(dash_query_2, connection)
    #print(dash_query_2)
    dash2_query_result = list(dash_query_2["_col0"])[0]

    # outputting result of comparison
    print(f"Check 3.6 (within-dash check-{dash_test_name}): ", eval("{} {} {}".format(dash1_query_result, logical_comparion_operator,dash2_query_result)), dash2_query_result, dash1_query_result)
    # returning whether dash comparison check was passed: NOTE: this will evaluate the string as a logical expression - allowing for the logic operator to be dynamic
    return eval("{} {} {}".format(dash1_query_result, logical_comparion_operator,dash2_query_result))


## DEFINING DRIVER FUNCTIONS - STAGE ONE

def stage_1_driver(clients, validation_client, connection):
    """
    Driver function for running the stage 2 checks above for a client
    Inputs:
        clients: to-do
        validation_client: to-do
    
            """ 
    # Checks being undertaken:
    #      1.1 - Check that for a prod region, there is same amount of rows for that region in union table
    #      1.2 - Check that for union table there is no null values

    # 5 a. Obtain all possible union tables from all regions of client:
    tables = dict()
    for index in range(0, len(clients[validation_client].regions)): 
        region_database = clients[validation_client].prod_databases[index]
        region = clients[validation_client].regions[index]
        tables_query = """
                        SELECT TABLE_NAME 
                        FROM information_schema.TABLES 
                        WHERE (TABLE_SCHEMA = '{}') ;""".format(region_database)
        #print(tables_query)
        region_tables = set(pd.read_sql(tables_query , connection)["TABLE_NAME"])

    #     print(region_tables)
        tables[region] = region_tables, region_database

    # Loop through each table between union database & prod database & complete checks for stage 1:
    #for table in region_tables:
    for region in tables:
        for table in tables[region][0]:
            prod_database = tables[region][1]

            # Undertaking checks for stage 1:

            #################################### Check 1.1: Check count of each region is correctly represented in union table ###########################################################
            check1 = check_1_1(source=table,target=table,sourcedbs=prod_database, targetdbs=clients[validation_client].client + "_prod_union", region=region, connection=connection)

                    # Adding failures from checks of stage 1 to client object:
            if check1 == False:
                #Then Check 1.1 has failed for this union table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in jj  - add additional failure
                    clients[validation_client].failures[table].failures["1.1"] = "FAILURE: Check 1.1 - Table {} for region: {} - Inconsistent region count between region production & union\n".format(table, region )
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_prod_union", [] )
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["1.1"] = "FAILURE: Check 1.1 - Table {} for region: {} - Inconsistent region count between region production & union\n".format(table, region )


            #################################### Check 1.2: Check that there are no NULLS in region column of union table ###########################################################   
            # This will only be run once per table (is outside of the region loop)
            check2 = check_1_2(target=table, targetdbs=clients[validation_client].client + "_prod_union", connection=connection)

            if check2 == False:
                #Then Check 1.2 has failed for this union table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in clients[validation_client]  - add additional failure
                    clients[validation_client].failures[table].failures["1.2"] = "FAILURE: Check 1.2 - Table {}: has null region values for region column in union table\n".format(table)
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_prod_union", [] )
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["1.2"] = "FAILURE: Check 1.2 - Table {}: has null region values for region column in union table\n".format(table)
    return clients

def stage_2_driver(primary_parents, clients, validation_client, definition_check_dictionary, connection):
    """
    Driver function for running the stage 2 checks above for a client
    Inputs:
        primary_parents: to-do
        clients: to-do
        definition_check_dictionary: to-do
        validation_client: to-do
    
        """ 
    # Getting list of all base tables
    base = set(pd.read_sql("""
                            SELECT DISTINCT TABLE_NAME 
                            FROM information_schema.TABLES 
                            WHERE (TABLE_SCHEMA = '{}') ;
                            """.format(clients[validation_client].client + "_base_tables"), connection)["TABLE_NAME"])

    # Getting primary keys for all base tables:
    # Assumes that ID is always in the first column

    primary_key_table = """
                        SELECT 
                            table_name, column_name
                        FROM information_schema.COLUMNS 
                        WHERE (TABLE_SCHEMA = '{}' ) ;""".format(clients[validation_client].client + "_base_tables")
    primary_key_table = pd.read_sql(primary_key_table , connection)
    
    # loop over each base table in base table database:
    for table in base:
        # obtaining the primary parent - the primary union table - that the base table is created from 
        if table in primary_parents: # note: this removes those tables without an obvious union table as primary parent, will need to fix in future

            # Undertaking checks for stage 2:

            # get the indexe in base table which reflects PK of base table or multiple columns if PK is composite keys (e.g col0, col1 make up PK for base table):
            base_PK_index = primary_parents[table][0]
             # getting primary key for base table:
            if type(base_PK_index) == list and  len(base_PK_index) > 1: # i.e. is a composite primary key:
                base_PK = list(primary_key_table[primary_key_table.table_name == table]["column_name"])[base_PK_index[0]:base_PK_index[1] + 1]
            else:
                base_PK = list(primary_key_table[primary_key_table.table_name == table]["column_name"])[base_PK_index]   

            source_query = primary_parents[table][1]

            # getting prod ids
            prod_ids = primary_parents[table][2]

            #################################### Check 2.1: Check that PK count of union table is the same as the PK count of the base table & that all PKs are in both tables ###########################################################   - NOTE: this will be done twice in loop, would be good to imrpove on this
            check1 = check_2_1(table, clients[validation_client].client + "_base_tables", base_PK, source_query, prod_ids, connection)

            #################################### Check 2.2: Check that all primary keys in prod/union are in base table and vice versa ###########################################################   - NOTE: this will be done twice in loop, would be good to imrpove on this

            check2 = check_2_2(table, clients[validation_client].client + "_base_tables", base_PK, source_query, prod_ids, connection)

            #################################### Check 2.3: Check that all  primary keys are unique ###########################################################   - NOTE: this will be done twice in loop, would be good to imrpove on this

            check3 = check_2_3(table, clients[validation_client].client + "_base_tables", base_PK, connection)    

            #################################### Check 2.4: Checking that defintion look-up tables are up-to-date ###########################################################   - NOTE: this will be done twice in loop, would be good to imrpove on this
            # Checking if for this base table, if there is a definition/look-up table associated with it which needs to be checked

            # setting check4 by default to be true (if there is no look-up table/definition to check for table)

            check4 = True


            if table in definition_check_dictionary: 
                # unpacking variables for checking over definition in check 2.4
                bpmclient, look_up_table, look_up_database, look_up_column, definition_exclusion_set, prod_query, prod_column   =   definition_check_dictionary[table]
                # making sure this base table <-> look up table is for the correct client:
                if bpmclient == clients[validation_client].client:
                    # run check for check 2.4:
                    check4 = check_2_4(look_up_database, look_up_table, look_up_column, prod_query, prod_column, definition_exclusion_set, connection)

            # Adding failures from checks of stage 1:
            if check1 == False:
                #Then Check 2.1 has failed for this base table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in clients - add additional failure
                    clients[validation_client].failures[table].failures["2.1"] = "FAILURE: Check 2.1 - Table {} - Inconsistent Primary key count between base & union tables\n".format(table)
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_base_tables", [])
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["2.1"] = "FAILURE: Check 2.1 - Table {} - Inconsistent Primary key count between base & union tables\n".format(table)

            if check2 == False:
                #Then Check 2.2 has failed for this base table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in clients[validation_client]  - add additional failure
                    clients[validation_client].failures[table].failures["2.2"] = "FAILURE: Check 2.2 - Table {} - Inconsistent or missing primary keys between base & union tables\n".format(table)
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_base_tables", [] )
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["2.2"] = "FAILURE: Check 2.2 - Table {} - Inconsistent or missing primary keys between base & union tables\n".format(table )

            if check3 == False:
                #Then Check 2.3 has failed for this base table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in clients[validation_client]  - add additional failure
                    clients[validation_client].failures[table].failures["2.3"] = "FAILURE: Check 2.3 - Table {} - Primary key constraint broken - primary key duplicates \n".format(table )
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_base_tables", [] )
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["2.3"] = "FAILURE: Check 2.3 - Table {}  - Primary key constraint broken - primary key duplicates \n".format(table )

            if check4 == False:
                #Then Check 2.4 has failed for this base table - create a node (if not already created for this table) and add its failure
                # adding to this overall client object:

                if table  in clients[validation_client].failures:
                    #then already in clients[validation_client]  - add additional failure
                    clients[validation_client].failures[table].failures["2.4"] = "WARNING: Check 2.4 - Table {} - new definition value missing from definition look-up table \n".format(table )
                else:
                    # then table has not failed a check, add to client object and add first failure:
                    failed_table = node(table, clients[validation_client].client , clients[validation_client].client + "_base_tables", [] )
                    clients[validation_client].failures[table] = failed_table
                    clients[validation_client].failures[table].failures["2.4"] = "WARNING: Check 2.4 - Table {} - new definition value missing from definition look-up table \n".format(table )

        # If table has not been defined in primary_parents (no obvious union table), it will not be checked - caused by mismatch in key names
        else:
            print(table, " has not been checked")
    return clients


def stage_3_driver(dash_to_base_query_dictionary, clients, cumulative_check_dict, onboard_stat_dict, business_logic_dict, between_dash_comparison_dict, validation_client, connection):
    """
    Driver function for running the stage 1 checks above for a client
    Inputs:
        dash_to_base_query_dictionary:
        clients:
        cumulative_check_dict: to-do
        onboard_stat_dict: to-do
        onboard_stat_dict: to-do
        business_logic_dict: to-do
        between_dash_comparison_dict: to-do
    
    """

    # Creating counter for no. of test - this allows for many errors of the same check over different statsitics
    counter = 0
    # For each non-cumulative statistic in dashboard:
    for statistic in dash_to_base_query_dictionary:

        counter += 1
        dashboard_table = dash_to_base_query_dictionary[statistic][0]

        #################################### Check 3.1: Check each statisic sum is same in base table ###########################################################

        check1 = check_3_2(dashboard_statistic=statistic, base_statistic_query=dash_to_base_query_dictionary[statistic][1], dash_table=dashboard_table, dash_database=clients[validation_client].client + "_dashboard_tables", connection=connection)

        # Adding failures from checks of stage 1 to client object:
        if check1 == False:


        #Then Check 3.2 has failed for this union table - create a node (if not already created for this table) and add its failure
        # adding to this overall client object:

            if dashboard_table  in clients[validation_client].failures:
            #then already in clients  - add additional failure
                clients[validation_client].failures[dashboard_table].failures["3.1." + str(counter)] = "FAILURE: Check 3.1 - Dashboard Table {}: - Dashboard statistic {} sum is inconsistent with derived base table statistic sum \n".format(dashboard_table, statistic )
            else:
                # then dashboard_table has not failed a check, add to client object and add first failure:
                failed_table = node(dashboard_table, clients[validation_client].client , clients[validation_client].client + "_dashboard_tables", [] )
                clients[validation_client].failures[dashboard_table] = failed_table
                clients[validation_client].failures[dashboard_table].failures["3.1." +  str(counter)] = "FAILURE: Check 3.1 - Dashboard Table {}: - Dashboard statistic {} sum is inconsistent with derived base table statistic sum \n".format(dashboard_table, statistic )

    counter = 0
    # For each cumulative statistic in dashboard:
    for cumulative_statistic in cumulative_check_dict:
        counter += 1
        #################################### Check 3.3: Check each cumulative statisic sum is same in base table: a. against select all for all regions or if just 1 pathway then that against base total  ###########################################################
        # note: two checks in check 3.2 (3.1 is an inner check of 3.2)
        cumulative_dash_query = cumulative_check_dict[cumulative_statistic][0]
        base_dash_query = cumulative_check_dict[cumulative_statistic][1]

        check3 = check_3_3(cumulative_dash_query, base_dash_query, clients[validation_client].regions, cumulative_statistic, connection=connection)

        # Adding failures from checks of stage 1 to client object:
        if check3 == False:

        #Then Check 1.1 has failed for this union table - create a node (if not already created for this table) and add its failure
        # adding to this overall client object:

            if dashboard_table  in clients[validation_client].failures:
            #then already in clients  - add additional failure
                clients[validation_client].failures[dashboard_table].failures["3.3." +  str(counter)] = "FAILURE: Check 3.3 - Dashboard Table {}: - Dashboard cumulative statistic {} sum is inconsistent with derived base table statistic sum \n".format(dashboard_table, cumulative_statistic )
            else:
                # then dashboard_table has not failed a check, add to client object and add first failure:
                failed_table = node(dashboard_table, clients[validation_client].client , clients[validation_client].client + "_dashboard_tables", [] )
                clients[validation_client].failures[dashboard_table] = failed_table
                clients[validation_client].failures[dashboard_table].failures["3.3." + str(counter)] = "FAILURE: Check 3.3 - Dashboard Table {}: - Dashboard cumulative statistic {} sum is inconsistent with derived base table statistic sum \n".format(dashboard_table, cumulative_statistic )

    counter = 0
    # evaluating dashboard statistic check for onboard stat (stat where each level should be the same):
    for onboard_statistic in onboard_stat_dict:
        counter += 1
        check4 = check_3_4(onboard_statistic, clients[validation_client].client + "_dashboard_tables", "overview_weekly", onboard_stat_dict[onboard_statistic], connection)

        # Adding failures from checks of stage 3 to client object:
        if check4 == False:

        #Then Check 3.4 has failed for this union table - create a node (if not already created for this table) and add its failure
        # adding to this overall client object:

            if dashboard_table  in clients[validation_client].failures:
            #then already in clients  - add additional failure
                clients[validation_client].failures[dashboard_table].failures["3.4." +  str(counter)] = "FAILURE: Check 3.4 - Dashboard Table {}: - Dashboard onboard statistic {}  is inconsistent across levels with derived base table statistic sum \n".format(dashboard_table, cumulative_statistic )
            else:
                # then dashboard_table has not failed a check, add to client object and add first failure:
                failed_table = node(dashboard_table, clients[validation_client].client , clients[validation_client].client + "_dashboard_tables", [] )
                clients[validation_client].failures[dashboard_table] = failed_table
                clients[validation_client].failures[dashboard_table].failures["3.4." +  str(counter)] = "FAILURE: Check 3.4 - Dashboard Table {}: - Dashboard onboard statistic {}  is inconsistent across levels with derived base table statistic sum \n".format(dashboard_table, cumulative_statistic )

    counter = 0
    # For each business logic check for dashboard:
    for  business_logic_name in  business_logic_dict:          
        counter += 1
        business_logic_query, base_stat_query = business_logic_dict[business_logic_name]

        #################################### Check 3.5: Check business logic queries against base tables for dashboard ###########################################################
        check5 = check_3_5(business_logic_query, base_stat_query,business_logic_name, connection)

        if check5 == False:
            if dashboard_table  in clients[validation_client].failures:
            #then already in clients  - add additional failure
                clients[validation_client].failures[dashboard_table].failures["3.5." +  str(counter)] = "FAILURE: Check 3.5 - Dashboard Table {}: - business logic check failed - {}\n".format(dashboard_table,  business_logic_name)
            else:
                 # then dashboard_table has not failed a check, add to client object and add first failure:
                failed_table = node(dashboard_table, clients[validation_client].client , clients[validation_client].client + "_dashboard_tables", [] )
                clients[validation_client].failures[dashboard_table] = failed_table
                clients[validation_client].failures[dashboard_table].failures["3.5." +  str(counter)] = "FAILURE: Check 3.5 - Dashboard Table {}: - business logic check failed - {}\n".format(dashboard_table,  business_logic_name)

    counter = 0
    ## For each in-dashboard comparison:
    for dashboard_comparison_name in between_dash_comparison_dict:
        counter += 1
        # Unpacking check variable input for each dashboard query that was set-up for client
        dashboard_table, dash_query_1, dash_query_2, logical_operator = between_dash_comparison_dict[dashboard_comparison_name]

    #     #################################### Check 3.6: Check in-dashboard comparions logic ###########################################################
        check6 = check_3_6(dash_query_1, dash_query_2,dashboard_comparison_name, logical_operator, connection)
        if check6 == False:
            if dashboard_table  in clients[validation_client].failures:
            #then already in clients  - add additional failure
                clients[validation_client].failures[dashboard_table].failures["3.6." +  str(counter)] = "FAILURE: Check 3.6: - in-dashboard comparison logic check failed - {}\n".format(dashboard_comparison_name)
            else:
                # then dashboard_table has not failed a check, add to client object and add first failure:
                failed_table = node(dashboard_table, clients[validation_client].client , clients[validation_client].client + "_dashboard_tables", [] )
                clients[validation_client].failures[dashboard_table] = failed_table
                clients[validation_client].failures[dashboard_table].failures["3.6." +  str(counter)] = "FAILURE: Check 3.6: - in-dashboard comparison logic check failed  - {}\n".format(dashboard_comparison_name)
    return clients