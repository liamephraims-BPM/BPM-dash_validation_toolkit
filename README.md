BPM_dash_validation_toolkit

Each new version (change in functions etc) to bring this into AWS jobs, we will need to:
  1. Increment the setup.py version by 1.
  2. Recreate the wheel file for package. For this:
  3. Run pip install wheels
  4. Delete old wheel files/folders 
  5. Run python setup.py bdist_wheel
  6. Add this new wheel file to our S3 Bucket which is pointed to by our glue jobs

BPM_dash_validation_toolkit is a Python package for our bpm analytics team. It provides functionality to undertake checks for stage one (region source to union), stage two (union to base) and stage three (base to dashboard) checks, and allows us to maintain our validation code for each client on more simply validation template scripts which can easily be added to when needed. If you want to add a new check, add it to the functions.py script, and init/imports script and it can be used (also increment the version of the package in settings 'setup' file. 

Use the package manager pip to install Toolbox like below. Rerun this command to check for and install updates (will check versioning so make sure to update this when editing).

pip install git+https://github.com/liamephraims-BPM/BPM_dash_validation_toolkit  

Functions available to import: 


Driver functions: these will perform all checks for a particular stage when inputs are provided:

    function.stage_1_driver  -->  stage_1_driver(clients, validation_client, connection)
        """
        Driver function for running the stage 1 checks  for a client
        Inputs:
            clients: to-do
            validation_client: to-do  """

    function.stage_2_driver  -->  stage_2_driver(primary_parents, clients, validation_client, definition_check_dictionary, connection):
        """
        Driver function for running the stage 2 checks  for a client
        Inputs:
            primary_parents: to-do
            clients: to-do
            definition_check_dictionary: to-do
            validation_client: to-do

            """ 

    function.stage_3_driver  -->    stage_3_driver(dash_to_base_query_dictionary, clients, cumulative_check_dict, onboard_stat_dict, business_logic_dict, between_dash_comparison_dict, validation_client, connection):
        """
        Driver function for running the stage 3 checks  for a client
        Inputs:
            dash_to_base_query_dictionary:
            clients:
            cumulative_check_dict: to-do
            onboard_stat_dict: to-do
            onboard_stat_dict: to-do
            business_logic_dict: to-do
            between_dash_comparison_dict: to-do

        """

Utility functions: more general functions for performing general tasks:

    utility.set_up_client  -->  (client_str, regions_list, prod_databases_list)
                            Create a client object for running and capturing checks


    utility.output_client_validation_results  -->  output_client_validation_results(clients, send_to_slack=True)
                                    At the end of validation, output the warnings/failures of checks and send to slack analytics channel if true 



Check functions: these are the individual checks being run in each stage by the driver functions (though could use individually)


    functions.check_1_1 -->  Utility function for undertaking check 1.1 for checking the count of regional prod data tables against their aggregated union tables 
        Inputs:
          source - "<table-name>" string e.g.'fact_encounter"
          target - "<table-name>" string e.g.'fact_encounter'
          sourcedbs - "<database-name>" string e.g. 'jj_prod' <- regional prod dbs pushed to uk
          targetdbs - "<database-name>" string e.g. 'jj_prod_union' <- corresponding union dbs in uk
          region - "<region>" string e.g. "ap-southeast-1" (singapore) <- the original region for the prod table before being pushed to uk
        Output:
          bool - True/False - 1/0

    functions.check_1_2 -->  """ Utility function for undertaking check 1.2 for checking that there are no nulls in region columns of union table
        Inputs:
          target - "<table-name>" string e.g.'fact_encounter'
          targetdbs - "<database-name>" string e.g. 'jj_prod_union' <- corresponding union dbs in uk
        Output:
          bool - True/False - 1/0   

    functions.check_2_1 -->    Utility function for undertaking check 2.1, checks that primary key or composite PK key count between union and base tables is the same 
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

    functions.check_2_2 -->  Utility function for undertaking check 2.2, checks that primary key or composite PK key of base are all in primary key of union table
        Inputs:
              target -  string base table name  - e.g. fact_encounter
              targetdbs - string base table database name - e.g. jj_base_tables
              target_PK - the string name of the primary key column of table - e.g. encounter_id
                            note: this is derived from the index for the id/column to be counted in base table - col 0 (1) for jj_base_tables.fact_encounter
                                  within the input section for each client
              parent_query - the corresponding query for the corresponding prod union - jj_prod_union.menicon_encounters, col 0 (id)
         Output:
          bool - True/False - 1/0

    functions.check_2_3 -->     Utility function for undertaking check 2.3, checks that every primary key is unique - for base tables this is only the primary id and not the update time	
         Inputs:
              target -  string base table name  - e.g. fact_encounter
              targetdbs - string base table database name - e.g. jj_base_tables
              target_PK - the list or string of the primary key columns of table - e.g. encounter_id or if composite (pathway_name, region) as list
                            note: this is derived from the index for the id/column to be counted in base table - col 0 (1) for jj_base_tables.fact_encounter
                                  within the input section for each client
         Output:
               bool - True/False - 1/0

    functions.check_2_4 --> Utility function for check 2.4, checking that defintion look-up tables are up-to-date - ie no new values in prod which are not in exclusion set & in look-up table
                    Inputs:
                        look-up definition = a string of the name of the defintion to be displayed on print out - e.g. ecp role definition
                        look_up_database = database name string for look up table - e.g. jj_sandbox
                        look_up_table = table name string for look-up table - e.g. ecp_user_roles_LOOK_UP
                        look_up_column = column name string of look-up table for value set being maintained - e.g. name
                        connection: the athena aws connection object
                    Output:
                        bool - True/False - 1/0

    functions.check_3_1 --> check_3_1(pathway_set, pathways, dashboard_stat, connection):
        """ Utility function for undertaking check 3.2, checks that select all == individual pathway sums"""

    functions.check_3_2 --> check_3_2(dashboard_statistic, base_statistic_query, dash_table, dash_database, connection):
        """ Utility function for undertaking check 3.2, checks that a dashboard statistic in end-point dashboard table has the same sum as when calculated 
        directly off the relevant base tables and checks that check 3.1 is true"""

    functions.check_3_3 --> check_3_3(cumulative_dash_query, base_dash_query, regions, cumulative_dash_statistic, connection):
        """ Utility function to test that for the cumulative table, if select all than this is equal the overall base statistic total - query should be by country summed

        functions.check_3_4 --> 
    check_3_4(dashboard_statistic, dash_database, dash_table, base_statistic_query, connection):
        """ Utility function to test check 3.4, which computes if a dashboard statistic, which cannot be summed across pathways as same for all - e.g. onboarded users is the same as base    """

     functiuons.check_3_5 -->  check_3_5(business_logic_query, base_stat_query,business_logic_name, connection):
        """     General utility function to compare if two queries are the same, generalised for testing business logic queries against base table queries  or could also check between dashboard queries   
                   NOTE: Needs to be queries resulting in single counts/ count comparison  """


    functions.check_3_6   --> check_3_6(dash_query_1, dash_query_2,dash_test_name, logical_comparion_operator, connection):
        """     General utility function to compare if two queries are the same, used in this case for two generic dashboard figures or queries  """

