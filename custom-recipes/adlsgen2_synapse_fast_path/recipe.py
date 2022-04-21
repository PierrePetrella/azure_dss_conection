
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from dataiku import SQLExecutor2
import logging


client = dataiku.api_client()
project = client.get_default_project()

# Get handle on input dataset
input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

# Get handle on output dataset name to feed to the "COPY" query
output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)


# Get input adlsgen2 Connection Information & Dataset metadata information
in_config = input_dataset.get_config()
path = in_config["params"]["path"]
container = in_config["params"]["container"]
in_cnx_name = in_config["params"]["connection"]
in_cnx = client.get_connection(in_cnx_name)
storage_account = in_cnx.get_info()["params"]["storageAccount"]
file_name = "/out-s0.csv"

adlsgen2_file_url = "'https://" + storage_account + ".dfs.core.windows.net/" + container + path + file_name + "'"
adlsgen2_file_url = adlsgen2_file_url.replace("${projectKey}",dataiku.default_project_key())
print(adlsgen2_file_url)

### Get output synapse Connection Information & Dataset metadata information
# Get output dataset related information
out_config = output_dataset.get_config()
out_params = out_config["params"]
out_connection = out_config["params"]["connection"]
out_table = out_params["table"]
formated_out_table = out_table.replace("${projectKey}",dataiku.default_project_key())
formated_out_table_w_quote = '"' + formated_out_table +'"'

# Get output connection related information
out_cnx_name = output_dataset.get_config()["params"]["connection"]
out_cnx = client.get_connection(out_cnx_name)
out_database = out_cnx.get_definition()["params"]["db"]


input_format_type = input_dataset.get_config()["formatType"]
if input_format_type == 'csv':
    logging.error("the input format type must be CSV, not " +input_format_type)

# Plain drop without check if table exists..
#drop_if_0 = "DROP TABLE " + formated_out_table_w_quote + " END; "
#print ("drop_if_0")
#print(drop_if_0)
#print("")

# Search if an object exists in DSS and then drop the object hoping it is a table
#drop_if_1 = "IF OBJECT_ID(N'" + out_database +".." + formated_out_table + "') IS NOT NULL BEGIN DROP TABLE " + \
#        formated_out_table_w_quote + " END; "
#print ("drop_if_1")
#print(drop_if_1)
#print("")

# Search if a table exists in the default dbo database
#drop_if_2 = "IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like '" + formated_out_table + \
#           "%') BEGIN DROP TABLE " + formated_out_table_w_quote  + "; END; "
#print("drop_if_2")
#print(drop_if_2)
#print("")




query_copy = " COPY INTO " + formated_out_table_w_quote + " FROM " + adlsgen2_file_url + """
    WITH (
        FILE_TYPE = 'CSV',
        FIELDQUOTE = '0x00',
        COMPRESSION = 'NONE',
        FIELDTERMINATOR='\\t'
    )"""
print ("query_copy")
print(query_copy)
print("")

#fp_query = drop_if_2 + query_copy

#print ("final_query:")
#print(fp_query)
#print("")

# Execute the conditional drop and COPY IN command
#executor = SQLExecutor2(dataset=output_dataset)
#executor.query_to_df(fp_query)
#print("Done")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#tmp_dataset = project.create_dataset("tmp_dataset_fp"  # dot is not allowed in dataset names
#        ,'Synapse'
#        , params={
#            'connection': out_connection
#        }, formatType='csv')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#tmp_dataset_handle = dataiku.Dataset("tmp_dataset_fp")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Get schema from inputand write in output
#schema = input_dataset.get_config()["schema"]["columns"]
#tmp_dataset_handle.write_schema(schema)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Get df from input schema and write back 
#out_df = tmp_dataset_handle.get_dataframe()
#output_dataset.write_from_dataframe(out_df)
#out_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#tmp_dataset.delete(drop_data=True)
generator = input_dataset.iter_dataframes(chunksize=1)
df = next(generator)
df_empty = df.drop(index = [0])
output_dataset.write_from_dataframe(df_empty)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# QUERY COPY
executor = SQLExecutor2(dataset=output_dataset)
executor.query_to_df(query_copy)

