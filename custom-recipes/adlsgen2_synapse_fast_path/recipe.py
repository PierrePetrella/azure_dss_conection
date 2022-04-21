
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from dataiku import SQLExecutor2


client = dataiku.api_client()
project = client.get_default_project()

### Get handles in INPUT and OUTPUT
# Get handle on input dataset
input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

# Get handle on output dataset name to feed to the "COPY" query
output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)


### Get input adlsgen2 Connection Information & Dataset metadata information
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


### CHECKS INPUT and OUTPUT respect the requirments

# Check input connection is adlsgen2
input_cnx_type = in_cnx.get_info()["type"]
if input_cnx_type != 'Azure':
    raise Exception("The input format type must be Azure, not " +input_cnx_type)

# Check input is stored as CSV
input_format_type = input_dataset.get_config()["formatType"]
if input_format_type != 'csv':
    #logging.error("the input format type must be CSV, not " +input_format_type)
    raise Exception("The input format type must be CSV, not " +input_format_type)

# Check output connection is synapse
output_cnx_type = out_cnx.get_info()["type"]
if output_cnx_type != 'Synapse':
    raise Exception("The output connection must be Synapse, not " +output_cnx_type)

### Build COPY INTO query (CSV only)
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

### Fetch schema from input and create empty table with schema in output
generator = input_dataset.iter_dataframes(chunksize=1)
df = next(generator)
df_empty = df.drop(index = [0])
output_dataset.write_from_dataframe(df_empty)

### QUERY COPY
executor = SQLExecutor2(dataset=output_dataset)
executor.query_to_df(query_copy)

