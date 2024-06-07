import pandas as pd
import sqlalchemy as sa
import asyncio
from prefect import task, flow, get_run_logger, serve
import yaml

# Load YAML file with pipeline configuration
with open("pipeline_config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Import ETL Functions to create nodes from the SQL queries
from ETLfunctions import (
                              create_patients_nodes,
                                create_chemlab_nodes,
                                create_hemolab_nodes,
                                create_vitalsigns_nodes,
                                create_adadas_nodes,
                                create_cibc_nodes,
                                create_treatment_nodes,
                                create_adverseevent_nodes,
                                create_visit_nodes)

# Import ETL Functions to create edges from the SQL queries

from ETLfunctions import (
create_patient_treatment_relationship,
create_patient_adverseevent_relationship,
create_patient_chemistrylab_relationship,
create_patient_hemolab_relationship,
create_patient_vitalsign_relationship,
create_patient_adadas_relationship,
create_patient_cibc_relationship,
create_patient_visit_relationship)

# Parses function names referenced in the pipeline_configuration.yaml file into actual python function objects imported

function_parser = {
"create_patients_nodes": create_patients_nodes,
"create_chemlab_nodes":create_chemlab_nodes,
"create_hemolab_nodes":create_hemolab_nodes,
"create_vitalsigns_nodes":create_vitalsigns_nodes,
"create_adadas_nodes":create_adadas_nodes,
"create_cibc_nodes":create_cibc_nodes,
"create_treatment_nodes":create_treatment_nodes,
"create_adverseevent_nodes": create_adverseevent_nodes,
"create_visit_nodes": create_visit_nodes,
"create_patient_treatment_relationship":create_patient_treatment_relationship,
"create_patient_adverseevent_relationship":create_patient_adverseevent_relationship,
"create_patient_chemistrylab_relationship":create_patient_chemistrylab_relationship,
"create_patient_hemolab_relationship": create_patient_hemolab_relationship,
"create_patient_vitalsign_relationship":create_patient_vitalsign_relationship,
"create_patient_adadas_relationship":create_patient_adadas_relationship,
"create_patient_cibc_relationship":create_patient_cibc_relationship,
"create_patient_visit_relationship": create_patient_visit_relationship
}

@task(name="read-sas-file", description="Passes sql state ment to run a query")
async def read_data(path: str):
    df = pd.read_sas(path,format='xport', encoding="utf-8")
    return df

@task(name="split-dataframe", description="Splits large dataframes into chunks")
async def split_dataframe(df:pd.DataFrame, max_chunk_size:int):
    num_rows = len(df)
    num_chunks = max(1, num_rows // max_chunk_size)  # Ensure at least one chunk
    chunk_size = num_rows // num_chunks
    remainder = num_rows % num_chunks  # Calculate the remainder
    # Split the dataframe into chunks
    chunks = [df.iloc[i * chunk_size: (i + 1) * chunk_size] for i in range(num_chunks)]
    # Add the remainder rows to the last chunk
    if remainder:
        last_chunk = df.iloc[-remainder:]
        chunks[-1] = pd.concat([chunks[-1], last_chunk])
    return chunks

@task(name="wirte-graph", description="Executes a writing operation on the graph")
async def write_graph(function, split_results: pd.DataFrame, logger):
    process_chunks = []
    for chunk in split_results:
        write_task = function(chunk, logger)
        process_chunks.append(write_task)
    await asyncio.gather(*process_chunks)


@flow
async def subflow(path: str, function):
    logger = get_run_logger()
    result = await read_data(path)
    split_results = await split_dataframe(result, 100)
    await write_graph(function, split_results, logger)
    del result

@flow(name="create-graph-flow",log_prints=True)
async def main_flow():
    # Configure the pipeline to load the desired data to the graph
    # Create and run node subflows
    node_subflows = []
    for item in config["create_nodes_functions"]:
        file_path = item["file_path"]
        function_name = function_parser.get(item["function"])
        node_type = item["node_type"]
        if node_type.startswith('visit'):
            visit_datasets = ['adlbc.xpt', 'adlbh.xpt', 'advs.xpt', 'adadas.xpt', 'adcibc.xpt']
            for i in visit_datasets:
                sub = subflow.with_options(name=f'Subflow for {node_type} nodes')(file_path+i, function_name)
                node_subflows.append(sub)
        else:
            sub = subflow.with_options(name=f'Subflow for {node_type} nodes')(file_path, function_name)
            node_subflows.append(sub)
    await asyncio.gather(*node_subflows)
    edges_subflows = []
    for item in config["create_edges_functions"]:
        file_path = item["file_path"]
        function_name = function_parser.get(item["function"])
        edge_type = item["edge_type"]
        if edge_type.startswith('visit'):
            visit_datasets = ['adlbc.xpt', 'adlbh.xpt', 'advs.xpt', 'adadas.xpt', 'adcibc.xpt']
            for i in visit_datasets:
                sub = subflow.with_options(name=f'Subflow for {edge_type} edges')(file_path+i, function_name)
                edges_subflows.append(sub)
        else:
            sub = subflow.with_options(name=f'Subflow for {edge_type} edges')(file_path, function_name)
            edges_subflows.append(sub)
    await asyncio.gather(*edges_subflows)

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    main_flow.serve('GraphETLdeployment')
    elapsed = time.perf_counter() - s
    print(f"Pipeline run in {elapsed:0.2f} seconds.")