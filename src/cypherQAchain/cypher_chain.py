import json
from langchain_openai import ChatOpenAI
from langchain.prompts import (
    PromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
    ChatPromptTemplate,
)
from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from dotenv import dotenv_values


# create connection object using neo4j
data = dotenv_values(".env")

graph = Neo4jGraph(
    url=data["NEO4J_URI"], 
                       username=data["NEO4J_USER"],              
                       password=data["NEO4J_PASSWORD"]
)

async def graph_chain(Question: str):
    system_prompt_template = """Task:Generate Cypher statement to query a graph database that represents a clinical trial.
    Instructions:
    Use only the provided relationship types and properties in the schema.
    Check the schema before building your query to make sure you filter the right property of the corresponding node.
    Temperature,Weight, Blood Pressure, Height and Pulse Rate are VitalSign nodes, not hematology nor chemistry parameter nodes.
    Do not use any other relationship types or properties that are not provided.
    Do not use MATCH clause when filtering text node properties.
    Use the WHERE clause together with the CONATINS clause to filter text node properties even when filtering multiple properties
    If the user requests a table or output in tablular format, return an array of objects for each record.
    Make sure that node properties are capitalized or not capitalized accordingly.
    Do not use variable names to filter on numeric properties.
    Make sure to capitalize the first letter of each node property word.
    Schema:
    {schema}
    Note: Do not include any explanations or apologies in your responses.
    Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
    Do not include any text except the generated Cypher statement.
    Examples of generated Cypher statements for specific questions:
    # How many patients does the study contain?
    MATCH (p:Patient)
    RETURN count(p) AS numberOfPatients
    # How many patients experienced an adverse event?
    MATCH (p:Patient)-[:EXPERIENCED_ADVERSE_EVENT]-(ae:AdverseEvent)
    RETURN COUNT(DISTINCT p.USUBJID)
    # How many patients have been assessed on the ADAS endpoint?
    MATCH (p:Patient)-[:ASSESSED_ENDPOINT]-(end:Endpoint)
    WHERE end.EndpointName CONTAINS 'ADAS'
    RETURN COUNT(DISTINCT p.USUBJID)
    # How many patients have been assessed on the CIBC endpoint?
    MATCH (p:Patient)-[:ASSESSED_ENDPOINT]-(end:Endpoint)
    WHERE end.EndpointName CONTAINS 'CIBC'
    RETURN COUNT(DISTINCT p.USUBJID)
    # Give me the hematology laboratory parameters and their values for the Patient with unique identifier 01-701-1192
    MATCH (p:Patient {{USUBJID: '01-701-1192'}})-[:MEASURED_LABPARAMETER]-(pa:Parameter:Hematology)
    RETURN pa.Parameter, pa.Value
    # Give me the chemistry laboratory parameters and their values for the Patient with unique identifier 01-701-1192
    MATCH (p:Patient {{USUBJID: '01-701-1192'}})-[:MEASURED_LABPARAMETER]-(pa:Parameter:Chemistry)
    RETURN pa.Parameter, pa.Value
    # What endpoints are evaluated in the clinical trial?
    MATCH (end:Endpoint)
    RETURN DISTINCT end.EndpointName
    # What patient had the highest hemoglobin value
    MATCH (p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter)
    WHERE pa.Parameter CONTAINS 'Hemoglobin'
    RETURN p.USUBJID, pa.Value
    ORDER BY pa.Value DESC
    LIMIT 1
    # What is the treatment group with more patients experiencing a cariac adverse event?
    MATCH (t:Treatment)<-[:WAS_TREATED]-(p:Patient)-[:EXPERIENCED_ADVERSE_EVENT]->(ae:AdverseEvent)
    WHERE ae.Term CONTAINS 'CARDIAC'
    WITH t, COUNT(DISTINCT p.USUBJID) AS numPatients
    RETURN t.Name AS TreatmentGroup, numPatients
    ORDER BY numPatients DESC
    LIMIT 1
    # I need the average albumin levels of the patients in each treatment group in the screeining visit
    MATCH (t:Treatment)<-[:WAS_TREATED]-(p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter)-[:MEASURED_IN_VISIT]-(vis:Visit)
    WHERE pa.Parameter CONTAINS 'Albumin' AND vis.Name CONTAINS 'SCREENING'
    RETURN t.Name AS TreatmentGroup, AVG(pa.Value) AS AverageAlbuminLevels
    # Give me a table with the hemoglobin measurements of the patients in each visit
    MATCH (p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter:Hematology)<-[:MEASURED_IN_VISIT]-(v:Visit)
    WHERE pa.Parameter CONTAINS 'Hemoglobin'
    RETURN p.USUBJID, v.Name, pa.Value, pa.Parameter
    # Give me a table with the hemoglobin measurements of the patients for visit at 2 weeks
    MATCH (p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter)<-[:MEASURED_IN_VISIT]-(v:Visit)
    WHERE pa.Parameter CONTAINS 'Hemoglobin' AND v.Name='WEEK 2'
    RETURN p.USUBJID, v.Name, pa.Value, pa.Parameter
    # Give me a table with monocytes counts for all patients in visit at 4 weeks and their treatment group
    MATCH (p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter:Hematology)-[:MEASURED_IN_VISIT]-(v:Visit)
    MATCH (p)-[:WAS_TREATED]->(t:Treatment)
    WHERE pa.Parameter CONTAINS 'Monocytes' AND v.Name CONTAINS 'WEEK 4'
    RETURN p.USUBJID, t.Name AS TreatmentGroup, pa.Value, pa.Parameter
    # I need the sodium levels for each patients in each visit in tabular format
    MATCH (p:Patient)-[:MEASURED_LABPARAMETER]-(pa:Parameter)<-[:MEASURED_IN_VISIT]-(v:Visit)
    WHERE pa.Parameter CONTAINS 'Sodium'
    RETURN p.USUBJID, v.Name, pa.Value, pa.Parameter
    # What are the blood pressures of each patient and their treatment in the screening visit?
    MATCH (t:Treatment)-[:WAS_TREATED]-(p:Patient)-[:MEASURED_VITALSIGN]-(vs:VitalSign)<-[:MEASURED_IN_VISIT]-(v:Visit)
    WHERE vs.Parameter CONTAINS 'Blood Pressure'
    RETURN p.USUBJID,t.Name, v.Name, vs.Value, vs.Parameter
    """

    system_prompt = SystemMessagePromptTemplate(
        prompt=PromptTemplate(
            input_variables=["schema"],
            template=system_prompt_template,
        )
    )

    human_prompt = HumanMessagePromptTemplate(
        prompt=PromptTemplate(
            input_variables=["question"],
            template="{question}",
        )
    )
    messages = [system_prompt, human_prompt]

    full_prompt_template = ChatPromptTemplate(
        input_variables=["schema", "question"],
        messages=messages,
    )

    cypherChain = GraphCypherQAChain.from_llm(
        ChatOpenAI(model="gpt-3.5-turbo-0125",temperature=0, openai_api_key=data['OPENAI_API_KEY']),
        graph=graph,
        verbose=True,
        return_direct=True,
        return_intermediate_steps=True,
        cypher_prompt=full_prompt_template,
        validate_cypher=True,
    )

    cypher_chain = full_prompt_template | cypherChain
    response = cypher_chain.invoke({"schema":graph.schema,"question": Question})
    return response