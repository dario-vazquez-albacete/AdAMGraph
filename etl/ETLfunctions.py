import pandas as pd
from Classes import Neo4jConnection
import json
import logging
from dotenv import dotenv_values

# create connection object using neo4j
data = dotenv_values(".env")
conn = Neo4jConnection(uri=data["NEO4J_URI"], 
                       username=data["NEO4J_USER"],              
                       password=data["NEO4J_PASSWORD"])

db="neo4j"

# Create graph nodes

async def create_patients_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds patient nodes to the Neo4j graph.
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (p:Patient {USUBJID: row.USUBJID, AGE: row.AGE, ARM:row.ARM, SEX: row.SEX, BMI: row.BMIBL})
    RETURN p
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")
    
async def create_chemlab_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds chemical laboratory measurements nodes to the Neo4j graph.
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (pa:Parameter:Chemistry {USUBJID: row.USUBJID, VISIT: row.VISIT, Laboratory: row.PARCAT1, Parameter: row.PARAM, Value: row.AVAL, Reference: row.LBNRIND, Dataset: 'adlbc'})
    RETURN pa
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN pa.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_hemolab_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds hematology laboratory measurements nodes to the Neo4j graph.
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (pa:Parameter:Hematology {USUBJID: row.USUBJID, VISIT: row.VISIT, Laboratory: row.PARCAT1, Parameter: row.PARAM, Value: row.AVAL, Reference: row.LBNRIND, Dataset: 'adlbh'})
    RETURN pa
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN pa.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_vitalsigns_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds vital signs measurements nodes to the Neo4j graph.
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (vs:Parameter:VitalSign {USUBJID: row.USUBJID, VISIT: row.VISIT, Laboratory: 'VS', Parameter: row.PARAM, Value: row.AVAL, Reference: '', Dataset: 'advs'})
    RETURN vs
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN vs.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_adadas_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds ADADAS endpoint nodes
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (ep:Endpoint:ADAS {USUBJID: row.USUBJID, VISIT: row.VISIT, EndpointName: 'ADAS-Cog', Parameter: row.PARAM, Value: row.AVAL, Reference: '', Dataset: 'adadas'})
    RETURN ep
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN ep.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_cibc_nodes(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    # Adds ADADAS endpoint nodes
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    CREATE (ep:Endpoint:CIBC {USUBJID: row.USUBJID, VISIT: row.VISIT, EndpointName: 'CIBC Score', Parameter: row.PARAM, Value: row.AVAL, Reference: '', Dataset: 'adcibc'})
    RETURN ep
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN ep.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_treatment_nodes(df: pd.DataFrame, logger:  logging.Logger):
    treatment_set = set(df['ARM'])
    treatment_list = list(treatment_set)
    try: 
        query = '''
        UNWIND $list AS item
        MERGE (t:Treatment {Name: item})
        '''
        return conn.query(query, parameters= {'list': treatment_list}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")
        

async def create_adverseevent_nodes(df: pd.DataFrame, logger:  logging.Logger):
    adversevent_set = set(df['AETERM'])
    adversevent_list = list(adversevent_set)
    try: 
        query = '''
        UNWIND $list AS item
        MERGE (ae:AdverseEvent {Term: item})
        '''
        return conn.query(query, parameters={'list': adversevent_list}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_visit_nodes(df: pd.DataFrame, logger:  logging.Logger):
    visit_set = set(df['VISIT'])
    visit_list = list(visit_set)
    try:
        query = '''
        UNWIND $list AS item
        MERGE (v:Visit {Name: item})
            '''
        return conn.query(query, parameters = {'list': visit_list}, db=db)
    except Exception as e:
        logger.error(f"Error sending the data: {e}")


# Create relationships

async def create_patient_treatment_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (t:Treatment {Name:row.ARM}) 
    MERGE (p)-[r:WAS_TREATED]->(t)
    SET r.Dose=row.TRT01PN
    RETURN p, r
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_adverseevent_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (ae:AdverseEvent {Term: row.AETERM}) 
    MERGE (p)-[r:EXPERIENCED_ADVERSE_EVENT]->(ae)
    SET r={Severity: row.AESEV, Type: row.AEBODSYS}
    RETURN p, r
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_visit_relationship(df: pd.DataFrame, logger:  logging.Logger):
    unique_df = df.drop_duplicates(subset=['USUBJID', 'VISIT'])
    data = unique_df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (v:Visit {Name: row.VISIT}) 
    MERGE (p)-[r:ATTENDED_VISIT]->(v)
    RETURN p, r
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
    '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_chemistrylab_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (param:Parameter:Chemistry {USUBJID: row.USUBJID, VISIT: row.VISIT, Laboratory: row.PARCAT1,Parameter: row.PARAM, Value: row.AVAL, Reference: row.LBNRIND, Dataset: 'adlbc'}), (v:Visit {Name: row.VISIT})
    CREATE (p)-[lb:MEASURED_LABPARAMETER]->(param)
    CREATE (param)<-[:MEASURED_IN_VISIT]-(v)
    SET lb={ChangeFromBaseline: row.CHG}
    RETURN p, lb
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
        '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_hemolab_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (param:Parameter:Hematology {USUBJID: row.USUBJID,VISIT: row.VISIT, Laboratory: row.PARCAT1,Parameter: row.PARAM, Value: row.AVAL, Reference: row.LBNRIND, Dataset: 'adlbh'}), (v:Visit {Name: row.VISIT})
    CREATE (p)-[lb:MEASURED_LABPARAMETER]->(param)
    CREATE (param)<-[:MEASURED_IN_VISIT]-(v)
    SET lb={ChangeFromBaseline: row.CHG}
    RETURN p, lb
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
        '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_vitalsign_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (param:Parameter:VitalSign {USUBJID: row.USUBJID, VISIT: row.VISIT, Laboratory: 'VS' ,Parameter: row.PARAM, Value: row.AVAL}), (v:Visit {Name: row.VISIT})
    CREATE (p)-[vs:MEASURED_VITALSIGN]->(param)
    CREATE (param)<-[:MEASURED_IN_VISIT]-(v)
    SET vs={ChangeFromBaseline: row.CHG}
    RETURN p, vs
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
        '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_adadas_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (end:Endpoint:ADAS {USUBJID: row.USUBJID, VISIT: row.VISIT, EndpointName: 'ADAS-Cog', Parameter: row.PARAM, Value: row.AVAL, Reference: '', Dataset: 'adadas'}), (v:Visit {Name: row.VISIT})
    CREATE (p)-[endrel:ASSESSED_ENDPOINT]->(end)
    CREATE (end)<-[:MEASURED_IN_VISIT]-(v)
    SET endrel={ChangeFromBaseline: row.CHG}
    RETURN p, endrel
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
        '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")

async def create_patient_cibc_relationship(df: pd.DataFrame, logger:  logging.Logger):
    data = df.to_dict(orient='records')
    query = '''
    UNWIND $rows AS row
    CALL {
    WITH row
    MATCH (p:Patient {USUBJID: row.USUBJID}), (end:Endpoint:CIBC {USUBJID: row.USUBJID, VISIT: row.VISIT, EndpointName: 'CIBC Score', Parameter: row.PARAM, Value: row.AVAL, Reference: '', Dataset: 'adcibc'}), (v:Visit {Name: row.VISIT})
    CREATE (p)-[endrel:ASSESSED_ENDPOINT]->(end)
    CREATE (end)<-[:MEASURED_IN_VISIT]-(v)
    SET endrel={ChangeFromBaseline: row.CHG}
    RETURN p, endrel
    } IN TRANSACTIONS OF 10000 ROWS
    ON ERROR CONTINUE
    REPORT STATUS AS s
    RETURN p.USUBJID, s.started, s.committed, s.errorMessage
        '''
    try: 
        return conn.query(query, parameters = {'rows': data}, db=db)
    except Exception as e:
            logger.error(f"Error sending the data: {e}")