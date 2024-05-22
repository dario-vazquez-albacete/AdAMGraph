from fastapi import FastAPI
from cypherQAchain.cypher_chain import graph_chain
import pandas as pd
import json
from pydantic import BaseModel

class Query(BaseModel):
    question: str

app = FastAPI(
    title="Clinical Trial Cypher Agent",
    description="Endpoints for a AI agent that translate natural language to Cypher queries",
)

@app.get("/")
async def get_status():
    return {"status": "running"}

@app.post("/graph-question")
async def query_clinical_graph(query: Query):
    print(query.question)
    query_response = await graph_chain(query.question)
    return query_response

# run the FastAPI with command: uvicorn main:app --host 0.0.0.0 --port 8000