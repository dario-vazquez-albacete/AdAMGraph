import os
import requests
import streamlit as st
import pandas as pd
import json

CHATBOT_URL = os.getenv("CHATBOT_URL")

with st.sidebar:
    st.header("About")
    st.markdown(
        """
        This chatbot interfaces with a
        [LangChain](https://python.langchain.com/docs/get_started/introduction)
        agent designed to tranlsate questions into Cypher queries.
        The AI agent returns the results of your question as a table and the cypher query produced to generate the table.
        You can check and tune the cypher query by running it in Neo4j browser interface
        """
    )

    st.header("Example Questions")
    st.markdown("- How many patients were enrolled in the study?")
    st.markdown("- What patient had the highest hemoglobin value?")
    st.markdown(
        "- What is the treatment group with more patients experiencing a cardiac adverse event?"
    )
    st.markdown("- Give me a table with the hemoglobin measurements of the patients for visit at 2 weeks")
    st.markdown(
        "- I need the sodium levels for each patients in each visit in tabular format"
    )
    st.markdown("- Give me a table with the hemoglobin measurements of the patients in each visit")

st.title("Clinical Data Manager Assistant")
st.info(
    "Ask me questions about the clinical trial stored in the clinical trial graph model. "
    "Request specific datasets to answer questions about patients and explore the clinical trial"
)

st.info("IMPORTANT: Make sure to validate the answer produced. You can do it by running the query in the Neo4j browser interface")
st.image('AdamGraph.png', caption='Clinical trial graph model')

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if "output" in message.keys():
            st.markdown(message["output"])

        # if "explanation" in message.keys():
        #     with st.status("How was this generated", state="complete"):
        #         st.info(message["explanation"])

if prompt := st.chat_input("What do you want to know?"):
    st.chat_message("user").markdown(prompt)

    st.session_state.messages.append({"role": "user", "output": prompt})

    data = {'question': prompt}
    with st.spinner("Querying the database..."):
        response = requests.post(url=CHATBOT_URL, json=data)
        if response.status_code == 200:
            cypher_table = response.json()["result"]
            explanation = response.json()["intermediate_steps"]
            cypher_query = str(explanation[0]['query'])

        else:
            cypher_table = """An error occurred while processing your message.
            Please try again or rephrase your message."""
            explanation = cypher_table
    
    df = pd.DataFrame(cypher_table)
    st.chat_message("assistant").table(df)
    st.download_button(
   "Press to Download",
   df.to_csv(index=False).encode('utf-8'),
   "file.csv",
   "text/csv",
   key='download-csv'
)
    st.status("Cypher query produced:", state="complete").code(cypher_query)
    st.session_state.messages.append(
        {   "role": "assistant",
            "output": cypher_table,
            "explanation": cypher_query,
        }
    )