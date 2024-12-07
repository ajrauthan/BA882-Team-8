import streamlit as st
import db_dtypes
import json
from google.cloud import bigquery
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerativeModel, ChatSession
from vertexai.generative_models import Part
from vertexai.generative_models import (
    Content,
    FunctionDeclaration,
    GenerationConfig,
    Tool,
    ToolConfig
)
from google.cloud.exceptions import NotFound


# Initialize BigQuery Client
client = bigquery.Client(project="ba882-team8-fall24")

# Define project and dataset info
project_id = "ba882-team8-fall24"
dataset_id = "mbta_LLM"
table_id = "LLM_join"
fully_qualified_table = f"{project_id}.{dataset_id}.{table_id}"

# Initialize GenAI model
model = GenerativeModel(model_name="gemini-1.5-flash-002")  # Replace with your GenAI model details
generation_config = GenerationConfig(temperature=0, response_mime_type="application/json")

# Streamlit App UI
st.title("Interactive SQL Query Generator with GenAI")
st.markdown("Enter your query prompt, and the system will generate and execute the corresponding SQL on BigQuery.")

user_input = st.text_area("Enter your query prompt:", "")

# Handle user input
if st.button("Generate and Execute SQL Query"):
    if user_input:
        # Fetch schema from BigQuery
        try:
            table_ref = client.get_table(fully_qualified_table)
            schema = {field.name: field.field_type for field in table_ref.schema}
        except NotFound as e:
            st.error(f"Error fetching schema: {e}")
            schema = {}

        # Construct the schema description and prompt
        if schema:
            schema_description = "\n".join([f"{name} ({dtype})" for name, dtype in schema.items()])
            prompt = f"""
            You are a SQL expert. Based on the user's input and the provided schema, generate a valid SQL query.
            Use the table `{fully_qualified_table}`. Ensure the query references the table and its columns correctly.
            Return the result as a valid JSON with a key `SQL` containing the SQL query.

            ### Schema
            {schema_description}

            ### User Prompt
            {user_input}

            ### SQL Query (as JSON)
            """

            # Use GenAI to generate the SQL query
            user_prompt_content = Content(
                role="user",
                parts=[Part.from_text(prompt)],
            )

            try:
                response = model.generate_content(user_prompt_content, generation_config=generation_config)

                # Extract the generated SQL query from the response
                llm_query = json.loads(response.text)
                sql_query = llm_query.get("SQL")

                if sql_query:
                    # Ensure proper fully qualified table references
                    if fully_qualified_table not in sql_query:
                        sql_query = sql_query.replace("LLM_join", fully_qualified_table)

                    # Display the generated SQL query
                    st.markdown("### Generated SQL Query")
                    st.code(sql_query)

                    # Execute the SQL query against BigQuery
                    try:
                        query_job = client.query(sql_query)
                        result = query_job.result()  # Wait for the query to finish
                        df = result.to_dataframe()  # Convert result to DataFrame

                        # Remove rows with null values
                        df_cleaned = df.dropna()

                        # Display the query results
                        st.markdown("### Query Results")
                        st.dataframe(df)  # Show result table
                    except Exception as e:
                        st.error(f"An error occurred while executing the query: {e}")
                else:
                    st.error("No SQL query was generated.")
            except Exception as e:
                st.error(f"Error generating the SQL query: {e}")
        else:
            st.error("Failed to fetch schema from BigQuery.")
    else:
        st.warning("Please enter a query prompt.")
