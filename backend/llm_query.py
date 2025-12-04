import os
import re
import uvicorn
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from huggingface_hub import InferenceClient

GRAPHDB_URL = "http://localhost:7200/repositories/disease-kg/sparql"

HF_API_TOKEN = "can't share it sorry :D"  
HF_MODEL = "meta-llama/Llama-3.1-8B-Instruct"

client = InferenceClient(
    model=HF_MODEL,
    token=HF_API_TOKEN,
    timeout=60,
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str


def ask_huggingface(question: str) -> str:
    prompt = f"""
You are an expert in SPARQL and semantic knowledge graphs. 
We have a knowledge graph with namespace DIS: <http://diseases.org/disease-kg/> and SNOMED: <http://snomed.info/id/>.

The graph contains HealthRecord entities with the following structure:

DIS:HealthRecord
  - dis:location          (string)      : e.g., "Europe"
  - dis:year              (integer)     : e.g., 2015
  - dis:causeName         (string)      : e.g., "Tuberculosis"
  - dis:cause             (IRI)         : SNOMED URI for the cause - find it yourself
  "HIV/AIDS": "86406008",
    "Leukemia": "93143009",
    "Diabetes mellitus type 2": "44054006",
    "Tuberculosis": "56717001",
    "COVID-19": "840539006",
    "Prostate cancer": "399068003", 
    "Stroke": "230690007",
    "Breast cancer": "254837009",
    "Stomach cancer": "363406005",
    "Anorexia nervosa": "447562003",
    "Schizophrenia": "58214004",
    "Bipolar disorder": "13746004",
    "Bulimia nervosa": "439960005",
    "Malaria": "1386000" HERE ARE THE CODES
  - dis:measureId          (integer)
  - dis:metricId           (integer)
  - dis:sexId              (integer)
  - dis:ageId              (integer)
  - dis:sex                (string)
  - dis:age                (string)
  - dis:metric             (string)      : e.g., "Number"
  - dis:value              (float)       : the numeric value
  - dis:measure            (string)      : e.g., "Deaths", "Prevalence", "Incidence"
  - dis:measureDescription (string)      : textual description of the measure

All literals are simple types (string, integer, float) and do NOT contain language tags like @en. SNOMED IDs are used as IRIs.

Convert the following natural language question into a SPARQL query.
Return EXACTLY in this format:
SPARQL: <your query>
VISUALIZATION: <bar|line|pie|map>

Do NOT include @en or other language tags in the query.
DO NOT GENERATE ANY OTHER TEXT IN THE SPARQL QUERY LIKE COMMENTS

Question: {question}
"""
    completion = client.chat.completions.create(
        model=HF_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
    )

    return completion.choices[0].message["content"]


def clean_sparql(llm_output: str) -> str:
    """Извлекува валиден SPARQL query од LLM output."""
    match = re.search(r"```sparql\s*(.*?)```", llm_output, re.DOTALL | re.IGNORECASE)
    if match:
        query = match.group(1)
    else:
        match = re.search(r"SPARQL:\s*(.*?)(VISUALIZATION:|$)", llm_output, re.DOTALL | re.IGNORECASE)
        query = match.group(1) if match else None

    if not query:
        return None

    query = query.replace("```", "")
    query = re.sub(r'\"@en', '"', query)

    lines = query.splitlines()
    valid_lines = []
    brace_count = 0
    for line in lines:
        brace_count += line.count("{")
        brace_count -= line.count("}")
        valid_lines.append(line)
        if brace_count <= 0 and "}" in line:
            break
    query = "\n".join(valid_lines).strip()

    return query


@app.post("/ask")
def ask_llm(req: AskRequest):
    try:
        llm_output = ask_huggingface(req.question)
    except Exception as e:
        return {"error": f"Failed to call Hugging Face API: {e}"}

    sparql_query = clean_sparql(llm_output)

    vis_match = re.search(r"VISUALIZATION:\s*(\w+)", llm_output, re.IGNORECASE)
    visualization = vis_match.group(1).strip() if vis_match else "bar"

    if not sparql_query:
        return {"error": "Could not parse SPARQL from LLM output", "llm_output": llm_output}

    headers = {
    "Accept": "application/sparql-results+json"
    }

    try:
        r = requests.get(GRAPHDB_URL, params={"query": sparql_query}, headers=headers, timeout=120)
        r.raise_for_status()
        result = r.json()
    except Exception as e:
        result = {"error": "GraphDB returned invalid response", "details": str(e), "text": getattr(r, "text", "")}

    return {
        "sparql": sparql_query,
        "result": result,
        "visualization": visualization
    }



if __name__ == "__main__":
    uvicorn.run("llm_query:app", host="127.0.0.1", port=8001, reload=True)
