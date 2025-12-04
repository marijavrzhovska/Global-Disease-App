from fastapi import FastAPI
from pydantic import BaseModel
import requests
import re
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv

# Load HF API key from .env
#load_dotenv()
#HF_API_KEY = os.getenv("HF_API_KEY")
HF_API_KEY="can't share it sorry :D"

HF_MODEL = "meta-llama/Llama-3.1-8B-Instruct"  
#HF_API_URL = f"https://api-inference.huggingface.co/models/{HF_MODEL_NAME}"
#HF_HEADERS = {"Authorization": f"Bearer {HF_API_KEY}"}

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GRAPHDB_URL = "http://localhost:7200/repositories/disease-kg"

class AskRequest(BaseModel):
    question: str

class QueryAnalysis:
    def __init__(self):
        self.diseases = {
            'malaria': 'Malaria', 'маларија': 'Malaria',
            'lung cancer': 'Lung Cancer', 'белодробен рак': 'Lung Cancer',
            'tuberculosis': 'Tuberculosis', 'туберкулоза': 'Tuberculosis',
            'covid': 'COVID-19', 'ковид': 'COVID-19', 'covid-19': 'COVID-19',
            'diabetes': 'Diabetes mellitus type 2', 'дијабетес': 'Diabetes mellitus type 2',
            'hiv': 'HIV/AIDS', 'aids': 'HIV/AIDS', 'hiv/aids': 'HIV/AIDS',
            'breast cancer': 'Breast cancer', 'рак на дојка': 'Breast cancer',
            'prostate cancer': 'Prostate cancer', 'рак на простата': 'Prostate cancer',
            'stomach cancer': 'Stomach cancer', 'рак на желудник': 'Stomach cancer',
            'leukemia': 'Leukemia', 'леукемија': 'Leukemia',
            'stroke': 'Stroke', 'мозочен удар': 'Stroke',
            'anorexia': 'Anorexia nervosa', 'анорексија': 'Anorexia nervosa',
            'schizophrenia': 'Schizophrenia', 'шизофренија': 'Schizophrenia',
            'bipolar': 'Bipolar disorder', 'биполарно': 'Bipolar disorder',
            'bulimia': 'Bulimia nervosa', 'булимија': 'Bulimia nervosa'
        }
        
        self.locations = {
            'africa': 'Africa', 'африка': 'Africa',
            'europe': 'Europe', 'европа': 'Europe', 
            'asia': 'Asia', 'азија': 'Asia',
            'america': 'America', 'америка': 'America',
            'world': 'Global', 'свет': 'Global', 'global': 'Global', 'глобално': 'Global',
            'albania': 'Albania', 'denmark': 'Denmark', 'hungary': 'Hungary',
            'spain': 'Spain', 'macedonia': 'Macedonia', 'serbia': 'Serbia', 'russia': 'Russia'
        }
        
        self.measures = {
            'deaths': 'Deaths', 'смртност': 'Deaths', 'умрени': 'Deaths', 'died': 'Deaths',
            'cases': 'Prevalence', 'случаи': 'Prevalence',
            'prevalence': 'Prevalence', 'распространетост': 'Prevalence',
            'incidence': 'Incidence', 'инциденција': 'Incidence', 'new cases': 'Incidence'
        }

    def analyze_question(self, question: str) -> Dict:
        question_lower = question.lower()
        
        analysis = {
            'diseases': [],
            'locations': [],
            'measures': [],
            'time_period': None,
            'time_range': None,
            'gender': None,
            'age_group': None,
            'query_type': 'general',
            'grouping': [],
            'aggregation': None,
            'visualization': 'table'
        }
        
        for key, value in self.diseases.items():
            if key in question_lower:
                if value not in analysis['diseases']:
                    analysis['diseases'].append(value)
        
        for key, value in self.locations.items():
            if key in question_lower:
                if value not in analysis['locations']:
                    analysis['locations'].append(value)
        
        for key, value in self.measures.items():
            if key in question_lower:
                if value not in analysis['measures']:
                    analysis['measures'].append(value)
        
        if any(word in question_lower for word in ['women', 'female', 'жени', 'females']):
            analysis['gender'] = 'Female'
        elif any(word in question_lower for word in ['men', 'male', 'мажи', 'males']):
            analysis['gender'] = 'Male'
        
        years = re.findall(r'\b(?:19|20)\d{2}\b', question)
        if len(years) >= 2:
            analysis['time_range'] = [min(years), max(years)]
            analysis['query_type'] = 'trend'
            analysis['visualization'] = 'line'
        elif len(years) == 1:
            analysis['time_period'] = years[0]
        
        if any(phrase in question_lower for phrase in ['by sex', 'by gender', 'grouped by sex', 'по пол']):
            analysis['grouping'].append('sex')
        if any(phrase in question_lower for phrase in ['by location', 'by country', 'by region', 'по локација']):
            analysis['grouping'].append('location')
        if any(phrase in question_lower for phrase in ['by age', 'age groups', 'по возраст']):
            analysis['grouping'].append('age')
        if any(phrase in question_lower for phrase in ['over time', 'by year', 'trend', 'низ години']):
            analysis['grouping'].append('year')
            analysis['query_type'] = 'trend'
            analysis['visualization'] = 'line'
        
        if any(word in question_lower for word in ['total', 'sum', 'вкупно']):
            analysis['aggregation'] = 'sum'
            analysis['query_type'] = 'total'
            if not analysis['grouping']:
                analysis['visualization'] = 'metric'
        elif any(word in question_lower for word in ['average', 'mean', 'просек']):
            analysis['aggregation'] = 'avg'
            analysis['query_type'] = 'average'
            analysis['visualization'] = 'metric'
        
        if any(word in question_lower for word in ['compare', 'comparison', 'vs', 'versus', 'спореди']):
            analysis['query_type'] = 'comparison'
            analysis['visualization'] = 'bar'
        elif any(word in question_lower for word in ['top', 'highest', 'most', 'најмногу', 'највисоки', 'ranking']):
            analysis['query_type'] = 'ranking'
            analysis['visualization'] = 'bar'
        elif any(word in question_lower for word in ['distribution', 'breakdown', 'дистрибуција']):
            analysis['visualization'] = 'pie'
        elif any(word in question_lower for word in ['map', 'geographic', 'geography', 'карта']):
            analysis['visualization'] = 'map'
        
        if 'sex' in analysis['grouping'] and len(analysis['grouping']) == 1:
            analysis['visualization'] = 'pie'
        elif 'location' in analysis['grouping'] and analysis['query_type'] != 'trend':
            analysis['visualization'] = 'bar'
        
        return analysis

# ----------------- SPARQL Generator -----------------
class SPARQLGenerator:
    def __init__(self):
        self.prefixes = """
PREFIX dis: <http://diseases.org/disease-kg/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

    def generate_query(self, analysis: Dict) -> Tuple[str, str]:
        if analysis.get("aggregation") in ["sum", "avg"]:
            group_vars = []
            select_vars = []

            if "sex" in analysis.get("grouping", []):
                group_vars.append("?sex")
                select_vars.append("?sex")
            if "location" in analysis.get("grouping", []):
                group_vars.append("?location")
                select_vars.append("?location")
            if "year" in analysis.get("grouping", []):
                group_vars.append("?year")
                select_vars.append("?year")

            # SELECT clause
            agg_func = "SUM" if analysis["aggregation"] == "sum" else "AVG"
            select_clause = "SELECT " + " ".join(select_vars) + f" ({agg_func}(?value) AS ?value)"

            # WHERE clause
            where_clause = "WHERE {\n"
            where_clause += "    ?record a dis:HealthRecord ;\n"
            where_clause += "            dis:value ?value ;\n"
            where_clause += "            dis:causeName ?causeName ;\n"
            where_clause += "            dis:location ?location ;\n"
            where_clause += "            dis:measure ?measure .\n"
            where_clause += "    OPTIONAL { ?record dis:year ?year }\n"
            where_clause += "    OPTIONAL { ?record dis:sex ?sex }\n"

            # Build FILTER dynamically
            filters = []
            if analysis.get("diseases"):
                filters.append(f'?causeName = "{analysis["diseases"][0]}"')
            if analysis.get("measures"):
                filters.append(f'?measure = "{analysis["measures"][0]}"')
            if analysis.get("locations") and "location" not in analysis.get("grouping", []):
                filters.append(f'?location = "{analysis["locations"][0]}"')
            if analysis.get("time_period"):
                filters.append(f'?year = "{analysis["time_period"]}"^^xsd:gYear')
            if filters:
                where_clause += "    FILTER(" + " && ".join(filters) + ")\n"

            where_clause += "}"

            # GROUP BY
            group_by_clause = ""
            if group_vars:
                group_by_clause = "GROUP BY " + " ".join(group_vars)

            sparql_query = f"{self.prefixes.strip()}\n{select_clause}\n{where_clause}\n{group_by_clause}\nLIMIT 20"
            return sparql_query, "bar" if group_vars else "metric"

        # --- Default query (no aggregation) ---
        else:
            select_clause = "SELECT ?value ?causeName ?location ?measure ?year ?sex"
            where_clause = "WHERE {\n"
            where_clause += "    ?record a dis:HealthRecord ;\n"
            where_clause += "            dis:value ?value ;\n"
            where_clause += "            dis:causeName ?causeName ;\n"
            where_clause += "            dis:location ?location ;\n"
            where_clause += "            dis:measure ?measure .\n"
            where_clause += "    OPTIONAL { ?record dis:year ?year }\n"
            where_clause += "    OPTIONAL { ?record dis:sex ?sex }\n"

            filters = []
            if analysis.get("diseases"):
                filters.append(f'?causeName = "{analysis["diseases"][0]}"')
            if analysis.get("measures"):
                filters.append(f'?measure = "{analysis["measures"][0]}"')
            if analysis.get("locations"):
                filters.append(f'?location = "{analysis["locations"][0]}"')
            if analysis.get("time_period"):
                filters.append(f'?year = "{analysis["time_period"]}"^^xsd:gYear')
            if filters:
                where_clause += "    FILTER(" + " && ".join(filters) + ")\n"

            where_clause += "}"

            sparql_query = f"{self.prefixes.strip()}\n{select_clause}\n{where_clause}\nLIMIT 20"
            return sparql_query, analysis.get("visualization", "table")


# ----------------- Execute Query -----------------
def execute_sparql_query(query: str) -> Dict:
    try:
        headers = {"Accept": "application/sparql-results+json"}
        response = requests.post(GRAPHDB_URL, data={"query": query}, headers=headers, timeout=120)
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            return {"error": f"GraphDB HTTP {response.status_code}", "text": response.text, "query": query}
    except requests.exceptions.RequestException as e:
        return {"error": f"GraphDB connection failed: {e}", "query": query}
    

def call_hf_model(question: str) -> str:
    try:
        payload = {
        "inputs": question,
        "parameters": {"max_new_tokens": 200}
        }
        API_URL = f"https://api-inference.huggingface.co/models/{HF_MODEL}"
        headers = {"Authorization": f"Bearer {HF_API_KEY}"}
        response = requests.post(API_URL, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list) and "generated_text" in data[0]:
            return data[0]["generated_text"]
        else:
            return str(data)
    except Exception as e:
        return f"Error calling Hugging Face API: {e}"

# ----------------- API Endpoints -----------------
@app.post("/ask")
def ask_llm(req: AskRequest):
    analyzer = QueryAnalysis()
    generator = SPARQLGenerator()

    analysis = analyzer.analyze_question(req.question)
    
    sparql_query, visualization = generator.generate_query(analysis)
    
    result = execute_sparql_query(sparql_query)
    
    model_feedback = call_hf_model(req.question)

    return {
        "sparql": sparql_query,
        "result": result,
        "visualization": visualization,
        "analysis": analysis,
        "model_feedback": model_feedback
    }

@app.get("/health")
def health_check():
    return {"status": "OK", "version": "2.1"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True) 