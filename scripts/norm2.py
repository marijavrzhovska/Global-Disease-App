import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
import requests

DIS = Namespace("http://diseases.org/disease-kg/")
SNOMED = Namespace("http://snomed.info/id/")

CAUSE_TO_SNOMED = {
    "Malaria": "1386000",
    "Lung Cancer": "254637007"  #(trachea, bronchus, lung)
}

def convert_malaria_dataset(file_path, g: Graph, dataset_link: str) -> Graph:
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        record_uri = DIS[f"record/{row['Code']}/{row['Year']}/Malaria/all/{dataset_link}"]
        g.add((record_uri, RDF.type, DIS.HealthRecord))
        g.add((record_uri, DIS.location, Literal(row["Entity"], datatype=XSD.string)))
        g.add((record_uri, DIS.year, Literal(int(row["Year"]), datatype=XSD.gYear)))
        g.add((record_uri, DIS.measureDescription, Literal("Number of deaths due to Malaria", datatype=XSD.string)))
        g.add((record_uri, DIS.value, Literal(float(row["malaria_deaths"]), datatype=XSD.float)))
        g.add((record_uri, DIS.causeName, Literal("Malaria", datatype=XSD.string)))
        g.add((record_uri, DIS.datasetLink, Literal(dataset_link, datatype=XSD.string)))
        g.add((record_uri, DIS.cause, SNOMED[CAUSE_TO_SNOMED["Malaria"]]))
    return g

def convert_lung_cancer_dataset(file_path, g: Graph, dataset_link: str) -> Graph:
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        
        record_uri_f = DIS[f"record/{row['Code']}/{row['Year']}/LungCancer/female/{dataset_link}"]
        g.add((record_uri_f, RDF.type, DIS.HealthRecord))
        g.add((record_uri_f, DIS.location, Literal(row["Entity"], datatype=XSD.string)))
        g.add((record_uri_f, DIS.year, Literal(int(row["Year"]), datatype=XSD.gYear)))
        g.add((record_uri_f, DIS.measureDescription, Literal("Age-standardized deaths per 100,000 females", datatype=XSD.string)))
        g.add((record_uri_f, DIS.value, Literal(float(row["Age-standardized deaths from trachea, bronchus, lung cancers in females in those aged all ages per 100,000 people"]), datatype=XSD.float)))
        g.add((record_uri_f, DIS.causeName, Literal("Lung Cancer", datatype=XSD.string)))
        g.add((record_uri_f, DIS.sex, Literal("female", datatype=XSD.string)))
        g.add((record_uri_f, DIS.datasetLink, Literal(dataset_link, datatype=XSD.string)))
        g.add((record_uri_f, DIS.cause, SNOMED[CAUSE_TO_SNOMED["Lung Cancer"]]))

        record_uri_m = DIS[f"record/{row['Code']}/{row['Year']}/LungCancer/male/{dataset_link}"]
        g.add((record_uri_m, RDF.type, DIS.HealthRecord))
        g.add((record_uri_m, DIS.location, Literal(row["Entity"], datatype=XSD.string)))
        g.add((record_uri_m, DIS.year, Literal(int(row["Year"]), datatype=XSD.gYear)))
        g.add((record_uri_m, DIS.measureDescription, Literal("Age-standardized deaths per 100,000 males", datatype=XSD.string)))
        g.add((record_uri_m, DIS.value, Literal(float(row["Age-standardized deaths from trachea, bronchus, lung cancers in males in those aged all ages per 100,000 people"]), datatype=XSD.float)))
        g.add((record_uri_m, DIS.causeName, Literal("Lung Cancer", datatype=XSD.string)))
        g.add((record_uri_m, DIS.sex, Literal("male", datatype=XSD.string)))
        g.add((record_uri_m, DIS.datasetLink, Literal(dataset_link, datatype=XSD.string)))
        g.add((record_uri_m, DIS.cause, SNOMED[CAUSE_TO_SNOMED["Lung Cancer"]]))

    return g

def upload_to_graphdb(graph: Graph, endpoint: str):
    ttl_data = graph.serialize(format="turtle")
    headers = {"Content-Type": "text/turtle"}
    r = requests.post(endpoint, data=ttl_data, headers=headers)
    if r.status_code in [200, 204]:
        print("Data successfully uploaded to GraphDB")
    else:
        print(f"Upload failed: {r.status_code} - {r.text}")

if __name__ == "__main__":
    g = Graph()
    g.bind("dis", DIS)
    g.bind("snomed", SNOMED)

    malaria_csv = r"data\global-malaria-deaths-by-world-region(ourWorldInData)\global-malaria-deaths-by-world-region.csv"
    malaria_link = "https://diseases.org/malaria_data_source"
    g = convert_malaria_dataset(malaria_csv, g, malaria_link)
    print("added to graph")
    lung_csv = r"data\lung-cancer-deaths-per-100000-by-sex-1950-2002(ourWorldInData)\lung-cancer-deaths-per-100000-by-sex-1950-2002.csv"
    lung_link = "https://diseases.org/lung_cancer_data_source"
    g = convert_lung_cancer_dataset(lung_csv, g, lung_link)
    print("added to graph")
    upload_to_graphdb(g, "http://localhost:7200/repositories/disease-kg/statements")

print("Uploaded")