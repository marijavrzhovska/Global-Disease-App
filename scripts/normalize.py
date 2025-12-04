import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
import requests  

DIS = Namespace("http://diseases.org/disease-kg/")
SNOMED = Namespace("http://snomed.info/id/")

CAUSE_TO_SNOMED = {
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
    "Malaria": "1386000"
}

def convert_ihme_dataset(file_path, g: Graph) -> Graph:
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        record_uri = DIS[f"record/{row['location_id']}/{row['year']}/{row['cause_id']}/{row['measure_id']}/{row['sex_id']}/{row['age_id']}"]

        g.add((record_uri, DIS.measureId, Literal(int(row["measure_id"]), datatype=XSD.integer)))
        g.add((record_uri, DIS.metricId, Literal(int(row["metric_id"]), datatype=XSD.integer)))
        g.add((record_uri, DIS.sexId, Literal(int(row["sex_id"]), datatype=XSD.integer)))
        g.add((record_uri, DIS.ageId, Literal(int(row["age_id"]), datatype=XSD.integer)))

        g.add((record_uri, RDF.type, DIS.HealthRecord))
        g.add((record_uri, DIS.location, Literal(row["location_name"], datatype=XSD.string)))
        g.add((record_uri, DIS.year, Literal(int(row["year"]), datatype=XSD.gYear)))
        g.add((record_uri, DIS.sex, Literal(row["sex_name"], datatype=XSD.string)))
        g.add((record_uri, DIS.age, Literal(row["age_name"], datatype=XSD.string)))
        g.add((record_uri, DIS.metric, Literal(row["metric_name"], datatype=XSD.string)))
        g.add((record_uri, DIS.value, Literal(float(row["val"]), datatype=XSD.float)))
        g.add((record_uri, DIS.measure, Literal(row["measure_name"], datatype=XSD.string)))
        g.add((record_uri, DIS.measureDescription, Literal(f"This record represents {row['measure_name']} measured in {row['metric_name']}", datatype=XSD.string)))
        g.add((record_uri, DIS.causeName, Literal(row["cause_name"], datatype=XSD.string)))

        cause_name = str(row["cause_name"]).strip()
        if cause_name in CAUSE_TO_SNOMED:
            g.add((record_uri, DIS.cause, SNOMED[CAUSE_TO_SNOMED[cause_name]]))

    return g

def upload_to_graphdb(graph: Graph, endpoint: str):
    """ Upload RDF graph directly as Turtle to GraphDB """
    ttl_data = graph.serialize(format="turtle")
    headers = {"Content-Type": "text/turtle"}
    r = requests.post(endpoint, data=ttl_data, headers=headers)

    if r.status_code in [200, 204]:
        print("Data successfully uploaded to GraphDB")
    else:
        print(f"Upload failed: {r.status_code} - {r.text}")

if __name__ == "__main__":

    datasets = [
        r"data\IHME-GBD_2021_DATA-4b50b8a1-1-TUBERCULOSIS\IHME-GBD_2021_DATA-4b50b8a1-1.csv",#tuberkuloza OK
        r"data\IHME-GBD_2021_DATA-9c6c333b-1-BREASTCANCER\IHME-GBD_2021_DATA-9c6c333b-1.csv",#breast cancer OK
        r"data\IHME-GBD_2021_DATA-09fdc579-1-HIVSIDA\IHME-GBD_2021_DATA-09fdc579-1.csv",#hiv OK
        r"data\IHME-GBD_2021_DATA-64b6ff04-1-DIABETESTYPE2\IHME-GBD_2021_DATA-64b6ff04-1.csv",#diabetes type 2 OK
        r"data\IHME-GBD_2021_DATA-99547af3-1-PROSTATECANCER\IHME-GBD_2021_DATA-99547af3-1.csv",#prostate cancer OK
        r"data\IHME-GBD_2021_DATA-b076edd1-1-STOMACHCANCER\IHME-GBD_2021_DATA-b076edd1-1.csv",#stomach cancer OK
        r"data\IHME-GBD_2021_DATA-b3342b47-1-LEUKEMIA\IHME-GBD_2021_DATA-b3342b47-1.csv",#leukemia OK
        r"data\IHME-GBD_2021_DATA-be6216e9-1-STROKE\IHME-GBD_2021_DATA-be6216e9-1.csv",#stroke OK
        r"data\IHME-GBD_2021_DATA-d484bcec-1-BIP-SHIZ-ANRX-BULIM\IHME-GBD_2021_DATA-d484bcec-1.csv",#bipolar,anorexia,shizof,bulim OK
        r"data\IHME-GBD_2021_DATA-d28911d8-1-COVID\IHME-GBD_2021_DATA-d28911d8-1.csv"#covid OK 
    ]

    for dataset in datasets:
        g = Graph()
        g.bind("dis", DIS)
        g.bind("snomed", SNOMED)
        g = convert_ihme_dataset(dataset, g)

        upload_to_graphdb(g, "http://localhost:7200/repositories/disease-kg/statements")

print("Uploaded")