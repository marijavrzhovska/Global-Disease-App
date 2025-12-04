# Lung cancer death rates - Data package

This data package contains the data that powers the chart ["Lung cancer death rates"](https://ourworldindata.org/grapher/lung-cancer-deaths-per-100000-by-sex-1950-2002?v=1&csvType=full&useColumnShortNames=false) on the Our World in Data website.

## CSV Structure

The high level structure of the CSV file is that each row is an observation for an entity (usually a country or region) and a timepoint (usually a year).

The first two columns in the CSV file are "Entity" and "Code". "Entity" is the name of the entity (e.g. "United States"). "Code" is the OWID internal entity code that we use if the entity is a country or region. For normal countries, this is the same as the [iso alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) code of the entity (e.g. "USA") - for non-standard countries like historical countries these are custom codes.

The third column is either "Year" or "Day". If the data is annual, this is "Year" and contains only the year as an integer. If the column is "Day", the column contains a date string in the form "YYYY-MM-DD".

The remaining columns are the data columns, each of which is a time series. If the CSV data is downloaded using the "full data" option, then each column corresponds to one time series below. If the CSV data is downloaded using the "only selected data visible in the chart" option then the data columns are transformed depending on the chart type and thus the association with the time series might not be as straightforward.

## Metadata.json structure

The .metadata.json file contains metadata about the data package. The "charts" key contains information to recreate the chart, like the title, subtitle etc.. The "columns" key contains information about each of the columns in the csv, like the unit, timespan covered, citation for the data etc..

## About the data

Our World in Data is almost never the original producer of the data - almost all of the data we use has been compiled by others. If you want to re-use data, it is your responsibility to ensure that you adhere to the sources' license and to credit them correctly. Please note that a single time series may have more than one source - e.g. when we stich together data from different time periods by different producers or when we calculate per capita metrics using population data from a second source.

### How we process data at Our World In Data
All data and visualizations on Our World in Data rely on data sourced from one or several original data providers. Preparing this original data involves several processing steps. Depending on the data, this can include standardizing country names and world region definitions, converting units, calculating derived indicators such as per capita measures, as well as adding or adapting metadata such as the name or the description given to an indicator.
[Read about our data pipeline](https://docs.owid.io/projects/etl/)

## Detailed information about each time series


## Women
Reported deaths from trachea, bronchus, lung cancers in females in those aged all ages per 100,000 people.
Last updated: August 5, 2025  
Next update: August 2026  
Date range: 1950–2023  
Unit: deaths per 100,000 people  


### How to cite this data

#### In-line citation
If you have limited space (e.g. in data visualizations), you can use this abbreviated in-line citation:  
WHO Mortality Database (2025) – with minor processing by Our World in Data

#### Full citation
WHO Mortality Database (2025) – with minor processing by Our World in Data. “Women” [dataset]. WHO Mortality Database, “WHO Mortality Database” [original data].
Source: WHO Mortality Database (2025) – with minor processing by Our World In Data

### What you should know about this data
* The International Classification of Diseases (Version 10) codes that define trachea, bronchus, lung cancers are C33-C34.

### Source

#### WHO Mortality Database
Retrieved on: 2025-08-05  
Retrieved from: https://platform.who.int/mortality  


## Men
Reported deaths from trachea, bronchus, lung cancers in males in those aged all ages per 100,000 people.
Last updated: August 5, 2025  
Next update: August 2026  
Date range: 1950–2023  
Unit: deaths per 100,000 people  


### How to cite this data

#### In-line citation
If you have limited space (e.g. in data visualizations), you can use this abbreviated in-line citation:  
WHO Mortality Database (2025) – with minor processing by Our World in Data

#### Full citation
WHO Mortality Database (2025) – with minor processing by Our World in Data. “Men” [dataset]. WHO Mortality Database, “WHO Mortality Database” [original data].
Source: WHO Mortality Database (2025) – with minor processing by Our World In Data

### What you should know about this data
* The International Classification of Diseases (Version 10) codes that define trachea, bronchus, lung cancers are C33-C34.

### Source

#### WHO Mortality Database
Retrieved on: 2025-08-05  
Retrieved from: https://platform.who.int/mortality  


    