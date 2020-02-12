# Amplitude to BigQuery

## TL/DR
In order to run the import just run `python extract_data_import.py`

### Summary
Script for extracting data from Amplitude and uploading it to Google BigQuery. Make usage of 
[Amplitude export API](https://help.amplitude.com/hc/en-us/articles/205406637-Export-API-Export-Your-Project-s-Event-Data).


#### Dependencies
To install all required packages 
`pip install -r requirements.txt`

### Requirements

* Python 3.6+
* JSON key for using BigQuery as a service account (should be placed inside config folder)
* Amplitude keys for the projects you want to extract
* dataset in BigQuery (needs to be created beforehand) where you want to store the tables
* Ensure to create the below configuration files inside the config folder

#### Configuration files
Two configuration files are required:

`globals.py`, this should contain:
`ENVIRONMENT = 'environment_name'`

`environment_name.py` (this file name should be the same one is assigned in ENVIRONMENT variable in globals.py)
which inside should contain the following keys:
* GCLOUD_JSON_KEY 
(Path to Gcloud json key)
* GCLOUD_PROJECT_ID
* AMPLITUDE_KEYS
* DATASET_AMPLITUDE
(Dataset where to store Amplitude data)
* START_DATE_EXTRACT
(First date to extract data from, with a format %Y-%m-%d)
* END_DATE_EXTRACT
(Last date to extract data from, with a format %Y-%m-%d)


Inside AMPLITUDE_KEYS you need to define a dictionary, with the key 
being the name of each platform (will be used to naming tables in BigQuery) and then PROP_KEY
and PROP_VALUE, for example:
```
AMPLITUDE_KEYS = {'ANDROID': {'PROP_VALUE' : 'value',
                            'PROP_KEY' : 'key'}}
```
#### Example configuration files
An example of globals and config file is provided in config folder, when creating your own remember to remove `_template`
from globals file name.

### How to run it?
In order to run the import just run `python extract_data_import.py`
