# Overview
Python code for generating the csv files containing the Kauffman Foundation's Early Stage Entrepreneurship (KESE) indicators data (see https://indicators.kauffman.org/series/earlystage). 


# Requirements
This repository provides two options for obtaining the raw data needed to create the indicators: Fetching the data from source, or using the pre-fetched data housed in `data/raw_data`. This option may be specified using an argument within `mpj_command` (see more on that below). If you choose to fetch raw data from source rather than use the provided data, you will need to do the following:
1. Install the kauffman library. See the installation instructions at https://github.com/EMKF/downwardata.
2. Create an environmental variable on your computer called "CENSUS_KEY" where the value is your census API key. If you do not have one, you can submit a request at the following URL: https://api.census.gov/data/key_signup.html.

[/If or when we get the kauffman library up on pip, we will need to remove language about downwardata reliance/]: #


# Files
The repository has two subdirectories:
### 1. `data`
This directory contains a raw_data folder with source data (eight csv files and a `pkl` file of a string of a timestamp when the data was
pulled), output (six csv files available after running `kese_command.py`), and a `TEMP` directory with data files created when `kese_command.py` is run. 

### 2. `tools` 
Within this directory there are three files:
1.  `kese_command.py`: Running this file will generate the data using the function `kese_data_create_all`, which has three parameters:
    * `raw_data_fetch`, which allows the user to specify whether to fetch the raw data from source (see below) or use the data in `data/raw_data`.
    * `raw_data_remove`, which allows the user to specify whether to remove the temporary data files.
    * `aws_filepath`, which allows the user to specify whether to stash the data in S3.   

    The output consists of six csv files: one formatted the same as the file available for download on the webpage (see https://indicators.kauffman.org/wp-content/uploads/sites/2/2021/03/Kauffman_Indicators_Early-Stage_Entrepreneurship_Data_2020_v2.csv.), and five (one for each indicator) that are used to create the visualizations on the webpage. The data consists of annual values for each indicator by state (including Washington DC) and U.S. The U.S. level data is further subset by type (ex: age, race, sex, etc.) and category (for type = age, categories include: 'Ages 20-34', 'Ages 35-44', etc.)

2. `kese_raw_data_fetch.py`: This file generates the data in the directory `data > raw_data`. It is used to update the data for the yearly KESE indicators update.
    * `raw_data_update()`, generates the US- and state-level datafiles, formated as csv, and the data timestamp.
    * `s3_update()`, stashes the csvs and timestamp in S3.

3. `constants.py`: A file with constant values used in `kese_command.py` and `kese_raw_data_fetch.py` 


# Feedback
Questions or comments can be directed to indicators@kauffman.org.


# Disclaimer
The content presented in this repository is presented as a courtesy to be used only for informational purposes. The 
content is provided “As Is” and is not represented to be error free. The Kauffman Foundation makes no representation or 
warranty of any kind with respect to the content and any such representations or warranties are hereby expressly 
disclaimed.