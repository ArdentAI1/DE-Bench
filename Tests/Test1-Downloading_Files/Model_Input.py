User_Input = """

You need to download 10 files that are sitting at the following specified
HTTP urls. You will use the Python package requests to do this
work.

You will need to pull the filename from the download uri.

The files are zip files that will also need to be unzipped into 
their csv format.

They should be downloaded into a folder called downloads which
does not exist currently inside the Exercise-1 folder. You should
use Python to create the directory, do not do it manually.

Generally, your script should do the following ...
1. create the directory downloads if it doesn't exist
2. download the files one by one.
3. split out the filename from the uri, so the file keeps its 
   original filename.
   
4. Each file is a zip, extract the csv from the zip and delete
the zip file.
5. For extra credit, download the files in an async manner using the 
   Python package aiohttp. Also try using ThreadPoolExecutor in 
   Python to download the files. Also write unit tests to improve your skills.

#### Download URIs are listed in the main.py file as follows:

import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # your code here
    pass


if __name__ == "__main__":
    main()


"""

System_Context = """"""
Model_Configs = """"""
Output_File_Location = """"""


