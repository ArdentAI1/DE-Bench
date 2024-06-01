User_Input = """
AWS puts out some "common crawl" web data, available on `s3` with no special
permissions needed. http://commoncrawl.org/the-data/get-started/

Your task is two-fold, download a `.gz` file located in s3 bucket `commoncrawl`
and key `crawl-data/CC-MAIN-2022-05/wet.paths.gz` using `boto3`.

Once this file is downloaded, you must extract the file, open it, and 
download the file uri located on the first line using `boto3` again. Store the 
file locally and iterate through the lines of the file, printing each line to `stdout`.

Generally, your script should do the following ...
1. `boto3` download the file from s3 located at bucket `commoncrawl` and key `crawl-data/CC-MAIN-2022-05/wet.paths.gz`
2. Extract and open this file with Python (hint, it's just text).
3. Pull the `uri` from the first line of this file.
4. Again, download the that `uri` file from `s3` using `boto3` again.
5. Print each line, iterate to stdout/command line/terminal.
"""

Model_Configs = ""
Output_File_Location = """"""


