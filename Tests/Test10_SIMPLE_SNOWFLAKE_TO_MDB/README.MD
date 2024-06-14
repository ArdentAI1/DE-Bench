## Exercise #10 - Moving data from Snowflake to MDB

In this task we need to move data from the Tasty Bytes sample data in snowflake and move it to the ChataDataGen
database in mongodb in Testing_ETL_SCRIPTS. We also want to calculate profit per item from the menu
so that we understand how much we make per item on the menu

#### Setup
Environment for code/scripts is Test Environment
#### Problems Statement
You need to move user data from Snowflake to MDB.

1. Understand the structure of the input data
2. Map that data properly to the target based on user instructions
3. Request information from the user when needed and take optimal actions with minimal errors


#### Download URIs are listed in the `main.py` file.

### Hints
1. Need to automatically get database structure quickly. Probably build tooling to automatically do this
2. Model should write the correct transformations to do this. This could be a point of error
3. The model needs to write the files and run them. It does not have any files in the environment already