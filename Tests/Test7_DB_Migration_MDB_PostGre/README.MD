## Exercise #7 - Migrating a MongoDB database to PostGRESQL

This is a real task provided by our design partner Chetan. 
The goal is to move 10k+ of user data from mongodb to PostGRESQL.
Users are currently complaining about high latency on the app because of the massive nested objects in MDB.
PostGRESQL is considered to improve this.

#### Setup
Environment for code/scripts is Test Environment
#### Problems Statement
You need to move user data from MDB to postGRESQL.

1. Understand the structure of the input data
2. Map that data properly to the target based on user instructions
3. Request information from the user when needed and take optimal actions with minimal errors


#### Download URIs are listed in the `main.py` file.

### Hints
1. Need to automatically get database structure quickly. Probably build tooling to automatically do this
2. Model should write the correct transformations to do this. This could be a point of error
3. The model needs to write the files and run them. It does not have any files in the environment already