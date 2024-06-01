User_Input = """
There is a folder called `data` in this current directory, `Exercises/Exercise-5`. There are also
3 `csv` files located in that folder. Open each one and examine it, the 
first task is to create a `sql` script with the `DDL` to hold
a `CREATE` statement for each data file. Remember to think about data types. 
Also, this `CREATE` statements should include indexes for each table, as well
as primary and foreign keys.

After you have finished this `sql` scripts, we must connect to `Postgres` using the `Python` package
called `psycopg2`. Once connected we will run our `sql` scripts against the database.

Note: The default `main.py` script already has the Python connection configured to connect
to the `Postgres` instance that is automatically spun up by `Docker` when you ran
the `docker-compose up run` command (inside `Exercises/Exercise-5` directory).

Finally, we will use `psycopg2` to insert the data in each `csv` file into the table you created.

Generally, your script should do the following ...
1. Examine each `csv` file in `data` folder. Design a `CREATE` statement for each file.
2. Ensure you have indexes, primary and forgein keys.
3. Use `psycopg2` to connect to `Postgres` on `localhost` and the default `port`.
4. Create the tables against the database.
5. Ingest the `csv` files into the tables you created, also using `psycopg2`.

"""

Model_Configs = ""
Output_File_Location = """"""


