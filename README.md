# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo into wherever you want. Ideally a tests folder
2. Set Environment variables
  a. Set BENCHMARK_ROOT to the full path of the folder you clone the repo into
  b. Set MODEL_PATH to the path to your model

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model

4. Use pytest to run. Pytest -m "TESTNAME" where TESTNAME is the name of the test to run a specific test

5. A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the neccesary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind







