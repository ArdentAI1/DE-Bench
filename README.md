# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve

To see the results of our runs: https://ardentai.io/progress

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo
2. Drop your model into the model/your_model folder. Link this model to be used by run_model in model/model_store
3. Change the absolute path in the tests to your local
4. MAKE SURE YOU HAVE DOCKER DESKTOP RUNNING (I forget this all the time)
5. Run pytest -s to run the tests
6. To run specific tests do pytest -m "number" where number is the test you want to run.
  a. pytest -m "one" runs test 1   


