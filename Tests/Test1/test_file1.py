# src/Tests/Test1_Subfolder/test_file1.py

# Import from the Model directory
from Model.Run_Model import run_model

# Import from the Functions directory
from Functions.utility import helper_function

# Import from external model (if necessary)


def test_some_function():
    assert run_model() == "AI model prediction"

