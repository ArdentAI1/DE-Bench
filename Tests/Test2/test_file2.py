# src/Tests/Test2_Another_Subfolder/test_file2.py

# Import from the Model directory
from Model.some_module import run_model

# Import from the Functions directory
from Functions.utility import helper_function



def test_different_scenario():
    assert run_model() == "AI model prediction"
