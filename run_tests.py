import os
import importlib.util
import json


def fake_model(User_Input,System_Context,Model_Configs,Output_File_Location):
    print("this is output")


#this function needs to go into each test file and then run the test within it
def run_test(test_path):
    #collect all tests
    tests = os.listdir(test_path)
    #create full paths for tests
    complete_paths = [test_path + "/" + item for item in tests]

    #now using the paths we get information from the files

    for test in complete_paths:
        namespace = {}

        input_file = "/Model_Input.py"

        with open(test+input_file, 'r') as file:
            file_content = file.read()

        # Execute the file content in the created namespace
        jsonified = json.dumps(file_content)
        print(jsonified)

        print(jsonified["User_Input"])
        #open the input file and draw out variables
        







if __name__ == "__main__":
    run_test(test_path = "./Tests")