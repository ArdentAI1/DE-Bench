import os
import importlib.util

def fake_model(User_Input, System_Context, Model_Configs, Output_File_Location):
    print(f"User Input: {User_Input}")
    print(f"System Context: {System_Context}")
    print(f"Model Configs: {Model_Configs}")
    print(f"Output File Location: {Output_File_Location}")

def run_test(test_path):
    # Collect all tests
    tests = os.listdir(test_path)
    # Create full paths for tests
    complete_paths = [os.path.join(test_path, item) for item in tests]

    # Now using the paths we get information from the files
    for test in complete_paths:
        input_file = os.path.join(test, "Model_Input.py")

        if os.path.isfile(input_file):
            namespace = {}

            # Create module specification for the input file
            spec = importlib.util.spec_from_file_location("Model_Input", input_file)
            # Load the module
            module = importlib.util.module_from_spec(spec)
            # Execute the module to populate its contents
            spec.loader.exec_module(module)

            # Access the variables
            user_input = module.User_Input
            system_context = module.System_Context
            model_configs = module.Model_Configs
            output_file_location = module.Output_File_Location

            # Now you can use these variables as needed
            fake_model(user_input, system_context, model_configs, output_file_location)

if __name__ == "__main__":
    run_test(test_path="./Tests")
