import sys
import os
from dotenv import load_dotenv

def pytest_configure(config):
    print("Configuring pytest...")
    load_dotenv()  # Load environment variables from the .env file

    # Get and set the test root directory
    test_root = os.getenv('BENCHMARK_ROOT')
    if test_root:
        test_root = os.path.abspath(test_root)
        if test_root not in sys.path:
            sys.path.insert(0, test_root)

    # Get and set the model path directory
    model_path = os.getenv('MODEL_PATH')
    if model_path:
        model_path = os.path.abspath(model_path)
        if model_path not in sys.path:
            sys.path.insert(0, model_path)