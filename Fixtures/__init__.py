# This file makes Fixtures a Python package 
def parse_test_name(test_name: str) -> str:
    """
    Parse the test name and remove pytest parametrization brackets

    :param str test_name: The name of the test
    :return: The parsed test name
    :rtype: str
    """
    return test_name[:test_name.find("[")].strip() if "[" in test_name else test_name.strip()