import os

User_Input = """
Fix my dag that is failing:
1. It should print "Hello World" to the logs
2. Runs daily at midnight
3. Has a single task named 'print_hello'
4. Create a new feature branch called 'fix/BRANCH_NAME'
5. Create a pull request named 'fix/PR_NAME'
"""

# Configuration will be generated dynamically by create_config function
