
#configure this file to run your model

model = ""


def run_model(user_input,code_environment_location,shell_access):
    #a wrapper for you to drop your model in
    print("----------user input----------")
    print(user_input)
    print("----------code_environment_location----------")
    print(code_environment_location)
    print("----------shell_access----------")
    print(shell_access)
    print("----------Executing in container----------")
    print(shell_access("echo hello world in container"))

    print("model running and doing stuff :)")
    
    





