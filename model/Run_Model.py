
#configure this file to run your model
import os
import sys
from dotenv import load_dotenv

load_dotenv()


from Configs.ArdentConfig import Ardent_Client

#import your AI model into this file


def run_model(container,task,configs):
    #A Wrapper for your model to do things. 



    result = Ardent_Client.create_and_execute_job(
        message=task,
    )







    
    return result
    
    





