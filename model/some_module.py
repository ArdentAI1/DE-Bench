
#configure this file to run your model
import os
import sys

#import your AI model into this file
from ai_model import predict 


def run_model():
    #a wrapper for you to drop your model in

    answer = predict()
    
    return answer
    
    





