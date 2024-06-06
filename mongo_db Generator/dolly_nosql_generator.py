import torch
from transformers import pipeline


import pandas as pd
import time as t
from openai import AzureOpenAI
from pymongo import MongoClient

 
generate_text = pipeline(model="databricks/dolly-v2-12b", torch_dtype=torch.bfloat16,
                         trust_remote_code=True, device_map="auto", return_full_text=True)


from langchain import PromptTemplate, LLMChain
from langchain.llms import HuggingFacePipeline

# template for an instrution with no input
# prompt = PromptTemplate(
#     input_variables=["instruction"],
#     template="{instruction}")

# template for an instruction with input
prompt_with_context = PromptTemplate(
    input_variables=["instruction", "context"],
    template="{instruction}\n\nInput:\n{context}")

hf_pipeline = HuggingFacePipeline(pipeline=generate_text)

# llm_chain = LLMChain(llm=hf_pipeline, prompt=prompt)
llm_context_chain = LLMChain(llm=hf_pipeline, prompt=prompt_with_context)


# context = """George Washington (February 22, 1732[b] - December 14, 1799) was an American military officer, statesman,
# and Founding Father who served as the first president of the United States from 1789 to 1797."""

# print(llm_context_chain.predict(instruction="When was George Washington president?", context=context).lstrip())




def extract_generic_query(query):
    print("the Query is:", query)
    index = query.find("users.find")
    if index != -1:
        return query[index:]
    else:
        print("Not Getting Correct Query Generated !")
        return "None"
    



import re

def extract_mongodb_queries(query_string):
    # Using regular expression to find all MongoDB queries
    matches = re.findall(r'\b(transactions\.find|cards\.find|users\.find|cards\.aggregate|transactions\.aggregate|users\.aggregate)\s*\((.*?)\)', query_string, re.DOTALL)
    return matches if matches else None

# Example usage

def mongodb_query_generator( question):

    context=""

    response = llm_context_chain.predict(instruction=question, context=context).lstrip()
    mongodb_query = response.choices[0].text.strip()
    #extract_generic_query(mongodb_query)
    return mongodb_query


# connectionString = "mongodb+srv://genai-user:Figital123@genai.4acr5.mongodb.net/?retryWrites=true&w=majority&appName=GenAI"

# def connect_mongoDB():
#     client = MongoClient(connectionString)
#     return client



# df = pd.read_csv('txns 3.csv')
# #question=input("enter the Question to generate Query: ")

# client = connect_mongoDB()
# print("client", client)
# detaxCoreDB = client['detax-core']
# ncaishAccountDB = client['ncaish-accounts']
# users = ncaishAccountDB["users"]
# transactions = detaxCoreDB["card_transactions"]
# cards = detaxCoreDB["detax_cards"]
# claims = detaxCoreDB["detax_claims"]
# monthlyFba = detaxCoreDB["fba_monthly_analytics"]

# '''question1="what is the breakup of the salary of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
# question="what is the fba allowance of the uid='95741f52-7d37-4176-947a-3def3c34396c'?"  
# question2="list the rejected transactions for the uid='95741f52-7d37-4176-947a-3def3c34396c'"
# question3="Give me all my reimbursed claims of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
# question4="What is FBA Balance of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
# question5="Can you tell me the amount spent on Food till now"
# '''



# # Define a context dictionary to provide necessary variables
# context = {
#     'transactions': transactions,
#     'cards': cards,
#     'claims': claims,
#     'monthlyFba': monthlyFba,
#     'users': users
# }

# # Execute the MongoDB query dynamically
# query_string = ''' Enter your Query
# Question: What is the total amount debited so far?
#                     transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "DEBIT"})
#                                '''


import ast
while True:
    question=input("Enter your Query: ")
    question+="of the 'uid'='95741f52-7d37-4176-947a-3def3c34396c'"
    mongodb_query=mongodb_query_generator(question)
    print("LLM Generated Query: ",mongodb_query)
    tuned_query = extract_mongodb_queries(mongodb_query)

    if tuned_query:
        print("Extracted MongoDB Tuned queries:")
        for query_name, query_content in tuned_query:
            mongodb_query_tuned=query_name + "(" + query_content.strip() + ")"
            print(mongodb_query_tuned)
            #t.sleep(5)
            break

    else:
        print("No MongoDB queries found.")
        