import pandas as pd
import time as t
from openai import AzureOpenAI
from pymongo import MongoClient
import streamlit as st
api_key_file = "api_key.txt"

# Read the API key from the file
with open(api_key_file, "r") as file:
    api_key2 = file.read().strip()

# Set up your AzureOpenAI client

client1 = AzureOpenAI(
    azure_endpoint="https://testing1234.openai.azure.com/",
    api_key=api_key2,
    api_version="2024-02-15-preview"
)


deployment_name='LLM_response'



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

ddl='''
You have a MongoDB collection named "transactions" with the following schema:

Attributes:
  - _id: ObjectId
  - uid: String
  - cid: String
  - cardId: String
  - amt: Double
  - type: String
  - source: String
  - txnWallet: String
  - remarks: String
  - txnStatus: String
  - billUploadedAt: Long
  - claimRaisedAt: Int
  - approvedAt: Long
  - batchId: String
  - partnerTxnId: String
  - claimRaised: Bool
  - recharge: Bool
  - objectId: String
  - createdBy: String
  - deletedAt: Int
  - updatedAt: Long
  - createdAt: Long
  - archive: Bool
  - _class: String
  - merchant: String
  - surCharge: Bool
  - appAmt: Double
  - approvalStatus: String
  - approvedBy: String
  - approverRemarks: String
  - baseAmt: Int
  - bills: Array of String
  - categoryId: String
  - cgst: Double
  - channel: String
  - claimId: String
  - creditRRNs: Array of String
  - day: String
  - declineReason: String
  - forceApproval: Bool
  - fuelSurchargeAmt: Double
  - gstin: String
  - institutionCode: String
  - kitNumber: String
  - lastCategoryId: String
  - linkedTxns: Bool
  - manualTransaction: Bool
  - manualTxnCreatedAt: Long
  - mcc: String
  - merchantId: String
  - month: String
  - network: String
  - partnerTxnType: String
  - rebateEligible: Bool
  - referenceAmt: Int
  - referenceDateTime: Long
  - referenceMerchantName: String
  - referenceRrn: String
  - referernceTxnId: String
  - reversedAt: Long
  - rrn: String
  - scope: String
  - sgst: Double
  - sysAssignedCategory: String
  - systemTraceAuditNumber: Int
  - taxRebateEarnedAmount: Double
  - taxRebateEarnedAmountForFY: Double
  - terminalID: String
  - traceNo: Int
  - userSelectedCategory: String
  - userSelectedCategoryApproved: String or Bool (mixed type array)
  - validationStatus: String
  This Schema is for collection name transactions
'''
def mongodb_query_generator(df, question):
    # Convert DataFrame to string

    Documentation_statement='''Here by I am providing some Basic Information:
    Our definition of financial year is that the financial year starts in April and concludes in March of the following year. For instance, if the current month is March 2024, the ongoing financial year spans from April 2023 to March 2024. If the current month is June 2024, the active financial year extends from April 2024 to March 2025.")
    Our definition of food is Food/Provisions")
    Our definition of fuel is Fuel & Travel")
    Our definition of available limit is availablelimit")
    Our definition of FBA categories is txnwallet='FBA'")
    Our definition of spending categories is txnwallet='FBA'"
    Our definition of expense wallet is txnwallet='TA'")
    Our definition of travel wallet is txnwallet='TA'")
    Our definition of so far is current financial year")
    "Our definition of till now is current financial year")
    Our definition of thus far is current financial year")
    "Our definition of pending bill upload is ApprovalStatus='BILL UPLOAD PENDING'")
    Our definition of bill upload pending is ApprovalStatus='BILL UPLOAD PENDING'")
    Our definition of FBA is Flexible Benefit Allowance")

    '''
    
    #Query:users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"}, {"_id":0, "salary": 1, "basicsalary": 1, "allowance": 1})
    refference='''
                    Question:what is the breakup of my salary?  
                    users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.salary" : 1, "salaryInfo.basicSalary" : 1, "salaryInfo.allowance" : 1})
                    Question:what is my fba allowance?  
                    users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.allowance" : 1})
                    Question:what is my fba balance?  
                    cards.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "fbaBalance" : 1})
                    Question:Can you tell me the amount I spent on Food?
                    transactions.aggregate([{'$match': {"uid": "95741f52-7d37-4176-947a-3def3c34396c","categoryId": "626a4fa4df010a2d9970f76d"}},{'$group': {'_id': None,'totalAmount': { '$sum': "$amt" }}}])
                    Question:List the Rejected transactions
                    transactions.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c", "approvalStatus" :"REJECTED"}, {"merchant": 1, "amt": 1})
                    Question:Give me all my reimbursed claims
                    claims.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c", "approvalStatus" :"REIMBURSED"}, {"merchant": 1, "amt": 1})
                    Question: what categories i have not spend so far ?
                    cards.aggregate([
                    {
                    '$match': { 
                    'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"
                    }
                    },
                    {
                        '$lookup': {
                        'from': "card-subwallets",
                        'let': { uid: "$uid" },
                        'pipeline': [
                                {
                            '$match': {
                                '$expr': {
                                '$and': [
                                    { '$eq': ["$uid", "$$uid"] },
                                    { '$eq': ["$taxRegimeCatEnabled", True] }]}}},
                            {
                            '$project': {
                                'categoryId': 1
                            }
                            }
                        ],
                        'as': "subwallets"
                        }
                    },
                    { '$unwind': "$subwallets" },
                    {
                        '$lookup': {
                        'from': "card_transactions",
                        'let': { 'categoryId': "$subwallets.categoryId", 'uid': "$uid" },
                        'pipeline': [
                            {
                            '$match': {
                                '$expr': {
                                '$and': [
                                    { '$eq': ["$categoryId", "$$categoryId"] },{ '$eq': ["$uid", "$$uid"] }]}}}],'as': "transactions"}},
                    { '$match': {'transactions': { '$size': 0 }}},
                    {
                        '$lookup': {
                        'from': "detax_categories",
                        'localField': "subwallets.categoryId",
                        'foreignField': "categoryId",'as': "categoryInfo"}},{'$project': {'_id': 0,categoryName: { '$arrayElemAt': ["$categoryInfo.name", 0] }}}])

                    Question: What is my De'tax Score?
                    cards.find({"uid" : "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"},{'score':1})

                    Question: What is the total amount credited so far?
                    transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "CREDIT","source" : "SYSTEM"})
                    Question: What is the total amount debited so far?
                    transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "DEBIT"})
                    Question: How many of my<<FBA/TA>>   transactions are pending bill upload?
                    transactions.find({"uid" : "628b934b5000aa370e985f0f","txnWallet" : "FBA","approvalStatus" : "BILL_UPLOAD_PENDING"})
                    Question: Which category has provided me the highest tax rebate?
                    transactions.aggregate([
                            {
                                '$match': {
                                'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"
                                }
                            },
                            {
                                '$group': {
                                '_id': "$categoryId",
                                'totalTaxRebateAmount': { '$sum': "$taxRebateEarnedAmount" }
                                }
                            },
                            {
                                '$sort': { 'totalTaxRebateAmount': -1 }
                            },
                            {
                                '$limit': 1
                            },
                            {
                                '$lookup': {
                                'from': "detax_categories",
                                'localField': "_id",
                                'foreignField': "objectId",
                                'as': "category"
                                }
                            },
                            {
                                '$unwind': "$category"
                            },
                            {
                                '$project': {
                                '_id': 0,
                                'categoryId': "$_id",
                                'categoryName': "$category.name",
                                'totalTaxRebateAmount': 1
                                }
                            }
                            ])
                    Question: In which category have I spent the maximum amount?
                    transactions.aggregate([
                    {
                        '$match': {
                        'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe","type" : "DEBIT"
                        }
                    },
                    {
                        '$group': {
                        '_id': "$categoryId",
                        'totalAmt': { '$sum': "$amt" }
                        }
                    },
                    {
                        '$lookup': {
                        'from': "detax_categories",
                        'localField': "_id",
                        'foreignField': "objectId",
                        'as': "category"
                        }
                    },
                    {
                        '$unwind': "$category"
                    },
                    {
                        '$sort': { 'totalAmt': -1 }
                    },
                    {
                        '$limit': 1
                    },
                    {
                        '$project': {
                        '_id': 0,
                        'categoryName': "$category.name",
                        'totalAmt': 1
                        }
                    }
                    ])
                    Question:What's the maximum tax rebate I have earned so far?
                    transactions.aggregate([
                    {
                        '$match': {
                        'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe",
                        'createdAt': {
                            '$gte': 'startDate',
                            '$lte': 'endDate'
                        }
                        }
                    },
                    {
                        '$group': {
                        '_id': "$categoryId",
                        'totalTaxRebateAmount': { '$sum': "$taxRebateEarnedAmount" }
                        }
                    },
                    {
                        '$group': {
                        '_id': 'None',
                        'totalTaxRebateAllCategories': { '$sum': "$totalTaxRebateAmount" }
                        }
                    },
                    {
                        '$project': {
                        '_id': 0,
                        'totalTaxRebateAllCategories': 1
                        }
                    }
                    ])
                    
                    Question: list all transactions for current financial year
                    card_transactions.find({"createdAt" : {'$gt' : 1711909800000, '$lt': 1743445799000}}) 

                
                    
                '''



    df_string = df.head(2).to_string(index=False)

    # Define the prompt
    system = "System: You are MongoDB Query Generator so only generate Query not any expalination or any thing else"
    prop='''{'role': 'system', 'content': "The user provides a question and you provide MongoDB Query. You will only respond with MongoDB Query and not with any explanations.\n\nRespond with only MongoDG Query. Do not answer with any explanations -- just the Query."},{"role":'system',"content":'Use responses to past questions also to guide you'},{"role":'user',"content":"what is the breakup of my salary?"},{"role":'user',"content":"""users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.salary" : 1, "salaryInfo.basicSalary" : 1, "salaryInfo.allowance" : 1})"""},
          {'role':'user','content':'what is my fba allowance? '},{'role':'user',"content":'users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.allowance" : 1})'},
          {"role":'user',"content":'Can you tell me the amount I spent on Food?'},{"role":'user',"content":'transactions.aggregate([{$match: {uid: "95741f52-7d37-4176-947a-3def3c34396c",categoryId: "626a4fa4df010a2d9970f76d"}},{$group: {_id: null,totalAmount: { $sum: "$amt" }}}])'},
        {"role":'user',"content":'what is my fba balance?'},{"role":'user',"content":'cards.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "fbaBalance" : 1})}
        {"role":'user',"content":'List the Rejected transactions?'},{"role":'user',"content":'transactions.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c", "approvalStatus" :"REJECTED"}, {"merchant": 1, "amt": 1})'}

          '''
    
    user_like = "User Instructions: Below, you'll find both the user's question and the Query Answered. Your objective is to thoroughly understand the provided data in order to craft a MongoDB query. Additionally, I'll offer an example of how the question is posed and how the query is generated to guide your process. your task is just to generate mongo db query by understanding"
    content = "Also giving the data frame how the data looks like in CSV Data:\n" + df_string + "\n"
    user_question = "Query: " + question + "\n"
    #prompt = [prop + "\n" + user_like + "\n" +refference +"\n"+ content+ "\n" + user_question+ "Only Generate MongoDB Query and don't give any other Information"]

    # Get the generated MongoDB query from the response
    start_phrase = system + refference +'\n'+ ddl+'\n'+  Documentation_statement + '\n' + "user_Question: " + question + "now Just generate only the Query no Explaination or anything else."
    response = client1.completions.create(model=deployment_name, prompt=start_phrase, max_tokens=1000)
    mongodb_query = response.choices[0].text.strip()
    #extract_generic_query(mongodb_query)
    return mongodb_query


connectionString = "mongodb+srv://genai-user:Figital123@genai.4acr5.mongodb.net/?retryWrites=true&w=majority&appName=GenAI"

def connect_mongoDB():
    client = MongoClient(connectionString)
    return client



df = pd.read_csv('txns 3.csv')
#question=input("enter the Question to generate Query: ")

client = connect_mongoDB()
print("client", client)
detaxCoreDB = client['detax-core']
ncaishAccountDB = client['ncaish-accounts']
users = ncaishAccountDB["users"]
transactions = detaxCoreDB["card_transactions"]
cards = detaxCoreDB["detax_cards"]
claims = detaxCoreDB["detax_claims"]
monthlyFba = detaxCoreDB["fba_monthly_analytics"]

'''question1="what is the breakup of the salary of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
question="what is the fba allowance of the uid='95741f52-7d37-4176-947a-3def3c34396c'?"  
question2="list the rejected transactions for the uid='95741f52-7d37-4176-947a-3def3c34396c'"
question3="Give me all my reimbursed claims of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
question4="What is FBA Balance of the uid='95741f52-7d37-4176-947a-3def3c34396c'"
question5="Can you tell me the amount spent on Food till now"
'''



# Define a context dictionary to provide necessary variables
context = {
    'transactions': transactions,
    'cards': cards,
    'claims': claims,
    'monthlyFba': monthlyFba,
    'users': users
}

# Execute the MongoDB query dynamically
query_string = ''' Enter your Query
Question: What is the total amount debited so far?
                    transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "DEBIT"})
                               '''


import ast
while True:
    question=input("Enter your Query: ")
    question+="of the 'uid'='95741f52-7d37-4176-947a-3def3c34396c'"
    mongodb_query=mongodb_query_generator(df,question)
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
        
        #detax_cards_cursor = users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.salary" : 1, "salaryInfo.basicSalary" : 1, "salaryInfo.allowance" : 1})
        #for data in detax_cards_cursor:
        #    print("data", data)
        # Execute the MongoDB query
    detax_cards_cursor = eval(mongodb_query_tuned)



# Now detax_cards_cursor contains the result of the executed query
    print(list(detax_cards_cursor))

# Now detax_cards_cursor contains the result of the executed query
    #print("The data is ::",list(detax_cards_cursor))
    # Iterate over the cursor to access the results
    for data in detax_cards_cursor:
        print("The Executed data is: ", data)


# #mongodb_query1=mongodb_query_generator(df,question2)
# #print("The Query Generated is :",mongodb_query)
# #if mongodb_query != "None":
#     detax_cards_cursor = mongodb_query
#     detax_cards_array = list(detax_cards_cursor)
#     print("data", detax_cards_array)

# #mongodb_query=mongodb_query_generator(df,question3)
# #print("The Query Generated is :",mongodb_query)
# #if mongodb_query != "None":
#     detax_cards_cursor = mongodb_query
#     detax_cards_array = list(detax_cards_cursor)
#     print("data", detax_cards_array)

# mongodb_query=mongodb_query_generator(df,question4)
# print("The Query Generated is :",mongodb_query)
# if mongodb_query != "None":
#     detax_cards_cursor = mongodb_query
#     detax_cards_array = list(detax_cards_cursor)
#     print("data", detax_cards_array)

# mongodb_query=mongodb_query_generator(df,question5)
# print("The Query Generated is :",mongodb_query)
# if mongodb_query != "None":
#     detax_cards_cursor = mongodb_query
#     detax_cards_array = list(detax_cards_cursor)
#     print("data", detax_cards_array)






