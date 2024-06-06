from llm_training_Chromadb import ChromaDB_VectorStore

vector_store = ChromaDB_VectorStore()

# # Add some sample data
# question_sql_id = vector_store.add_question_nosql("what is the breakup of my salary?", '''users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.salary" : 1, "salaryInfo.basicSalary" : 1, "salaryInfo.allowance" : 1})''')

# vector_store.add_question_nosql("what is my fba allowance?", '''users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.allowance" : 1})''')
# vector_store.add_question_nosql("what is the breakup of my salary?", '''users.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "salaryInfo.salary" : 1, "salaryInfo.basicSalary" : 1, "salaryInfo.allowance" : 1})''')

# vector_store.add_question_nosql("what is my fba balance?", '''cards.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c"},{"_id":0, "fbaBalance" : 1})''')
# vector_store.add_question_nosql("Can you tell me the amount I spent on Food?", '''transactions.aggregate([{'$match': {"uid": "95741f52-7d37-4176-947a-3def3c34396c","categoryId": "626a4fa4df010a2d9970f76d"}},{'$group': {'_id': None,'totalAmount': { '$sum': "$amt" }}}])''')

# vector_store.add_question_nosql("List the Rejected transactions", '''transactions.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c", "approvalStatus" :"REJECTED"}, {"merchant": 1, "amt": 1})''')
# vector_store.add_question_nosql("Give me all my reimbursed claims", '''claims.find({"uid" : "95741f52-7d37-4176-947a-3def3c34396c", "approvalStatus" :"REIMBURSED"}, {"merchant": 1, "amt": 1})''')


# vector_store.add_question_nosql("what categories i have not spend so far ?", ''' cards.aggregate([
#                     {
#                     '$match': { 
#                     'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"
#                     }
#                     },
#                     {
#                         '$lookup': {
#                         'from': "card-subwallets",
#                         'let': { uid: "$uid" },
#                         'pipeline': [
#                                 {
#                             '$match': {
#                                 '$expr': {
#                                 '$and': [
#                                     { '$eq': ["$uid", "$$uid"] },
#                                     { '$eq': ["$taxRegimeCatEnabled", True] }]}}},
#                             {
#                             '$project': {
#                                 'categoryId': 1
#                             }
#                             }
#                         ],
#                         'as': "subwallets"
#                         }
#                     },
#                     { '$unwind': "$subwallets" },
#                     {
#                         '$lookup': {
#                         'from': "card_transactions",
#                         'let': { 'categoryId': "$subwallets.categoryId", 'uid': "$uid" },
#                         'pipeline': [
#                             {
#                             '$match': {
#                                 '$expr': {
#                                 '$and': [
#                                     { '$eq': ["$categoryId", "$$categoryId"] },{ '$eq': ["$uid", "$$uid"] }]}}}],'as': "transactions"}},
#                     { '$match': {'transactions': { '$size': 0 }}},
#                     {
#                         '$lookup': {
#                         'from': "detax_categories",
#                         'localField': "subwallets.categoryId",
#                         'foreignField': "categoryId",'as': "categoryInfo"}},{'$project': {'_id': 0,categoryName: { '$arrayElemAt': ["$categoryInfo.name", 0] }}}])
#  ''')



# vector_store.add_question_nosql("What is my De'tax Score?", ''' cards.find({"uid" : "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"},{'score':1}) ''')


# vector_store.add_question_nosql(" What is the total amount credited so far?", '''transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "CREDIT","source" : "SYSTEM"}) ''')

# vector_store.add_question_nosql("What is the total amount debited so far?", ''' transactions.find({"uid" : "628b934b5000aa370e985f0f","type" : "DEBIT"}) ''')
# vector_store.add_question_nosql(" How many of my<<FBA/TA>>   transactions are pending bill upload? ", '''  transactions.find({"uid" : "628b934b5000aa370e985f0f","txnWallet" : "FBA","approvalStatus" : "BILL_UPLOAD_PENDING"}) ''')

# vector_store.add_question_nosql("Which category has provided me the highest tax rebate?", ''' transactions.aggregate([
#                             {
#                                 '$match': {
#                                 'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe"
#                                 }
#                             },
#                             {
#                                 '$group': {
#                                 '_id': "$categoryId",
#                                 'totalTaxRebateAmount': { '$sum': "$taxRebateEarnedAmount" }
#                                 }
#                             },
#                             {
#                                 '$sort': { 'totalTaxRebateAmount': -1 }
#                             },
#                             {
#                                 '$limit': 1
#                             },
#                             {
#                                 '$lookup': {
#                                 'from': "detax_categories",
#                                 'localField': "_id",
#                                 'foreignField': "objectId",
#                                 'as': "category"
#                                 }
#                             },
#                             {
#                                 '$unwind': "$category"
#                             },
#                             {
#                                 '$project': {
#                                 '_id': 0,
#                                 'categoryId': "$_id",
#                                 'categoryName': "$category.name",
#                                 'totalTaxRebateAmount': 1
#                                 }
#                             }
#                             ])
#                                 ''')


# vector_store.add_question_nosql(" In which category have I spent the maximum amount?", '''   transactions.aggregate([
#                     {
#                         '$match': {
#                         'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe","type" : "DEBIT"
#                         }
#                     },
#                     {
#                         '$group': {
#                         '_id': "$categoryId",
#                         'totalAmt': { '$sum': "$amt" }
#                         }
#                     },
#                     {
#                         '$lookup': {
#                         'from': "detax_categories",
#                         'localField': "_id",
#                         'foreignField': "objectId",
#                         'as': "category"
#                         }
#                     },
#                     {
#                         '$unwind': "$category"
#                     },
#                     {
#                         '$sort': { 'totalAmt': -1 }
#                     },
#                     {
#                         '$limit': 1
#                     },
#                     {
#                         '$project': {
#                         '_id': 0,
#                         'categoryName': "$category.name",
#                         'totalAmt': 1
#                         }
#                     }
#                     ])
#                     Question:What's the maximum tax rebate I have earned so far?
#                     transactions.aggregate([
#                     {
#                         '$match': {
#                         'uid': "f8c9cfb5-c505-4d9d-8ff4-f0e5763b14fe",
#                         'createdAt': {
#                             '$gte': 'startDate',
#                             '$lte': 'endDate'
#                         }
#                         }
#                     },
#                     {
#                         '$group': {
#                         '_id': "$categoryId",
#                         'totalTaxRebateAmount': { '$sum': "$taxRebateEarnedAmount" }
#                         }
#                     },
#                     {
#                         '$group': {
#                         '_id': 'None',
#                         'totalTaxRebateAllCategories': { '$sum': "$totalTaxRebateAmount" }
#                         }
#                     },
#                     {
#                         '$project': {
#                         '_id': 0,
#                         'totalTaxRebateAllCategories': 1
#                         }
#                     }
#                     ])
#                                 ''')


# vector_store.add_question_nosql("list all transactions for current financial year ", ''' card_transactions.find({"createdAt" : {'$gt' : 1711909800000, '$lt': 1743445799000}}) ''')

# # vector_store.add_question_nosql("", ''' ''')
# # vector_store.add_question_nosql("", ''' ''')

# # vector_store.add_question_nosql("", ''' ''')
# # vector_store.add_question_nosql("", ''' ''')


# ddl_id = vector_store.add_ddl('''
# You have a MongoDB collection named "transactions" with the following schema:

# Attributes:
#   - _id: ObjectId
#   - uid: String
#   - cid: String
#   - cardId: String
#   - amt: Double
#   - type: String
#   - source: String
#   - txnWallet: String
#   - remarks: String
#   - txnStatus: String
#   - billUploadedAt: Long
#   - claimRaisedAt: Int
#   - approvedAt: Long
#   - batchId: String
#   - partnerTxnId: String
#   - claimRaised: Bool
#   - recharge: Bool
#   - objectId: String
#   - createdBy: String
#   - deletedAt: Int
#   - updatedAt: Long
#   - createdAt: Long
#   - archive: Bool
#   - _class: String
#   - merchant: String
#   - surCharge: Bool
#   - appAmt: Double
#   - approvalStatus: String
#   - approvedBy: String
#   - approverRemarks: String
#   - baseAmt: Int
#   - bills: Array of String
#   - categoryId: String
#   - cgst: Double
#   - channel: String
#   - claimId: String
#   - creditRRNs: Array of String
#   - day: String
#   - declineReason: String
#   - forceApproval: Bool
#   - fuelSurchargeAmt: Double
#   - gstin: String
#   - institutionCode: String
#   - kitNumber: String
#   - lastCategoryId: String
#   - linkedTxns: Bool
#   - manualTransaction: Bool
#   - manualTxnCreatedAt: Long
#   - mcc: String
#   - merchantId: String
#   - month: String
#   - network: String
#   - partnerTxnType: String
#   - rebateEligible: Bool
#   - referenceAmt: Int
#   - referenceDateTime: Long
#   - referenceMerchantName: String
#   - referenceRrn: String
#   - referernceTxnId: String
#   - reversedAt: Long
#   - rrn: String
#   - scope: String
#   - sgst: Double
#   - sysAssignedCategory: String
#   - systemTraceAuditNumber: Int
#   - taxRebateEarnedAmount: Double
#   - taxRebateEarnedAmountForFY: Double
#   - terminalID: String
#   - traceNo: Int
#   - userSelectedCategory: String
#   - userSelectedCategoryApproved: String or Bool (mixed type array)
#   - validationStatus: String
#   This Schema is for collection name transactions
# ''')


# documentation_id = vector_store.add_documentation('''Here by I am providing some Basic Information:
#     Our definition of financial year is that the financial year starts in April and concludes in March of the following year. For instance, if the current month is March 2024, the ongoing financial year spans from April 2023 to March 2024. If the current month is June 2024, the active financial year extends from April 2024 to March 2025.")
#     Our definition of food is Food/Provisions")
#     Our definition of fuel is Fuel & Travel")
#     Our definition of available limit is availablelimit")
#     Our definition of FBA categories is txnwallet='FBA'")
#     Our definition of spending categories is txnwallet='FBA'"
#     Our definition of expense wallet is txnwallet='TA'")
#     Our definition of travel wallet is txnwallet='TA'")
#     Our definition of so far is current financial year")
#     "Our definition of till now is current financial year")
#     Our definition of thus far is current financial year")
#     "Our definition of pending bill upload is ApprovalStatus='BILL UPLOAD PENDING'")
#     Our definition of bill upload pending is ApprovalStatus='BILL UPLOAD PENDING'")
#     Our definition of FBA is Flexible Benefit Allowance")

#     ''')



# Get training data
training_data = vector_store.get_training_data()
print("Training Data:")
print(training_data)

# Get similar SQL question
similar_nosql_questions = vector_store.get_similar_question_nosql("list all the rejected transactions ")

print("Similar NOSQL Questions:")
print(type(similar_nosql_questions))

# Get related DDL
related_ddl = vector_store.get_related_ddl("list all the rejected transactions ")
print("Related DDL:")
print(type(related_ddl))

# Get related documentation
related_documentation = vector_store.get_related_documentation("list all the rejected transactions ")
print("Related Documentation:")
print(type(related_documentation))


initial_prompt = "Please help to generate a MONGODB Query ONLY to answer the question. Your response should ONLY be based on the given context and follow the response guidelines and format instructions."

# Initialize the full_prompt with the initial prompt
full_prompt = initial_prompt.strip() + "\n"

import re

def remove_extra_spaces(input_string):
    # Remove line breaks
    no_line_breaks = input_string.replace('\n', ' ').replace('\r', ' ')
    # Remove multiple spaces
    no_extra_spaces = re.sub(' +', ' ', no_line_breaks)
    return no_extra_spaces.strip()

if similar_nosql_questions:
    full_prompt += "Similar NoSQL Questions:\n"
    for item in similar_nosql_questions:
        question = remove_extra_spaces(item['question'])
        nosql = remove_extra_spaces(item['nosql'])
        full_prompt += f"Question: {question}\nNoSQL Query: {nosql}\n"


# Process related_ddl
if related_ddl:
    full_prompt += "Related DDL:\n"
    for ddl in related_ddl:
        full_prompt += ddl.strip().replace('\n', ' ').replace('  ', ' ') + "\n"

# Process related_documentation
if related_documentation:
    full_prompt += "Related Documentation:\n"
    for doc in related_documentation:
        full_prompt += doc.strip().replace('\n', ' ').replace('  ', ' ') + "\n"

# Remove any trailing whitespace and newlines
full_prompt = full_prompt.strip()
    


import tiktoken

def count_tokens(text):
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(text)
    return len(tokens)

print(full_prompt)

print("the token lenght is :", count_tokens(full_prompt))
# # Remove a piece of training data
# vector_store.remove_training_data(question_sql_id)

# # Remove a collection
# vector_store.remove_collection("ddl")

