

from llm_training_Chromadb import ChromaDB_VectorStore
from no_sql_generator import NoSQLGenerator


api_key_file = "api_key.txt"
with open(api_key_file, "r") as file:
    api_key = file.read().strip()

deployment_name='LLM_response'
azure_endpoint="https://testing1234.openai.azure.com/"

connectionString = "mongodb+srv://genai-user:Figital123@genai.4acr5.mongodb.net/?retryWrites=true&w=majority&appName=GenAI"

vector_store = ChromaDB_VectorStore()
nosql_generator = NoSQLGenerator(connectionString)



while True:

    question=input("Enter Your Question here: ")
    prompt=nosql_generator.extract_prompt(question)
    token_len=nosql_generator.count_tokens(prompt)
    
    print("Your prompt Token Length: ",token_len)

    query=nosql_generator.Azure_llm_Connection(api_key,azure_endpoint,deployment_name,prompt)
    
    print("The Query Generated is: ",query)
  
    detax_cards_cursor=nosql_generator.run_query(query)
    print("------------------------------------------------------------------")
    print("result : ",list(detax_cards_cursor))