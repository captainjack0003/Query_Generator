from llm_training_Chromadb import ChromaDB_VectorStore
from no_sql_generator import NoSQLGenerator

# Store Your api-key with file name api_key.txt

api_key_file = "api_key.txt"
with open(api_key_file, "r") as file:
    api_key = file.read().strip()


def main() :
    deployment_name='Enter Your Deployment Name on azure'
    azure_endpoint="enter your Azure End Point"
    
    connectionString = "Enter Your DB Connection String With user name and password"
    
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


if __name__ == "__main__":
    main()
