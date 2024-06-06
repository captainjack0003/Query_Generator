import pandas as pd
import time as t
from openai import AzureOpenAI
from pymongo import MongoClient
import re
import tiktoken
from llm_training_Chromadb import ChromaDB_VectorStore


class NoSQLGenerator:
    
    def __init__(self, connection_string: str):
        """
        Initialize the NoSQLGenerator with a connection to MongoDB.
        :param connection_string: MongoDB connection string.
        Initialize all the Collections.

        """
        self.vector_store = ChromaDB_VectorStore()

        self.client = self.db_connection(connection_string)
        print("client", self.client)
        self.detaxCoreDB = self.client['detax-core']
        self.ncaishAccountDB = self.client['ncaish-accounts']
        self.users = self.ncaishAccountDB["users"]
        self.transactions = self.detaxCoreDB["card_transactions"]
        self.cards = self.detaxCoreDB["detax_cards"]
        self.claims = self.detaxCoreDB["detax_claims"]
        self.monthlyFba = self.detaxCoreDB["fba_monthly_analytics"]

        pass

    
    def db_connection(self, connection_string: str):
        """
        Establish a connection to MongoDB.
        :param connection_string: MongoDB connection string.
        :return: MongoClient instance.
        """
        return MongoClient(connection_string)
    
    def llm_connection(self, prompt: str) -> str:
        """
        Connect to the LLM and get a response for the given prompt.

        :param prompt: The prompt to send to the LLM.
        :return: Response from the LLM.
        """
        response = AzureOpenAI.Completion.create(prompt=prompt)  # Update this line with actual call
        return response
    
    def Azure_llm_Connection(self,api_key,end_point,deployment_name,full_prompt):
        
        token_len=self.count_tokens(full_prompt)
        
        if token_len > 4096:
            print("Your prompt is out of Token limit, Please Reduce your Prompt Size. ")
            return None
        
        client_azure = AzureOpenAI(
            azure_endpoint=end_point,
            api_key=api_key,
            api_version="2024-02-15-preview"
            )
        
        response = client_azure.completions.create(model=deployment_name, prompt=full_prompt, max_tokens=1000)
        mongodb_query = response.choices[0].text.strip()
        #extract_generic_query(mongodb_query)
        return mongodb_query

    
    def generate_query(self, user_question: str) -> str:
        """
        Generate a MongoDB query based on a user's question using the LLM.

        :param user_question: The user's question in natural language.
        :return: Generated MongoDB query.
        """
        #prompt = f"Generate a MongoDB query for the following question: {user_question}"
        #query = self.llm_connection(prompt)
        #return query
        pass
    
    def preprocess_query(self,query: str)->str:
        
        tuned_query=self.extract_mongodb_tuned_query(query)

        if tuned_query:
            for query_name, query_content in tuned_query:
                mongodb_query_tuned=query_name + "(" + query_content.strip() + ")"
                print("Extracted MongoDB Tuned query:",mongodb_query_tuned)
                return mongodb_query_tuned
                #t.sleep(5)

        else:
            print("No MongoDB query found.")
            return None

        pass

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a MongoDB query and return the results as a pandas DataFrame.
        :param query: The MongoDB query.
        :return: Query results as a pandas DataFrame.
        """

        users = self.users
        transactions = self.transactions
        cards = self.cards
        claims = self.claims
        monthlyFba = self.monthlyFba
        null=None
        tuned_query=self.preprocess_query(query)
        if tuned_query is None:
            print("Query was not able to extract.")
        
        detax_cards_cursor = eval(tuned_query)

        return detax_cards_cursor

    
    
    def extract_prompt(self, Question: str):
        initial_prompt = ''' Please help to generate a MONGODB Query ONLY to answer the question. Your response should ONLY be based on the given context and follow the response guidelines and format instructions.'''
        
        full_prompt = initial_prompt.strip() + "\n"
        similar_nosql_questions = self.vector_store.get_similar_question_nosql(Question)
        related_ddl = self.vector_store.get_related_ddl(Question)
        related_documentation = self.vector_store.get_related_documentation(Question)


        if similar_nosql_questions:
            full_prompt += "Similar NoSQL Questions:\n"
            for item in similar_nosql_questions:
                question = self.remove_extra_spaces(item['question'])
                nosql = self.remove_extra_spaces(item['nosql'])
                full_prompt += f"Question: {question}\nNoSQL Query: {nosql}\n"
        
        if related_ddl:
            full_prompt += "Related DDL:\n"
            for ddl in related_ddl:
                full_prompt += ddl.strip().replace('\n', ' ').replace('  ', ' ') + "\n"
        
        if related_documentation:
            full_prompt += "Related Documentation:\n"
            for doc in related_documentation:
                full_prompt += doc.strip().replace('\n', ' ').replace('  ', ' ') + "\n"
        
        Question+=" of the 'uid'='95741f52-7d37-4176-947a-3def3c34396c'"
        last_prompt="This is the Qurstion Given Below " + "\n"+ Question+"\n"+"Provide only the MongoDB query as an answer to the question below. Use the format and style of the examples provided earlier. Do not include any explanation or additional text just the MongoDB query."
        
        full_prompt=full_prompt + "\n" + last_prompt
        full_prompt = full_prompt.strip()
        return full_prompt
    
    @staticmethod
    def extract_mongodb_tuned_query(query_string: str):
        """
        Extract MongoDB queries from a string using regular expressions.

        :param query_string: The string containing MongoDB queries.
        :return: List of extracted MongoDB queries.
        """
        matches = re.findall(
            r'\b(transactions\.find|cards\.find|users\.find|cards\.aggregate|transactions\.aggregate|users\.aggregate)\s*\((.*?)\)',
            query_string, re.DOTALL
        )
        return matches if matches else None
    
    @staticmethod
    def remove_extra_spaces(input_string):
        # Remove line breaks
        no_line_breaks = input_string.replace('\n', ' ').replace('\r', ' ')
        # Remove multiple spaces
        no_extra_spaces = re.sub(' +', ' ', no_line_breaks)
        return no_extra_spaces.strip()
    
    @staticmethod
    def count_tokens(text):
        encoding = tiktoken.get_encoding("cl100k_base")
        tokens = encoding.encode(text)
        return len(tokens)


# Example usage:
# connection_string = "your_mongodb_connection_string"
# nosql_generator = NoSQLGenerator(connection_string)
# user_question = "Find all transactions above $1000"
# query = nosql_generator.generate_query(user_question)
# result_df = nosql_generator.run_query(database_name="your_db", collection_name="transactions", query=query)
# print(result_df)

# connectionString = "mongodb+srv://genai-user:Figital123@genai.4acr5.mongodb.net/?retryWrites=true&w=majority&appName=GenAI"


# vector_store = ChromaDB_VectorStore()

# question=input("Enter Your Question here: ")
# nosql_generator = NoSQLGenerator(connectionString)
# similar_nosql_questions = vector_store.get_similar_question_nosql(question)
# related_ddl = vector_store.get_related_ddl(question)
# related_documentation = vector_store.get_related_documentation(question)

# prompt=nosql_generator.extract_prompt(similar_nosql_questions,related_ddl,related_documentation,question)

# api_key_file = "api_key.txt"
# with open(api_key_file, "r") as file:
#     api_key = file.read().strip()

# deployment_name='LLM_response'
# azure_endpoint="https://testing1234.openai.azure.com/"

# query=nosql_generator.Azure_llm_Connection(api_key,azure_endpoint,deployment_name,prompt)
# #query=nosql_generator.extract_mongodb_queries(query)
# print("The Query Generated is: ",query)

# detax_cards_cursor=nosql_generator.run_query(query)
# print(list(detax_cards_cursor))