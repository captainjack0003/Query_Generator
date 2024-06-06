import json
from typing import List
import os
import chromadb
import pandas as pd
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from uuid import uuid4

default_ef = embedding_functions.DefaultEmbeddingFunction()

class ChromaDB_VectorStore:
    def __init__(self, config=None):
        if config is None:
            config = {}
        

        path = config.get("path", ".")
        
        # Create the folder if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)

        path = config.get("path", ".")
        self.embedding_function = config.get("embedding_function", default_ef)
        curr_client = config.get("client", "persistent")
        collection_metadata = config.get("collection_metadata", None)
        self.n_results_nosql = config.get("n_results_nosql", config.get("n_results", 10))
        self.n_results_documentation = config.get("n_results_documentation", config.get("n_results", 10))
        self.n_results_ddl = config.get("n_results_ddl", config.get("n_results", 10))

        if curr_client == "persistent":
            self.chroma_client = chromadb.PersistentClient(
                path=path, settings=Settings(anonymized_telemetry=False)
            )
        elif curr_client == "in-memory":
            self.chroma_client = chromadb.EphemeralClient(
                settings=Settings(anonymized_telemetry=False)
            )
        elif isinstance(curr_client, chromadb.api.client.Client):
            # allow providing client directly
            self.chroma_client = curr_client
        else:
            raise ValueError(f"Unsupported client was set in config: {curr_client}")

        self.documentation_collection = self.chroma_client.get_or_create_collection(
            name="documentation",
            embedding_function=self.embedding_function,
            metadata=collection_metadata,
        )
        self.ddl_collection = self.chroma_client.get_or_create_collection(
            name="ddl",
            embedding_function=self.embedding_function,
            metadata=collection_metadata,
        )
        self.nosql_collection = self.chroma_client.get_or_create_collection(
            name="nosql",
            embedding_function=self.embedding_function,
            metadata=collection_metadata,
        )

    def generate_embedding(self, data: str, **kwargs) -> List[float]:
        embedding = self.embedding_function([data])
        if len(embedding) == 1:
            return embedding[0]
        return embedding

    def add_question_nosql(self, question: str, nosql: str, **kwargs) -> str:
        question_nosql_json = json.dumps(
            {
                "question": question,
                "nosql": nosql,
            },
            ensure_ascii=False,
        )
        id = str(uuid4()) + "-nosql"
        self.nosql_collection.add(
            documents=question_nosql_json,
            embeddings=self.generate_embedding(question_nosql_json),
            ids=id,
        )

        return id

    def add_ddl(self, ddl: str, **kwargs) -> str:
        id = str(uuid4()) + "-ddl"
        self.ddl_collection.add(
            documents=ddl,
            embeddings=self.generate_embedding(ddl),
            ids=id,
        )
        return id

    def add_documentation(self, documentation: str, **kwargs) -> str:
        id = str(uuid4()) + "-doc"
        self.documentation_collection.add(
            documents=documentation,
            embeddings=self.generate_embedding(documentation),
            ids=id,
        )
        return id

    def get_training_data(self, **kwargs) -> pd.DataFrame:
        nosql_data = self.nosql_collection.get()

        df = pd.DataFrame()

        if nosql_data is not None:
            # Extract the documents and ids
            documents = [json.loads(doc) for doc in nosql_data["documents"]]
            ids = nosql_data["ids"]

            # Create a DataFrame
            df_nosql = pd.DataFrame(
                {
                    "id": ids,
                    "question": [doc["question"] for doc in documents],
                    "content": [doc["nosql"] for doc in documents],
                }
            )

            df_nosql["training_data_type"] = "nosql"

            df = pd.concat([df, df_nosql])

        ddl_data = self.ddl_collection.get()

        if ddl_data is not None:
            # Extract the documents and ids
            documents = [doc for doc in ddl_data["documents"]]
            ids = ddl_data["ids"]

            # Create a DataFrame
            df_ddl = pd.DataFrame(
                {
                    "id": ids,
                    "question": [None for doc in documents],
                    "content": [doc for doc in documents],
                }
            )

            df_ddl["training_data_type"] = "ddl"

            df = pd.concat([df, df_ddl])

        doc_data = self.documentation_collection.get()

        if doc_data is not None:
            # Extract the documents and ids
            documents = [doc for doc in doc_data["documents"]]
            ids = doc_data["ids"]

            # Create a DataFrame
            df_doc = pd.DataFrame(
                {
                    "id": ids,
                    "question": [None for doc in documents],
                    "content": [doc for doc in documents],
                }
            )

            df_doc["training_data_type"] = "documentation"

            df = pd.concat([df, df_doc])

        return df

    def remove_training_data(self, id: str, **kwargs) -> bool:
        if id.endswith("-nosql"):
            self.nosql_collection.delete(ids=id)
            return True
        elif id.endswith("-ddl"):
            self.ddl_collection.delete(ids=id)
            return True
        elif id.endswith("-doc"):
            self.documentation_collection.delete(ids=id)
            return True
        else:
            return False

    def remove_collection(self, collection_name: str) -> bool:
        """
        This function can reset the collection to empty state.

        Args:
            collection_name (str): sql or ddl or documentation

        Returns:
            bool: True if collection is deleted, False otherwise
        """
        if collection_name == "nosql":
            self.chroma_client.delete_collection(name="nosql")
            self.nosql_collection = self.chroma_client.get_or_create_collection(
                name="nosql", embedding_function=self.embedding_function
            )
            return True
        elif collection_name == "ddl":
            self.chroma_client.delete_collection(name="ddl")
            self.ddl_collection = self.chroma_client.get_or_create_collection(
                name="ddl", embedding_function=self.embedding_function
            )
            return True
        elif collection_name == "documentation":
            self.chroma_client.delete_collection(name="documentation")
            self.documentation_collection = self.chroma_client.get_or_create_collection(
                name="documentation", embedding_function=self.embedding_function
            )
            return True
        else:
            return False

    @staticmethod
    def _extract_documents(query_results) -> list:
        """
        Static method to extract the documents from the results of a query.

        Args:
            query_results (pd.DataFrame): The dataframe to use.

        Returns:
            List[str] or None: The extracted documents, or an empty list or
            single document if an error occurred.
        """
        if query_results is None:
            return []

        if "documents" in query_results:
            documents = query_results["documents"]

            if len(documents) == 1 and isinstance(documents[0], list):
                try:
                    documents = [json.loads(doc) for doc in documents[0]]
                except Exception as e:
                    return documents[0]

            return documents

    def get_similar_question_nosql(self, question: str, **kwargs) -> list:
        return ChromaDB_VectorStore._extract_documents(
            self.nosql_collection.query(
                query_texts=[question],
                n_results=self.n_results_nosql,
            )
        )

    def get_related_ddl(self, question: str, **kwargs) -> list:
        return ChromaDB_VectorStore._extract_documents(
            self.ddl_collection.query(
                query_texts=[question],
                n_results=self.n_results_ddl,
            )
        )

    def get_related_documentation(self, question: str, **kwargs) -> list:
        return ChromaDB_VectorStore._extract_documents(
            self.documentation_collection.query(
                query_texts=[question],
                n_results=self.n_results_documentation,
            )
        )


def main():
    # Create a ChromaDB_VectorStore instance
    vector_store = ChromaDB_VectorStore()

    # Add some sample data
    question_sql_id = vector_store.add_question_nosql("What is the query to find all users?", "db.users.find({})")
    ddl_id = vector_store.add_ddl("CREATE TABLE table_name (column1 datatype, column2 datatype, ...)",)
    documentation_id = vector_store.add_documentation("This is the documentation for the table creation process.")

    # Get training data
    training_data = vector_store.get_training_data()
    print("Training Data:")
    print(training_data)
    
    # Get similar SQL question
    similar_nosql_questions = vector_store.get_similar_question_nosql("find all users")
    print("Similar NOSQL Questions:")
    print(similar_nosql_questions)

    # Get related DDL
    related_ddl = vector_store.get_related_ddl("How to create a table?")
    print("Related DDL:")
    print(related_ddl)

    # Get related documentation
    related_documentation = vector_store.get_related_documentation("Table creation process")
    print("Related Documentation:")
    print(related_documentation)

    # # Remove a piece of training data
    # vector_store.remove_training_data(question_sql_id)

    # # Remove a collection
    # vector_store.remove_collection("ddl")


if __name__ == "__main__":
    main()
