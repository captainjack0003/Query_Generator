import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from llm_training_Chromadb import ChromaDB_VectorStore
from no_sql_generator import NoSQLGenerator


connectionString = "Enter Your DB Connection String with User name and Password"

vector_store = ChromaDB_VectorStore()
nosql_generator = NoSQLGenerator(connectionString)

def custom_llm(prompt):
    torch.set_default_device("cuda")

    model = AutoModelForCausalLM.from_pretrained("microsoft/phi-2", torch_dtype="auto", trust_remote_code=True)
    tokenizer = AutoTokenizer.from_pretrained("microsoft/phi-2", trust_remote_code=True)
    inputs = tokenizer(prompt, return_tensors="pt", return_attention_mask=False)

    outputs = model.generate(**inputs, max_length=400)
    text = tokenizer.batch_decode(outputs)[0]
    print(text)
    return text


while True:

    question=input("Enter Your Question here: ")
    prompt=nosql_generator.extract_prompt(question)
    query=custom_llm(prompt)
    print("The Query Generated is: ",query)
    print("------------------------------------------------------------------")
    detax_cards_cursor=nosql_generator.run_query(query)
    print("result : ",list(detax_cards_cursor))
