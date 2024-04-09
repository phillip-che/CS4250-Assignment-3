#-------------------------------------------------------------------------
# AUTHOR: Phillip Che
# FILENAME: db_connection_mongo.py
# SPECIFICATION: 
# FOR: CS 4250- Assignment #3
# TIME SPENT: 
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime

def connectDataBase():
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017
    # Create a database connection object using pymongo
    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        return db
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    terms = docText.lower().split(" ")
    index = {}

    for term in terms:
        if term not in index:
            index[term] = 0
        index[term] = index[term] + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    termObjects = []
    for key in index:
        termObj = {
            "term": key,
            "count": index[key],
            "num_char": len(key)
        }
        termObjects.append(termObj)
    
    # produce a final document as a dictionary including all the required document fields
    doc = {"id": docId, "text": docText, "title": docTitle ,"date": docDate, "category": docCat}
    
    # insert the document
    res = col.insert_one(doc)
    print(res)

def deleteDocument(col, docId):
    print("delete")
    # Delete the document from the database
    # --> add your Python code here

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    print("index")
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here