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
import string

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
    terms = str(docText.lower().translate(str.maketrans('', '', string.punctuation))).split(" ")
    
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
    doc = {"id": docId, "text": docText, "title": docTitle ,"date": docDate, "category": docCat, "terms": termObjects}
    
    # insert the document
    res = col.insert_one(doc)

def deleteDocument(col, docId):
    # Delete the document from the database
    res = col.delete_one({"id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...

    pipeline = [
        {"$unwind": { "path": "$terms" }},
        {"$project": { "_id": 0}},
        {"$sort": {"terms.term": 1 } }
    ]
    res = col.aggregate(pipeline)

    index = {}

    for doc in res:
        title = doc["title"]
        term = doc["terms"]["term"]
        count = str(doc["terms"]["count"])
        val = title + ":" + count

        if term not in index:
            index[term] = val
        else:
            index[term] = index[term] + "," + val
    
    return index