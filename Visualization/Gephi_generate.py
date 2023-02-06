import csv
import pickle as pkl
import sys
from datetime import datetime
import pymongo
from pymongo import MongoClient

now = datetime.now()

cluster = MongoClient('mongodb+srv://djs:djs2503@twitchoverlap.d1ezuc3.mongodb.net/test?authMechanism=SCRAM-SHA-1')
db = cluster["TwitchOverlap"]
def upload_to_mongo(data,Cname):
    global db
    collection = db[Cname]
    collection.insert_one(data)
#Generates a new csv file for the edge list on Gephi
def GenerateGephiData(dict):
    fileString = "Visualization/GephiData/%s" % (now.strftime("%m.%d.%Y.%H.%MEDGELIST.csv"))
    with open(fileString, 'w') as csvfile:
        writer = csv.writer(csvfile)
        #writer.writeheader()
        writer.writerow(["Source", "Target", "Weight"]) #These column headers are used in Gephi automatically
        for key, value in dict.items():
            nodeA = key
            for node, count in value.items():
                nodeB = node
                writer.writerow([nodeA, nodeB, count]) #nodeA is streamer1, nodeB is streamer2, and count is their overlapping viewers

#Generates a new csv file for the node list labels on Gephi
def GenerateGephiLabels(rawDict):
    fileString = "Visualization/GephiData/%s" % (now.strftime("%m.%d.%Y.%H.%MLABELS.csv"))
    print("Generating Labels...")
    sys.stdout.flush()
    with open(fileString, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["ID", "Label", "Count"]) #These columns are used in Gephi automatically
        for key, value in rawDict.items():
            writer.writerow([key, key, len(value)]) #This data is streamer1, streamer1, and # of unique viewers for streamer1


if __name__ == "__main__":
    cols=[]
    for coll_name in db.list_collection_names():
        cols.append(coll_name)
    raw_dict=db[cols[0]]
    overlap_count_dict=db[cols[1]]
    raw_overlap_dict= list(raw_dict.find())
    overlap_count_dict = list(overlap_count_dict.find())
    overlap_count_dict.drop()
    raw_dict.drop()
    GenerateGephiData(overlap_count_dict)
    GenerateGephiLabels(raw_overlap_dict)
