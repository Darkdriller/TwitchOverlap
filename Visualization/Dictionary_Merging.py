import pickle as pkl
import datetime
import pymongo
from pymongo import MongoClient
#S3 Helper Functions

cluster = MongoClient('mongodb+srv://djs:djs2503@twitchoverlap.d1ezuc3.mongodb.net/test?authMechanism=SCRAM-SHA-1')
db = cluster["TwitchOverlap"]
def upload_to_mongo(data,Cname):
    global db
    collection = db[Cname]
    collection.insert_one(data)

def combine_dictionaries(dict1, dict2):
    shared_set = dict1.keys() & dict2.keys()
    
    master_dict = {}
    for key in shared_set:
        shared_chatters = dict1[key]
        shared_chatters.extend(dict2[key])
        master_dict[key] = list(set(shared_chatters))
        del dict1[key]
        del dict2[key]

    master_dict.update(dict1)
    master_dict.update(dict2)

    return master_dict

def main():
    global db
    
    master_dict = {}
    count = 0
    for coll_name in db.list_collection_names():
        current_collection=db[coll_name]
        data = list(current_collection.find())
        master_dict = combine_dictionaries(master_dict, data)
        count += 1
        current_collection.drop()
    filename = datetime.datetime.utcnow().strftime("%m-%d-%Y_%H:%M")
    upload_to_mongo(master_dict,filename)

if __name__ == '__main__':
    main()