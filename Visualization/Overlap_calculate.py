import csv
import sys
import io
import pymongo
from pymongo import MongoClient

now = datetime.now()

cluster = MongoClient('mongodb+srv://djs:djs2503@twitchoverlap.d1ezuc3.mongodb.net/test?authMechanism=SCRAM-SHA-1')
db = cluster["TwitchOverlap"]

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="UTF-8")

def create_overlap_dict(dict):
    viewerOverlapDict = {}
    count = 1
    completedStreamers = [] #Save which streamers have been processed to avoid repeating
    for key in dict:
        dict[key] = set(dict[key]) #Make viewer list a set to dramatically decrease comparison time
    for key in dict:
        tempList = {}

        totalLength = len(dict.keys())
        logger.info(str(count) + "/" + str(totalLength)) #Print progress so I can keep track

        for comparisonKey in dict: #Loop through every key again for each key in the dictionary
            if(comparisonKey != key and comparisonKey not in completedStreamers): #If its not a self comparison and the comparison hasn't already been completed
                overlapSize = len(dict[key] & dict[comparisonKey]) #Find the overlap size of the two streamers using set intersection
                if(overlapSize > 500 ):
                    tempList[comparisonKey] = overlapSize #If the size is over 300 add {comparisonStreamer: overlap} to the dictionary
        viewerOverlapDict[key] = tempList #Add this comparison dictionary to the larger dictionary for that streamer
        completedStreamers.append(key) #Add the streamer to completed as no comparisons using this streamer need to be done anymore
        count+=1
    return viewerOverlapDict

if __name__ == '__main__':
    cols=[]
    for coll_name in db.list_collection_names():
        cols.append(coll_name)
    c_name=db[cols[0]]
    raw_dict= list(c_name.find())
    overlap_dict = create_overlap_dict(raw_dict)
    
