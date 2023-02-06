import pymongo
from pymongo import MongoClient
import os
import csv
import sys
import datetime
import json
import requests
import io
import aiohttp
import asyncio


cluster = MongoClient('mongodb+srv://djs:djs2503@twitchoverlap.d1ezuc3.mongodb.net/test?authMechanism=SCRAM-SHA-1')
db = cluster["TwitchOverlap"]


#Headers = {'Client-ID': , 'Authorization': "Bearer " + } Currently not accessing anything that needs credentials
#Asyncio optimization makes the viewer requests almost instantaneos.

def read_csv_channel_list(file):
    channel_list = []
    with open(file, 'r', encoding='utf-8') as filehandler:
        channel_list = filehandler.readlines()
    channel_list = [streamer[:-1] for streamer in channel_list]
    return channel_list
def upload_to_mongo(data,Cname):
    global db
    collection = db[Cname]
    collection.insert_one(data)





async def get_viewers_for_streamer(streamer, session):
    print(f"Getting viewers for {streamer}")
    #Requests to tmi.twitch for the viewers in each stream
    try:
        url = f"http://tmi.twitch.tv/group/user/{streamer.lower()}/chatters"
        response = await session.get(url)
        response = await response.json(content_type=None)
    except json.decoder.JSONDecodeError:
        response = ""
    #viewerlist consists of the streamers vips, mods, and chatters
    if response:
        viewers = response['chatters']['vips'] + response['chatters']['moderators'] + response['chatters']['viewers'] #List consists of users in chat tagged as viewer or VIP
        return ({streamer: viewers})
    #dict[streamer] = viewers

async def create_streamer_viewer_dict(channel_list):
    dict = {}
    async with aiohttp.ClientSession() as session:
        obj = await asyncio.gather(*[get_viewers_for_streamer(streamer, session) for streamer in channel_list])
    for pair in obj:
        if pair:
            dict.update(pair)
    return dict

def main(data, context):
    channel_list = read_csv_channel_list("ChannelList.txt")
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(create_streamer_viewer_dict(channel_list))
    loop.close()

    filename = datetime.datetime.utcnow().strftime("%m-%d-%Y_%H:%M")
    upload_to_mongo(data=data,Cname=filename)   
    # save_pickle(filename, "visualizingtwitchcommunities.appspot.com", data)

if __name__ == '__main__':
    main('data', 'context')