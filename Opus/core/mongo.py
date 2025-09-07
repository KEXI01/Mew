from motor.motor_asyncio import AsyncIOMotorClient as _mongo_client_
from pymongo import MongoClient
from pyrogram import Client
import config
from ..logging import LOGGER

TEMP_MONGODB = "mongodb+srv://Billa20:uAJc5rGK18FzOiJz@cluster0.ul24roe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
ANOTHERDB = "mongodb+srv://spacebilla01:G7juW2Ig6SJLQAyG@cluster0.xwrt5nf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "MusicBot"
ANOTHERDB_NAME = "Opus" 
BOT_USERNAME = "BILLAMUSIC2_BOT"  

class MergedMongoDB:
    def __init__(self):
    
        self.temp_mongo_async = _mongo_client_(TEMP_MONGODB)
        self.temp_mongo_sync = MongoClient(TEMP_MONGODB)

        self.another_mongo_async = _mongo_client_(ANOTHERDB)
        self.another_mongo_sync = MongoClient(ANOTHERDB)


        if config.MONGO_DB_URI is None:
            LOGGER(__name__).warning(
                "ɴᴏ ᴍᴏɴɢᴏᴅʙ ᴜʀɪ ꜰᴏᴜɴᴅ. ᴜꜱɪɴɢ ᴍᴇᴡ ᴍᴜꜱɪᴄ ᴅᴀᴛᴀʙᴀꜱᴇ ᴡɪᴛʜ ʙᴏᴛ ᴜꜱᴇʀɴᴀᴍᴇ."
            )

            self.mongodb_temp = self.temp_mongo_async[BOT_USERNAME]
            self.pymongodb_temp = self.temp_mongo_sync[BOT_USERNAME]
        else:

            mongo_async = _mongo_client_(config.MONGO_DB_URI)
            mongo_sync = MongoClient(config.MONGO_DB_URI)
            self.mongodb_temp = mongo_async["Opus"]
            self.pymongodb_temp = mongo_sync["Opus"]


        self.mongodb_musicbot = self.temp_mongo_async[DB_NAME]
        self.pymongodb_musicbot = self.temp_mongo_sync[DB_NAME]
        
        self.mongodb_another = self.another_mongo_async[ANOTHERDB_NAME]
        self.pymongodb_another = self.another_mongo_sync[ANOTHERDB_NAME]

    async def get_collection_async(self, collection_name):
        if collection_name in await self.mongodb_musicbot.list_collection_names():
            return self.mongodb_musicbot[collection_name]
        return self.mongodb_another[collection_name]

    def get_collection_sync(self, collection_name):
        if collection_name in self.pymongodb_musicbot.list_collection_names():
            return self.pymongodb_musicbot[collection_name]
        return self.pymongodb_another[collection_name]

    async def find_one_async(self, collection_name, query):
        collection = await self.get_collection_async(collection_name)
        return await collection.find_one(query)

    def find_one_sync(self, collection_name, query):
        collection = self.get_collection_sync(collection_name)
        return collection.find_one(query)

    async def insert_one_async(self, collection_name, document):
        collection = self.mongodb_musicbot[collection_name]
        return await collection.insert_one(document)

    def insert_one_sync(self, collection_name, document):
        collection = self.pymongodb_musicbot[collection_name]
        return collection.insert_one(document)

db = MergedMongoDB()
