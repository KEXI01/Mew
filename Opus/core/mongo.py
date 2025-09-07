from motor.motor_asyncio import AsyncIOMotorClient as _mongo_client_
from pymongo import MongoClient
import config
from ..logging import LOGGER
import asyncio

# MongoDB URIs
TEMP_MONGODB = "mongodb+srv://Billa20:uAJc5rGK18FzOiJz@cluster0.ul24roe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
ANOTHERDB = "mongodb+srv://spacebilla01:G7juW2Ig6SJLQAyG@cluster0.xwrt5nf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "MusicBot"
ANOTHERDB_NAME = "Opus"
BOT_USERNAME = "BILLAMUSIC2_BOT"

class MergedMongoDB:
    def __init__(self):
        try:
            # Initialize clients for TEMP_MONGODB
            self.temp_mongo_async = _mongo_client_(TEMP_MONGODB)
            self.temp_mongo_sync = MongoClient(TEMP_MONGODB)
            # Initialize clients for ANOTHERDB
            self.another_mongo_async = _mongo_client_(ANOTHERDB)
            self.another_mongo_sync = MongoClient(ANOTHERDB)

            # Database objects
            if config.MONGO_DB_URI is None:
                LOGGER(__name__).warning(
                    "No MongoDB URI found. Using Shukla Music database with bot username."
                )
                self.mongodb_temp = self.temp_mongo_async[BOT_USERNAME]
                self.pymongodb_temp = self.temp_mongo_sync[BOT_USERNAME]
            else:
                mongo_async = _mongo_client_(config.MONGO_DB_URI)
                mongo_sync = MongoClient(config.MONGO_DB_URI)
                self.mongodb_temp = mongo_async["Opus"]
                self.pymongodb_temp = mongo_sync["Opus"]

            # MusicBot database for TEMP_MONGODB
            self.mongodb_musicbot = self.temp_mongo_async[DB_NAME]
            self.pymongodb_musicbot = self.temp_mongo_sync[DB_NAME]
            # ANOTHERDB database
            self.mongodb_another = self.another_mongo_async[ANOTHERDB_NAME]
            self.pymongodb_another = self.another_mongo_sync[ANOTHERDB_NAME]

            # Log successful connection
            LOGGER(__name__).info("MongoDB connections initialized successfully.")
        except Exception as e:
            LOGGER(__name__).error(f"Failed to initialize MongoDB connections: {str(e)}")
            raise

    async def list_collections_async(self):
        """List all collections in both databases for debugging."""
        musicbot_collections = await self.mongodb_musicbot.list_collection_names()
        another_collections = await self.mongodb_another.list_collection_names()
        LOGGER(__name__).info(f"MusicBot DB collections: {musicbot_collections}")
        LOGGER(__name__).info(f"ANOTHERDB collections: {another_collections}")
        return {"MusicBot": musicbot_collections, "Opus": another_collections}

    async def get_collection_async(self, collection_name):
        """Return async collection, preferring MusicBot DB if collection exists, else ANOTHERDB."""
        try:
            if collection_name in await self.mongodb_musicbot.list_collection_names():
                LOGGER(__name__).info(f"Accessing collection {collection_name} from MusicBot DB")
                return self.mongodb_musicbot[collection_name]
            LOGGER(__name__).info(f"Accessing collection {collection_name} from ANOTHERDB")
            return self.mongodb_another[collection_name]
        except Exception as e:
            LOGGER(__name__).error(f"Error accessing collection {collection_name}: {str(e)}")
            raise

    def get_collection_sync(self, collection_name):
        """Return sync collection, preferring MusicBot DB if collection exists, else ANOTHERDB."""
        try:
            if collection_name in self.pymongodb_musicbot.list_collection_names():
                LOGGER(__name__).info(f"Accessing collection {collection_name} from MusicBot DB (sync)")
                return self.pymongodb_musicbot[collection_name]
            LOGGER(__name__).info(f"Accessing collection {collection_name} from ANOTHERDB (sync)")
            return self.pymongodb_another[collection_name]
        except Exception as e:
            LOGGER(__name__).error(f"Error accessing collection {collection_name}: {str(e)}")
            raise

    async def find_one_async(self, collection_name, query):
        """Find one document in the merged database."""
        try:
            collection = await self.get_collection_async(collection_name)
            result = await collection.find_one(query)
            LOGGER(__name__).info(f"Query {query} in {collection_name} returned: {result}")
            return result
        except Exception as e:
            LOGGER(__name__).error(f"Error querying {collection_name}: {str(e)}")
            return None

    def find_one_sync(self, collection_name, query):
        """Find one document synchronously in the merged database."""
        try:
            collection = self.get_collection_sync(collection_name)
            result = collection.find_one(query)
            LOGGER(__name__).info(f"Sync query {query} in {collection_name} returned: {result}")
            return result
        except Exception as e:
            LOGGER(__name__).error(f"Error querying {collection_name} (sync): {str(e)}")
            return None

    async def insert_one_async(self, collection_name, document):
        """Insert one document into MusicBot DB (primary)."""
        try:
            collection = self.mongodb_musicbot[collection_name]
            result = await collection.insert_one(document)
            LOGGER(__name__).info(f"Inserted document into {collection_name}: {result.inserted_id}")
            return result
        except Exception as e:
            LOGGER(__name__).error(f"Error inserting into {collection_name}: {str(e)}")
            raise

    def insert_one_sync(self, collection_name, document):
        """Insert one document synchronously into MusicBot DB (primary)."""
        try:
            collection = self.pymongodb_musicbot[collection_name]
            result = collection.insert_one(document)
            LOGGER(__name__).info(f"Inserted document into {collection_name} (sync): {result.inserted_id}")
            return result
        except Exception as e:
            LOGGER(__name__).error(f"Error inserting into {collection_name} (sync): {str(e)}")
            raise

    async def load_all_data_async(self, collection_name):
        """Load all documents from a collection in both databases."""
        try:
            musicbot_collection = self.mongodb_musicbot[collection_name]
            another_collection = self.mongodb_another[collection_name]
            musicbot_data = await musicbot_collection.find().to_list(None)
            another_data = await another_collection.find().to_list(None)
            LOGGER(__name__).info(f"Loaded {len(musicbot_data)} documents from MusicBot.{collection_name}")
            LOGGER(__name__).info(f"Loaded {len(another_data)} documents from Opus.{collection_name}")
            return {"MusicBot": musicbot_data, "Opus": another_data}
        except Exception as e:
            LOGGER(__name__).error(f"Error loading data from {collection_name}: {str(e)}")
            return {"MusicBot": [], "Opus": []}

# Instantiate the database
db = MergedMongoDB()

# Export mongodb for async operations (MusicBot DB as primary)
mongodb = db.mongodb_musicbot

# Export pymongodb for sync operations (MusicBot DB as primary)
pymongodb = db.pymongodb_musicbot
