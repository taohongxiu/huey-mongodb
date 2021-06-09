from huey.api import Huey
from huey.constants import EmptyData
from huey.storage import BaseStorage
import time
from huey.utils import to_timestamp
from pymongo import MongoClient


class MongoStorage(BaseStorage):

    def __init__(self, name='huey', database=None, strict_fifo=False, **kwargs):
        self.database = database
        self._conn_kwargs = kwargs
        if strict_fifo:
            self.initialize_schema()
        super(MongoStorage, self).__init__(name)

    def initialize_schema(self):
        self.db.huey_schedule.createIndex({
            "queue": 1,
            "timestamp": 1
        }, {
            "name": "schedule_queue_timestamp",
            "unique": False,
            "sparse": True,
            "expireAfterSeconds": 3600
        })
        self.db.huey_queue.createIndex({
            "priority": -1,
            "_id": 1,
        }, {
            "name": "queue_priority_id",
            "unique": False,
            "sparse": True,
            "expireAfterSeconds": 3600
        })
        self.db.huey_kv.createIndex({
            "queue": 1,
            "key": 1,
        }, {
            "name": "kv_queue_key",
            "unique": True,
            "sparse": True,
            "expireAfterSeconds": 3600
        })

    @property
    def db(self):
        return self._create_connection().get_database(self.database) if self.database is not None else self._create_connection().get_default_database()

    def _create_connection(self):
        client = MongoClient(**self._conn_kwargs)
        return client

    def enqueue(self, data, priority=None):
        self.db.huey_queue.insert_one({
            "queue": self.name,
            "data": data,
            "datachange_lasttime": int(time.time() * 1000),
            "priority": priority or 0
        })

    def dequeue(self):
        result = self.db.huey_queue.find_one_and_delete({"queue": self.name}, sort=[("priority",  -1), ("datachange_lasttime", 1)])
        if result is not None:
            tid, data = result["_id"], result["data"]
            return data

    def queue_size(self):
        return self.db.huey_queue.count_documents({"queue": self.name})

    def enqueued_items(self, limit=None):
        results = self.db.huey_queue.find({"queue": self.name}, sort=[("priority",  -1), ("datachange_lasttime", 1)])
        if limit is not None:
            results = results.limit(limit)
        return [result["data"] for result in results]

    def flush_queue(self):
        self.db.huey_queue.delete_many({"queue": self.name})

    def add_to_schedule(self, data, ts, utc):
        self.db.huey_schedule.insert_one({
            "queue": self.name,
            "data": data,
            "timestamp": to_timestamp(ts),
            "datachange_lasttime": int(time.time() * 1000)
        })

    def read_schedule(self, ts):
        results = self.db.huey_schedule.find({"queue": self.name, "timestamp": {"$lte": to_timestamp(ts)}})
        id_list, data = [], []
        for result in results:
            id_list.append(result["_id"])
            data.append(result["data"])
        if id_list:
            self.db.huey_queue.delete_many({"_id": {"$in": id_list}})
        return data

    def schedule_size(self):
        return self.db.huey_schedule.count_documents({"queue": self.name})

    def scheduled_items(self, limit=None):
        results = self.db.huey_schedule.find({"queue": self.name}, sort=[("timestamp", 1)])
        if limit is not None:
            results = results.limit(limit)
        return [result["data"] for result in results]

    def flush_schedule(self):
        self.db.huey_schedule.delete_many({"queue": self.name})

    def put_data(self, key, value, is_result=False):
        self.db.huey_kv.find_one_and_update({
            "queue": self.name,
            "key": key
        }, {"$set": {
            "queue": self.name,
            "key": key,
            "value": value,
            "datachange_lasttime": int(time.time() * 1000),
        }}, upsert=True)

    def peek_data(self, key):
        res = self.db.huey_kv.find_one({"queue": self.name, "key": key})
        return res["value"] if res else EmptyData

    def pop_data(self, key):
        result = self.db.huey_kv.find_one_and_delete({"queue": self.name, "key": key})
        if result is not None:
            return result["value"]
        return EmptyData

    def has_data_for_key(self, key):
        return bool(self.db.huey_kv.count_documents({"queue": self.name, "key": key}))

    def put_if_empty(self, key, value):
        if self.has_data_for_key(key):
            return False
        self.db.huey_kv.insert_one({
            "queue": self.name,
            "key": key,
            "value": value,
            "datachange_lasttime": int(time.time() * 1000),
        })
        return True

    def result_store_size(self):
        return self.db.huey_kv.count_documents({"queue": self.name})

    def result_items(self):
        res = self.db.huey_kv.find({"queue": self.name})
        return dict((r["key"], r["value"]) for r in res)

    def flush_results(self):
        self.db.huey_kv.delete_many({"queue": self.name})


class MongoHuey(Huey):
    storage_class = MongoStorage
