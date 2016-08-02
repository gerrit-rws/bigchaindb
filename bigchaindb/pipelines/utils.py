"""Utility classes and functions to work with the pipelines."""


import rethinkdb as r
import pymongo
from multipipes import Node

from bigchaindb import Bigchain


class ChangeFeed(Node):
    """This class wraps a RethinkDB changefeed adding a `prefeed`.

    It extends the ``multipipes::Node`` class to make it pluggable in
    other Pipelines instances, and it makes usage of ``self.outqueue``
    to output the data.

    A changefeed is a real time feed on inserts, updates, and deletes, and
    it's volatile. This class is a helper to create changefeeds. Moreover
    it provides a way to specify a `prefeed`, that is a set of data (iterable)
    to output before the actual changefeed.
    """

    INSERT = 'insert'
    DELETE = 'delete'
    UPDATE = 'update'

    def __init__(self, table, operation, prefeed=None):
        """Create a new RethinkDB ChangeFeed.

        Args:
            table (str): name of the table to listen to for changes.
            operation (str): can be ChangeFeed.INSERT, ChangeFeed.DELETE, or
                ChangeFeed.UPDATE.
            prefeed (iterable): whatever set of data you want to be published
                first.
        """

        super().__init__(name='changefeed')
        self.prefeed = prefeed if prefeed else []
        self.table = table
        self.operation = operation
        self.bigchain = Bigchain()

    def run_forever(self):
        for element in self.prefeed:
            self.outqueue.put(element)

        for change in r.table(self.table).changes().run(self.bigchain.conn):

            is_insert = change['old_val'] is None
            is_delete = change['new_val'] is None
            is_update = not is_insert and not is_delete

            if is_insert and self.operation == ChangeFeed.INSERT:
                self.outqueue.put(change['new_val'])
            elif is_delete and self.operation == ChangeFeed.DELETE:
                self.outqueue.put(change['old_val'])
            elif is_update and self.operation == ChangeFeed.UPDATE:
                self.outqueue.put(change)


class ChangeFeedMongoDB(Node):
    """This class wraps a RethinkDB changefeed adding a `prefeed`.

    It extends the ``multipipes::Node`` class to make it pluggable in
    other Pipelines instances, and it makes usage of ``self.outqueue``
    to output the data.

    A changefeed is a real time feed on inserts, updates, and deletes, and
    it's volatile. This class is a helper to create changefeeds. Moreover
    it provides a way to specify a `prefeed`, that is a set of data (iterable)
    to output before the actual changefeed.
    """

    INSERT = 'insert'
    DELETE = 'delete'
    UPDATE = 'update'

    def __init__(self, table, operation, prefeed=None):
        """Create a new MongoDB tailable cursor.

        The only operation supported is INSERT

        Args:
            table (str): name of the table to listen to for changes.
            operation (str): can be ChangeFeed.INSERT, ChangeFeed.DELETE, or
                ChangeFeed.UPDATE.
            prefeed (iterable): whatever set of data you want to be published
                first.
        """

        super().__init__(name='changefeed')
        self.prefeed = prefeed if prefeed else []
        self.table = table
        self.operation = operation
        self.bigchain = Bigchain()
        # connection to mongodb
        self.conn = pymongo.MongoClient(host=self.bigchain.host, port=self.bigchain.port)
        # namespace
        self.ns = '{}.{}'.format(self.bigchain.dbname, self.table)

    def run_forever(self):
        for element in self.prefeed:
            self.outqueue.put(element)

        cursor = self.conn.local.oplog.rs.find({'ns': self.ns}, cursor_type=pymongo.CursorType.TAILABLE_AWAIT)
        while cursor.alive:
            try:
                record = cursor.next()
            except StopIteration:
                print('mongodb cursor waiting')
            else:
                is_insert = record['op'] == 'i'
                is_delete = record['op'] == 'd'
                is_update = record['op'] == 'u'

                if is_insert and self.operation == ChangeFeed.INSERT:
                    self.outqueue.put(record['o'])
                elif is_delete and self.operation == ChangeFeed.DELETE:
                    # on delete it only returns the id of the document
                    self.outqueue.put(record['o'])
                elif is_update and self.operation == ChangeFeed.UPDATE:
                    # not sure what to do here. Lets return the entire operation
                    self.outqueue.put(record)
                print(record)
