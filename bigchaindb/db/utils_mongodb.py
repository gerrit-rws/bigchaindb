"""Utils to initialize and drop the MongoDB database."""

import logging
from pymongo import MongoClient

import bigchaindb
from bigchaindb import exceptions
from bigchaindb.db.utils import get_database_name


logger = logging.getLogger(__name__)


def get_conn():
    """"Get the connection to the database."""
    return MongoClient(host=bigchaindb.config['database']['host'],
                       port=bigchaindb.config['database']['port'])


def create_database(conn, dbname):
    # left here for reference. With mongodb we don't need to create a database.
    # When a collection is created in a database the database will be created if it does not exists.
    # Don't think we can create a database without a collection
    if dbname in conn.database_names():
        raise exceptions.DatabaseAlreadyExists('Database `{}` already exists'.format(dbname))

    logger.info('Create database `%s`.', dbname)


def create_table(conn, dbname, table_name):
    # In mongodb a table is called a collection
    logger.info('Create `%s` table.', table_name)
    # create the table
    conn[dbname].create_collection(table_name, capped=True, size=100000)


def create_bigchain_secondary_index(conn, dbname):
    logger.info('Create `bigchain` secondary index.')
    # not creating any secondary indexes for now


def create_backlog_secondary_index(conn, dbname):
    logger.info('Create `backlog` secondary index.')
    # not creating any secondary indexes for now


def create_votes_secondary_index(conn, dbname):
    logger.info('Create `votes` secondary index.')
    # not creating any secondary indexes for now


def init():
    # Try to access the keypair, throws an exception if it does not exist
    b = bigchaindb.Bigchain()

    conn = get_conn()
    dbname = get_database_name()
    create_database(conn, dbname)

    table_names = ['bigchain', 'backlog', 'votes']
    for table_name in table_names:
        create_table(conn, dbname, table_name)
    create_bigchain_secondary_index(conn, dbname)
    create_backlog_secondary_index(conn, dbname)
    create_votes_secondary_index(conn, dbname)

    logger.info('Create genesis block.')
    b.create_genesis_block()
    logger.info('Done, have fun!')


def drop(assume_yes=False):
    conn = get_conn()
    dbname = bigchaindb.config['database']['name']

    if assume_yes:
        response = 'y'
    else:
        response = input('Do you want to drop `{}` database? [y/n]: '.format(dbname))

    if response == 'y':
        logger.info('Drop database `%s`', dbname)
        # does not raise an exception if the database does not exist
        conn.drop_database(dbname)
        logger.info('Done.')
    else:
        logger.info('Drop aborted')
