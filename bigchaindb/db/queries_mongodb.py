# TODO:
# mongodb primary key is `_id` while with rethinkdb its `id`
# we need to set document['_id'] = document['id'] before writing to the database, maybe?
# this would prevent duplicated documents


def get_blocks_count(conn):
    return conn.bigchain.count()


def write_block(conn, block, durability=None):
    return conn.bigchain.insert_one(block)


def write_backlog(conn, signed_transaction, durability=None):
    return conn.backlog.insert_one(signed_transaction)
