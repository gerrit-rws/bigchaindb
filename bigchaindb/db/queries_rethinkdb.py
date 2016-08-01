import rethinkdb as r
import rapidjson


def get_blocks_count(conn):
    return r.table('bigchain', read_mode='majority').count().run(conn)


def write_block(conn, block, durability):
    block_serialized = rapidjson.dumps(block)
    return r.table('bigchain').insert(r.json(block_serialized), durability=durability).run(conn)


def write_backlog(conn, signed_transaction, durability):
    return r.table('backlog').insert(signed_transaction, durability=durability).run(conn)
