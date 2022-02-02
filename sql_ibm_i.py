import sqlalchemy as sa

# see documentation for available connection options
# pass connection options in url query string, eg.
# engine = sa.create_engine("ibmi://user:pass@host?autocommit=true&timeout=10"
# find usage of create_engine database urls here
# https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
# this is the base connection which connects to *LOCAL on the host

engine = sa.create_engine("WRSERV://user:@host")

cnxn = engine.connect()
metadata = sa.MetaData()
table = sa.Table('table_name', metadata, autoload=True, autoload_with=engine)

query = sa.select([table])

result = cnxn.execute(query)
result = result.fetchall()

# print first entry
print(result[0])