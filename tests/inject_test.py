from pypika import Query, Table, Field
t = Table('the_table')
unsafe = "John\'; DROP TABLE the_table --"
a =  Query.from_(t).select('*')
q = Query.from_(t).select('*').where(t.name == unsafe)
print(str(q))