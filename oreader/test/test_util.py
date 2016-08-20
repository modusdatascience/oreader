from sqlalchemy.sql.schema import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import String, Integer
from nose.tools import assert_equal
from sqlalchemy.engine import create_engine
import names
import random
from sqlalchemy.sql.expression import select
from oreader.util import vector_greater_than


def test_vector_greater_than():
    metadata = MetaData()
    table = Table('people', metadata,
                  Column('id', Integer, primary_key=True), 
                  Column('first_name', String),
                  Column('middle_name', String),
                  Column('last_name', String),
                  Column('blood_type', String))
    def random_person(idx):
        first = names.get_first_name()
        last = names.get_last_name()
        middle = random.choice([names.get_first_name, names.get_last_name, lambda: None])()
        blood_type = random.choice(['A', 'A', 'B', 'B', 'O', 'O', 'O', 'O', 'AB'])
        return {'id': idx,
                'first_name': first,
                'middle_name': middle,
                'last_name': last,
                'blood_type': blood_type
                }
    engine = create_engine('sqlite:///:memory:', echo=False)
    metadata.create_all(engine)
    def compare_results(compa, cols, vals):
        res = set([row['id'] for row in engine.execute(select(table.columns).where(compa))])
        all_ = [row for row in engine.execute(select(table.columns))]
        cor = set()
        for row in all_:
            if tuple(row[col.name] for col in cols) > vals:
                cor.add(row['id'])
        assert_equal(res, cor)
    
    for i in range(1000):
        engine.execute(table.insert(random_person(i)))
    
    
    col_tuples = [(table.columns['id'],),
                  (table.columns['blood_type'], table.columns['id']),
                  (table.columns['blood_type'], table.columns['middle_name'], table.columns['id']),
                  (table.columns['blood_type'], table.columns['id'], table.columns['middle_name']),
                  (table.columns['middle_name'], table.columns['blood_type'], table.columns['id']),]
    val_tuples = [(5,),
                  ('AB', 500),
                  ('B', None, 500),
                  ('B', 500, None),
                  (None, 'B', 500)]
    for cols, vals in zip(col_tuples, val_tuples):
        compare_results(vector_greater_than(cols, vals), cols, vals)

if __name__ == '__main__':
    # This code will run the test in this file.'
    import sys
    import nose
    module_name = sys.modules[__name__].__file__

    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s','-v'])
