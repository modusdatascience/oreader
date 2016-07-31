from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.expression import or_, and_

def vector_greater_than(columns, values):
    '''
    Construct a lexicographic vector inequality condition from a combination of 
    logical operators so that it will work in any sql database server.
    '''
    assert len(columns) == len(values)
    if values[0] is None or values[0] is Null or isinstance(values[0], Null):
        if len(columns) > 1:
            return or_(columns[0] != None, 
                       and_(columns[0] == None, 
                            vector_greater_than(columns[1:], values[1:])))
        else:
            return columns[0] != None
    else:
        if len(columns) > 1:
            return or_(columns[0] > values[0], 
                       and_(columns[0] == values[0], 
                            vector_greater_than(columns[1:], values[1:])))
        else:
            return columns[0] > values[0]

class DataSourceError(Exception):
    def __init__(self, error):
        self.error = error
