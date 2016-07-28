import csv
from sqlalchemy.sql.elements import True_
from sqlalchemy.sql.expression import select, and_
from oreader.util import vector_greater_than
import warnings
import time

class SimpleReaderConfig(object):
    pass

class TupleSimpleReaderConfig(SimpleReaderConfig):
    def translate(self, reader, raw):
        if raw is None:
            return None
        return reader.klass(**dict(zip([col.name for col in reader.klass.columns], raw)))

class CsvReaderConfig(TupleSimpleReaderConfig):
    def __init__(self, files, header, csv_config={}):
        self.files = files
        self.header = header
        self.csv_config = csv_config
    
    def start_source(self, reader, filename):
        result = csv.reader(open(filename, 'r'), **self.csv_config)
        if self.header:
            result.next()
        return result
    
    def stop_source(self, reader, source):
        source.close()
        
    def get_sources(self, reader):
        return [src for src in self.files]

class SqaReaderState(object):
    def __init__(self, table, engine, klass, starter, filter=True_(), n_tries=float('inf'), wait=0.1, warn_every=10,
                 yield_per=None):
        self.table = table
        self.engine = engine
        self.klass = klass
        self.starter = starter
        self.filter = filter
        self.sort_key = klass.sort_column_names
        self.sort_index = klass.sort_column_numbers
        self.n_tries = n_tries
        self.wait = wait
        self.warn_every = warn_every
        self.last_result = None
        self.uncalled = True
        self.yield_per = yield_per
        
    def __iter__(self):
        return self
    
    def fresh_result_proxy(self):
        self.result_proxy = None
        if self.last_result is None:
            try:
                expr = select(self.table.columns).order_by(*[self.table.columns[nm] for nm in self.sort_key])
#                 print self.filter
#                 print and_(self.starter, self.filter)
                if self.starter is not None:
                    expr = expr.where(and_(self.starter, self.filter))
                else:
                    expr = expr.where(self.filter)
            except:
#                 print self.klass
#                 print self.last_result
#                 print [col.name for col in self.table.columns]
                raise
#             self.expression.order_by(self.expression)
        else:
            try:
                where_clause = vector_greater_than([self.table.columns[nm] for nm in self.sort_key], \
                                                  [self.last_result[n] for n in self.sort_index])
#                 print where_clause
#                 print {self.sort_key[i]: self.last_result[n] for i, n in enumerate(self.sort_index)}
#                 print and_(where_clause, self.filter)
                expr = (select(self.table.columns).order_by(*[self.table.columns[nm] for nm in self.sort_key]) \
                       .where(and_(where_clause, self.filter)))
#                 tuple_(*[self.expression.columns[self.sort_key[i]] for i, n in enumerate(self.sort_index)]) > \
#                             tuple_(*[self.last_result[n] for n in self.sort_index]))
#                        for i, n in enumerate(self.sort_index)])))
            except:
#                 print self.klass
#                 print self.last_result.values()
#                 print [col.name for col in self.table.columns]
                raise
        
        # yield_per is implemented using limit instead of using yield_per.  This is more efficient in terms of
        # memory for backends that don't support streaming results.
        if self.yield_per is not None:
            expr = expr.limit(self.yield_per)
        
        try:
#             print expr
            self.result_proxy = self.engine.execute(expr)
#             if self.yield_per is not None:
#                 self.result_proxy = self.result_proxy.yield_per(self.yield_per)
        except:
#             warnings.warn('Failure to execute query on %s reader.  Query:\n%s' % (self.klass.__name__, str(expr)))
#             traceback.print_exc()
            pass
            
    def next(self):
        attempt = 0
        stop_count = 0
        if self.uncalled:
            self.fresh_result_proxy()
            self.uncalled = False
        while attempt < self.n_tries:
            try:
                result = self.result_proxy.fetchone()
                # If using yield_per, the end of a result proxy may not be the end of the relevant
                # results because yield_per is implemented using limits (instead of using sqlalchemy's
                # yield_per).  This way is more memory efficient for more different backends and reduces 
                # initial loading time.
                if self.yield_per is not None and result is None and stop_count == 0:
                    stop_count += 1
                    self.fresh_result_proxy()
                else:
                    if result is not None and len(result._row) == 0:
                        print 'EMPTY ROW:', self.klass.__name__, 'after:', self.last_result 
                        raise ValueError
                    self.last_result = result
                    return result
            except StopIteration:
                raise
            except:
                if (attempt + 1) % self.warn_every == 0:
                    warnings.warn('Lost connection for %s reader.  Trying to re-establish.' % self.klass.__name__)
                time.sleep(self.wait)
                self.fresh_result_proxy()
                attempt += 1
    
    def close(self):
        self.result_proxy.close()
        
class SqaReaderConfig(TupleSimpleReaderConfig):
    def __init__(self, expression, engine, starter=None, filter=True_(), n_tries=float('inf'), wait=0.1, warn_every=10,
                 yield_per=None):
        self.expression = expression
        self.engine = engine
        self.starter = starter
        self.filter = filter
        self.n_tries = n_tries
        self.wait = wait
        self.warn_every = warn_every
        self.yield_per = yield_per
        
    def start_source(self, reader, source):
        return source
    
    def stop_source(self, reader, source):
        source.close()
        
    def get_sources(self, reader):
        return [SqaReaderState(self.expression, self.engine, reader.klass, self.starter, 
                         self.filter, self.n_tries, self.wait, self.warn_every, 
                         self.yield_per)]
    

