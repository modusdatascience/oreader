import csv
from sqlalchemy.sql.elements import True_
from sqlalchemy.sql.expression import select, and_
from oreader.util import vector_greater_than
import warnings
import time
from oreader.readers import DataSourceError

class SimpleReaderConfig(object):
    pass

class TupleSimpleReaderConfig(SimpleReaderConfig):
    def translate(self, reader, raw):
        if raw is None:
            return None
        return reader.klass(**dict(zip([col.name for col in reader.klass.columns], raw)))

class CsvReaderConfig(TupleSimpleReaderConfig):
    def __init__(self, files, header, csv_config={}, opener=open):
        self.files = files
        self.header = header
        self.csv_config = csv_config
        self.opener = opener
        
    def start_source(self, reader, filename):
        result = csv.reader(self.opener(filename, 'r'), **self.csv_config)
        if self.header:
            result.next()
        return result
    
    def stop_source(self, reader, source):
        source.close()
        
    def get_sources(self, reader):
        return [src for src in self.files]

class SqaReaderState(object):
    def __init__(self, table, engine, klass, starter, filter=True_(), n_tries=float('inf'), wait=0.1, warn_every=10,
                 limit_per=None, yield_per=None):
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
        self.limit_per = limit_per
        self.yield_per = yield_per
        self.result_proxy = None
        
    def __iter__(self):
        return self
    
    def fresh_result_proxy(self):
        self.result_proxy = None
        if self.last_result is None:
            try:
                expr = select(self.table.columns).order_by(*[self.table.columns[nm] for nm in self.sort_key])
                if self.starter is not None:
                    expr = expr.where(and_(self.starter, self.filter))
                else:
                    expr = expr.where(self.filter)
            except:
                raise
        else:
            try:
                where_clause = vector_greater_than([self.table.columns[nm] for nm in self.sort_key], \
                                                  [self.last_result[n] for n in self.sort_index])
                expr = (select(self.table.columns).order_by(*[self.table.columns[nm] for nm in self.sort_key]) \
                       .where(and_(where_clause, self.filter)))
            except:
                raise
        
        if self.limit_per is not None:
            expr = expr.limit(self.limit_per)
        
        self.result_proxy = self.engine.execute(expr)
        
        if self.yield_per is not None:
            self.result_proxy = self.result_proxy.yield_per(self.yield_per)
            
    def next(self):
        attempt = 0
        stop_count = 0
        while True:
            try:
                result = self.result_proxy.fetchone()
                # If using limit_per, the end of a result proxy may not be the end of the relevant
                # results.  This way is more memory efficient for more different backends and reduces 
                # initial loading time compared to yield_per, but may take longer in the end if a lot of 
                # long running queries get repeated.
                if self.limit_per is not None and result is None and stop_count == 0:
                    stop_count += 1
                    self.fresh_result_proxy()
                else:
                    if result is not None and len(result._row) == 0:
                        print 'EMPTY ROW:', self.klass.__name__, 'after:', self.last_result 
                        raise ValueError()
                    self.last_result = result
                    return result
            except StopIteration:
                raise
            except Exception as e:
                if (attempt + 1) % self.warn_every == 0:
                    warnings.warn('Lost connection for %s reader.  Trying to re-establish.' % self.klass.__name__)
                if attempt > self.n_tries:
                    raise DataSourceError(e)
                attempt += 1
                time.sleep(self.wait)
                while True:
                    try:
                        self.fresh_result_proxy()
                        break
                    except Exception as e:
                        if attempt > self.n_tries:
                            raise DataSourceError(e)
                        attempt += 1
    
    def close(self):
        self.result_proxy.close()
        
class SqaReaderConfig(TupleSimpleReaderConfig):
    def __init__(self, expression, engine, starter=None, filter=True_(), n_tries=float('inf'), wait=0.1, warn_every=10,
                 limit_per=None, yield_per=None):
        self.expression = expression
        self.engine = engine
        self.starter = starter
        self.filter = filter
        self.n_tries = n_tries
        self.wait = wait
        self.warn_every = warn_every
        self.limit_per = limit_per
        self.yield_per = yield_per
        
    def start_source(self, reader, source):
        return source
    
    def stop_source(self, reader, source):
        source.close()
        
    def get_sources(self, reader):
        return [SqaReaderState(self.expression, self.engine, reader.klass, self.starter, 
                         self.filter, self.n_tries, self.wait, self.warn_every, 
                         limit_per=self.limit_per, yield_per=self.yield_per)]
    

