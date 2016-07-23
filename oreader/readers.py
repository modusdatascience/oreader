import csv
import warnings
import traceback
from sqlalchemy.sql.expression import and_, or_, select
import time
from sqlalchemy.sql.elements import Null, True_

class ReaderCollection(object):#Done
    def __init__(self, klasses, config):
        '''
        klasses : (dict) contains name:class pairs
        '''
        self.readers = dict([(name,klass.reader_class(config)(klass,config)) for name, klass in klasses.iteritems()])
        self._peeks = {}
        for name, reader in self.readers.iteritems():
            try:
                self._peeks[name] = reader.next()
            except StopIteration: # OK
                self.readers[name].close()
        self.update()
        
    def report(self):
        return dict([(name,reader.report()) for name,reader in self.readers.iteritems()])
        
    def update(self):#TODO: This could be made more efficient by maintaining a sort.
        self._peek = None
        if self._peeks:
            for k, v in self._peeks.iteritems():
                if v is not None:
                    if self._peek is None:
                        self._peek = k
                    if v < self._peeks[self._peek]:
                        self._peek = k
        
    def peek(self):
        if self._peek is None:
            return None
        return self._peeks[self._peek]
    
    def next(self):
        if self._peek is None:
            raise StopIteration
        result = self._peeks[self._peek]
        if result is None:
            raise StopIteration
        try:
            self._peeks[self._peek] = self.readers[self._peek].next()
        except StopIteration:
            del self._peeks[self._peek]
            self.readers[self._peek].close()
        self.update()
        return result
    
    def __iter__(self):
        return self
    
    def close(self):
        for reader in self.readers.values():
            reader.close()

class Reader(object):
    def __init__(self, klass, config):
        self.klass = klass
        self.count = 0
        
    def next(self):
        result = self.peek()
        if result is None:
            raise StopIteration
        while True:
            try:
                self.count += 1
                self.update()
                break
            except:
                warnings.warn('Problem reading %s object at position %d.  Skipping.' % (self.klass.__name__, self.count))
                traceback.print_exc()
                result = self.peek()
                if result is None:
                    raise StopIteration
        return result
    
    def peek(self):
        return self._peek
    
    def __iter__(self):
        return self

    def report(self):
        raise NotImplementedError
    
    def update(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError

class SimpleReaderConfig(object):
    pass

class TupleSimpleReaderConfig(SimpleReaderConfig):
    def translate(self, reader, raw):
        if raw is None:
            return None
        return reader.klass(**dict(zip([col.name for col in reader.klass.columns], raw)))

def vector_greater_than(columns, values):
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

class SqaState(object):
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
        
class SqaConfig(TupleSimpleReaderConfig):
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
        return [SqaState(self.expression, self.engine, reader.klass, self.starter, 
                         self.filter, self.n_tries, self.wait, self.warn_every, 
                         self.yield_per)]
    
class CsvConfig(TupleSimpleReaderConfig):
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

class SimpleReader(Reader):
    def __init__(self, klass, config):
        super(SimpleReader, self).__init__(klass, config)
        self.config = config[klass]
        self.sources = self.config.get_sources(self)
        self.source_index = -1
        self.current_source = iter([])
        self.update()
    
    def next_source(self):
        if self.source_index >= 0:
            self.config.stop_source(self, self.current_source)
        self.source_index += 1
        try:
            self.current_source = self.config.start_source(self, self.sources[self.source_index])
        except IndexError:
            raise StopIteration # OK
    
    def translate(self, raw):
        return self.config.translate(self, raw)
            
    def update(self):
        try:
            self._peek = self.translate(self.current_source.next())
        except StopIteration: # OK
            try:
                self.next_source()
                self._peek = self.translate(self.current_source.next())
            except StopIteration: # OK
                self._peek = None
#                 print self.klass, None
#         print self.klass, self.peek(), self.peek().identity_key()
    
    def report(self):
        return {self.klass.__name__: self.config}
    
    def close(self):
        self.config.stop_source(self, self.current_source)
        
class CompoundReader(Reader):#Done
    '''Contains a SimpleReader and a ReaderCollection.'''
    
    def __init__(self, klass, config):
        super(CompoundReader, self).__init__(klass, config)
        self.simple_reader = SimpleReader(klass, config)
        self.relatives = dict([(name,group[0]) for name, group in klass.relationships.iteritems()])
        self.readers = ReaderCollection(self.relatives, config)
        self.update()
    
    def report(self):
        return {'base':self.simple_reader.report(),
                'relatives':self.readers.report()}
    
    def update(self):
        if self.simple_reader.peek() is None:
            self._peek = None
            return
        self._peek = self.simple_reader.next()
        while self.readers.peek() is not None and self.readers.peek().container_key() < self._peek.identity_key():
#             warnings.warn('Orphaned %s with key %s.' % (self.readers.peek().__class__.__name__, str(self.readers.peek().identity_key())))
            self.readers.next()
        while self.readers.peek() is not None and self.readers.peek().container_key() == self._peek.identity_key():
            found = False
            item = self.readers.next()
            for name, group in self.klass.relationships.iteritems():
                if isinstance(item, group[0]):
                    if group[1]:
                        getattr(self._peek, name).append(item)
                    else:
                        setattr(self._peek, name, item)
                    found = True
                    break
            if not found:
                raise ValueError('The ReaderCollection returned a value that doesn\'t seem to fit into any known relationships.')
            
    def close(self):
        self.simple_reader.close()
        self.readers.close()
        
class ImplicitReader(Reader):#Done
    '''Contains a ReaderCollection.'''
    def __init__(self, klass, config):
        super(ImplicitReader, self).__init__(klass, config)
        self.relatives = dict([(name,group[0]) for name, group in klass.relationships.iteritems()])
        self.readers = ReaderCollection(self.relatives, config)
        self._peek = 1 #An object that is not None
        self.update()
        
    def report(self):
        return {'relatives': self.readers.report()}
    
    def update(self):
        if self.readers.peek() is None:
            self._peek = None
            return
        current_key = self.readers.peek().container_key()
        try:
            self._peek = self.klass(**current_key)
        except:
            raise
        while self.readers.peek() is not None and self.readers.peek().container_key() == current_key:
            found = False
            item = self.readers.next()
            for name, group in self.klass.relationships.iteritems():
                if isinstance(item, group[0]):
                    if group[1]:
                        getattr(self._peek, name).append(item)
                    else:
                        setattr(self._peek, name, item)
                    found = True
                    break
            if not found:
                raise ValueError('The ReaderCollection returned a value that doesn\'t seem to fit into any known relationships.')

    def close(self):
        self.readers.close()
    

class PolymorphicReader(Reader):#Done
    '''Contains a ReaderCollection'''
    def __init__(self, klass, config):
        super(PolymorphicReader, self).__init__(klass, config)
        klasses = {}
        stack = [klass]
        while stack:
            item = stack.pop()
            stack.extend(item.subs)
            if item.concrete():
                klasses[item.__name__] = item
        self.readers = ReaderCollection(klasses, config)
        self.update()
    
    def report(self):
        return {'subclasses':self.readers.report()}
    
    def update(self):
        try:
            self._peek = self.readers.next()
        except StopIteration: # OK
            self._peek = None
    
    def close(self):
        self.readers.close()
        
   