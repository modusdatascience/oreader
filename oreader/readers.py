import warnings
import traceback
from six import Iterator

class DataSourceError(Exception):
    '''
    The Reader classes normally skip over exceptions with a warning.  However, when readers encounter 
    DataSourceErrors they will instead raise the error which the DataSourceError is wrapping.  This 
    mechanism allows data sources (such as provided by the SqaReaderConfig, to raise exceptions that will 
    be propagated past the readers.
    '''
    def __init__(self, error):
        self.error = error

class ReaderCollection(Iterator):#Done
    def __init__(self, klasses, config):
        '''
        klasses : (dict) contains name:class pairs
        '''
        self.readers = dict([(name,klass.reader_class(config)(klass,config)) for name, klass in klasses.items()])
        self._peeks = {}
        for name, reader in self.readers.items():
            try:
                self._peeks[name] = next(reader)
            except StopIteration: # OK
                self.readers[name].close()
        self.update()
        
    def report(self):
        return dict([(name,reader.report()) for name,reader in self.readers.items()])
        
    def update(self):#TODO: This could be made more efficient by maintaining a sort.
        self._peek = None
        if self._peeks:
            for k, v in self._peeks.items():
                if v is not None:
                    if self._peek is None:
                        self._peek = k
                    if v.sort_key() < self._peeks[self._peek].sort_key():
                        self._peek = k
        
    def peek(self):
        if self._peek is None:
            return None
        return self._peeks[self._peek]
    
    def __next__(self):
        if self._peek is None:
            raise StopIteration
        result = self._peeks[self._peek]
        if result is None:
            raise StopIteration
        try:
            self._peeks[self._peek] = next(self.readers[self._peek])
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

class Reader(Iterator):
    def __init__(self, klass, config):
        self.klass = klass
        self.count = 0
        
    def __next__(self):
        
        result = self.peek()
        
        if result is None:
            raise StopIteration
        while True:
            try:
                self.count += 1
                self.update()
                break
            except DataSourceError as e:
                raise e.error
            except:
                warnings.warn('Problem reading %s object at position %d.  Skipping.' % (self.klass.__name__, self.count))
                traceback.print_exc()
                result = self.peek()
                if result is None:
                    raise StopIteration
#         print result
#         print result.__dict__
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
        except DataSourceError as e:
            raise e.error
        except IndexError:
            raise StopIteration # OK
    
    def translate(self, raw):
        return self.config.translate(self, raw)
            
    def update(self):
        try:
            raw = next(self.current_source)
        except StopIteration: # OK
            try:
                self.next_source()
                raw = next(self.current_source)
            except StopIteration: # OK
                raw = None
        if raw is None:
            self._peek = None
        else:
            try:
                self._peek = self.translate(raw)
            except Exception as e:
                traceback.print_exc()
                raise ValueError('Failed to translate. ' + '\n' + 
                                 'Class: ' + self.klass.__name__ + '\n' + 
                                 'Exception: ' +  repr(e))    
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
        self.relatives = dict([(name,group[0]) for name, group in klass.relationships.items()])
        self.readers = ReaderCollection(self.relatives, config)
        self.update()
    
    def report(self):
        return {'base':self.simple_reader.report(),
                'relatives':self.readers.report()}
    
    def update(self):
        if self.simple_reader.peek() is None:
            self._peek = None
            return
        self._peek = next(self.simple_reader)
        while self.readers.peek() is not None and self.readers.peek().container_key() < self._peek.identity_key():
            warnings.warn('Orphaned %s with key %s.' % (self.readers.peek().__class__.__name__, str(self.readers.peek().identity_key())))
            next(self.readers)
        while self.readers.peek() is not None and self.readers.peek().container_key() == self._peek.identity_key():
            found = False
            item = next(self.readers)
            for name, group in self.klass.relationships.items():
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
        self.relatives = dict([(name,group[0]) for name, group in klass.relationships.items()])
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
        self._peek = self.klass(**self.klass.translate_identity_key(current_key))
        while self.readers.peek() is not None and self.readers.peek().container_key() == current_key:
            found = False
            item = next(self.readers)
            for name, group in self.klass.relationships.items():
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
            stack.extend(item.__subclasses__())
            if item in config:
                klasses[item.__name__] = item
        self.readers = ReaderCollection(klasses, config)
        self.update()
    
    def report(self):
        return {'subclasses':self.readers.report()}
    
    def update(self):
        try:
            self._peek = next(self.readers)
        except StopIteration: # OK
            self._peek = None
    
    def close(self):
        self.readers.close()
        
   