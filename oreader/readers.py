import warnings
import traceback

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
        self._peek = self.klass(**self.klass.translate_identity_key(current_key))
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
            stack.extend(item.__subclasses__())
            if item in config:
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
        
   