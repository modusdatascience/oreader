# 
# 
# class WriterCollection(object):
#     def __init__(self, klasses, config):
#         '''
#         klasses : (dict) contains name:class pairs
#         '''
#         self.writers = dict([(name,klass.writer_class(config)(klass, config)) for name, klass in klasses.iteritems()])
#         
#     def write(self, obj):
#         pass

class Writer(object):
    def __init__(self, klass, config):
        self.klass = klass
    
    def write(self, obj):
        raise NotImplementedError

class SimpleWriter(Writer):
    def __init__(self, klass, config):
        super(SimpleWriter, self).__init__(klass, config)
        self.config = config[klass]
        self.sink = self.config.start_sink(self)
        
    def write(self, obj):
        self.sink.write(obj)
        
    def close(self):
        self.sink.close()
    
class PolymorphicWriter(Writer):
    def __init__(self, klass, config):
        super(PolymorphicWriter, self).__init__(klass, config)
        self.writers = {}
#         stack = [klass]
        for subclass in klass.__subclasses__():
            self.writers[subclass] = subclass.writer_class(config)(subclass, config)
#         while stack:
#             item = stack.pop()
#             stack.extend(item.__subclasses__())
#             if item in config:
#                 self.writers[item] = item.writer_class(config)(item, config)

    def write(self, obj):
        writer = self.writers[obj.__class__]
        writer.write(obj)
        
    def close(self):
        for writer in self.writers.values():
            writer.close()
    
class ImplicitWriter(Writer):
    def __init__(self, klass, config):
        super(ImplicitWriter, self).__init__(klass, config)
        self.writers = {}
        for name, (child_class, _) in self.klass.relationships.items():
            self.writers[name] = child_class.writer_class(config)(child_class, config)
    
    def write(self, obj):
        for name, (_, plural) in self.klass.relationships.items():
            if plural:
                for child in sorted(getattr(obj, name, []), key=lambda x: x.sort_key()):
                    self.writers[name].write(child)
            else:
                child = getattr(obj, name, None)
                if child is not None:
                    self.writers[name].write(child)
                    
    def close(self):
        for writer in self.writers.values():
            writer.close()

class CompoundWriter(Writer):
    def __init__(self, klass, config):
        super(CompoundWriter, self).__init__(klass, config)
        self.config = config[klass]
        self.simple_writer = SimpleWriter(klass, config)
        self.implicit_writer = ImplicitWriter(klass, config)
    
    def write(self, obj):
        self.simple_writer.write(obj)
        self.implicit_writer.write(obj)
    
    def close(self):
        self.simple_writer.close()
        self.implicit_writer.close()

        