from abc import abstractmethod
import pandas
import datetime
from .readers import PolymorphicReader, CompoundReader, ImplicitReader,\
    SimpleReader
import random
from .mfrozendict import frozendict, FrozenOrderedDict
from oreader.reader_configs import SimpleReaderConfig
from decimal import Decimal
from oreader.writers import SimpleWriter, PolymorphicWriter, CompoundWriter,\
    ImplicitWriter
from oreader.writer_configs import SimpleWriterConfig
import traceback
import arrow
from arrow.parser import ParserError
from sqlalchemy.sql.sqltypes import Integer, String, Float, Date, DateTime,\
    Boolean
from sqlalchemy.sql.schema import Column, Table
from six import text_type
from toolz.dicttoolz import valmap

class classproperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()

def _backrelate(cls, relationships):
    for k, v in relationships.items():
        d = {k: tuple([cls] + [v[i] for i in range(1,len(v))])}
        d.update(v[0].relationships)
        v[0].relationships = FrozenOrderedDict(d)
        v[0].init_relationships()
    return cls

def backrelate(relationships):
    return lambda cls: _backrelate(cls, relationships)

def _relate(cls, relationships):
    for k, v in relationships.items():
        d = {k: v}
        d.update(cls.relationships)
        cls.relationships = FrozenOrderedDict(d)
    cls.init_relationships()
    return cls
        
def relate(relationships):
    return lambda cls: _relate(cls, relationships)

class OColumn(object):
    def __init__(self, **kwargs):
        self.name = kwargs['name']
        if 'random' in kwargs:
            self.random_values = [self.convert(item) for item in kwargs['random'].split(';')]

    def draw(self):
        return random.choice(self.random_values)
    
    def to_sqa(self):
        return Column(self.name, self.sqa_type)
    
class StringColumn(OColumn):
    sqa_type = String()
    def __init__(self, **kwargs):
        super(StringColumn,self).__init__(**kwargs)
        self.strip = True if kwargs.get('format', None) == 'strip' else False
        
    def convert(self, value):
        if value is None:
            return value
        if type(value) is str:
            val = value
        elif type(value) is text_type:
            val = value.encode('utf8')
        else:
            val = str(value)
        return val.strip() if self.strip else val

    def unconvert(self, value):
        if value is None:
            return None
        return str(value)
    
class RealColumn(OColumn):
    sqa_type = Float()
    def convert(self, value):
        if value is None:
            return value
        try:
            value = value.strip()
            if not value:
                return None
        except AttributeError:
            pass
        try:
            return float(value)
        except ValueError:
            return None

    def unconvert(self, value):
        if value is None:
            return None
        return str(value)
    
class IntegerColumn(OColumn):
    sqa_type = Integer()
    def convert(self, value):
        try:
            value = value.strip()
            if not value:
                return None
        except AttributeError:
            pass
        try:
            return int(value)
        except ValueError:
            return None
        except TypeError:
            return None
        
    def unconvert(self, value):
        if value is None:
            return None
        return str(value)

class DateColumn(OColumn):
    sqa_type = Date
    def __init__(self, **kwargs):
        super(DateColumn,self).__init__(**kwargs)
        self.format = kwargs.get('format', 'YYYY-MM-DD')
        
    def convert(self, value):
        if value is None:
            return value
        if type(value) is datetime.date:
            return arrow.get(value).date()
        if type(value) is datetime.datetime:
            return arrow.get(value).date()
        try:
            value = value.strip()
        except AttributeError:
            pass
        if not value:
            return None
        try:
            return arrow.get(value, self.format).date()
        except (ParserError, ValueError):
            return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return arrow.get(value).format(self.format)

class DateTimeColumn(OColumn):
    sqa_type = DateTime
    def __init__(self, **kwargs):
        super(DateTimeColumn,self).__init__(**kwargs)
        self.format = kwargs.get('format', 'YYYY-MM-DD HH:mm:ss')
        
    def convert(self, value):
        if value is None:
            return value
        if type(value) is datetime.date:
            return datetime.datetime(value.year, value.month, value.day, 0, 0, 0)
        if type(value) is datetime.datetime:
            return value
        try:
            value = value.strip()
        except AttributeError:
            pass
        if not value:
            return None
        try:
            return arrow.get(value,self.format).datetime
        except (ParserError, ValueError):
            return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return arrow.get(value).format(self.format)

class BooleanColumn(OColumn):
    sqa_type = Boolean
    true_flags = {'1', 1, 't', 'T', 'true', 'True', 1.0, '1.0', '1.', 'y', 'Y', 'Yes', 'yes', 'YES'}
    false_flags = {'0', 0, 'f', 'F', 'false', 'False', 0.0, '0.0', '0.', 'n', 'N', 'No', 'no', 'NO'}
    none_flags = {''}
    def convert(self, value):
        try:
            value = value.strip()
#             if value == '':
#                 return None
        except AttributeError:
            pass
        if value in self.true_flags:
            return True
        elif value in self.false_flags:
            return False
        elif value in self.none_flags:
            return None
        else:
            raise ValueError('Cannot convert %s to boolean' % str(value))
#         try:
#             return bool(value)
#         except ValueError:
#             return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return str(value)
    
class EmptyColumn(OColumn):
    '''
    Ensures that the column never contains data.  If strict, any data in the column
    causes an error.
    '''
    def __init__(self, **kwargs):
        super(EmptyColumn,self).__init__(**kwargs)
        if kwargs['format'].strip() in ('strict',):
            self.strict = True
        elif kwargs['format'].strip() in ('loose',''):
            self.strict = False
        else:
            raise ValueError('Unknown format option %s for EmptyColumn' % kwargs['format'])
        
    def convert(self, value):
        if self.strict:
            try:
                value = value.strip()
            except AttributeError:
                pass
            if value == '':
                value = None
            try:
                assert value is None
            except:
                raise AssertionError('EmptyColumn(name="%s") contains nonempty value "%s"' % (self.name, value))
        return None

    def unconvert(self, value):
        return ''
    
types = {'string': StringColumn, 
         'real': RealColumn,
         'integer': IntegerColumn,
         'date': DateColumn,
         'datetime': DateTimeColumn,
         'boolean': BooleanColumn,
         'empty': EmptyColumn}

sqa_types = {str: StringColumn,
             float: RealColumn,
             int: IntegerColumn,
             datetime.datetime: DateTimeColumn,
             datetime.date: DateColumn,
             bool: BooleanColumn,
             Decimal: RealColumn}

sqa_args = {str: {'format': None},
            float: {},
            int: {},
            datetime.datetime: {'format': '%Y-%m-%d %H:%M:%S'},
            datetime.date: {'format': '%Y-%m-%d'},
            bool: {},
            Decimal: {}}

def _schema(cls,columns):
    cls.columns = tuple(columns)
    cls.columns_by_name = frozendict([(col.name, col) for col in cls.columns])
    cls.init_schema()
    return cls
    
def schema(columns):
    return lambda cls: _schema(cls,columns)

def csv_schema(filename):
    table = pandas.read_csv(filename,dtype=object)
    table = table.fillna('')
    columns = []
    for _, row in table.iterrows():
        columns.append(types[row['type']](**row))
    return schema(columns)

def sqa_schema(table):
    columns = []
    for sqa_col in table.columns:
        columns.append(sqa_types[sqa_col.type.python_type](name=sqa_col.name, **sqa_args[sqa_col.type.python_type]))
    return schema(columns)

def freeze(obj):
    if type(obj) is dict:
        return frozendict(obj.items)
    elif type(obj) is list:
        return tuple(obj)
    else:
        return obj

class DataObject(object):
    
    def __init__(self, **kwargs):
        for column in self.__class__.columns:
            if column.name in kwargs:
                try:
                    converted_value = column.convert(kwargs[column.name])
                except Exception as e:
                    traceback.print_exc(e)
                    try:
                        error_string = 'Unable to convert value %s for field %s' % (kwargs[column.name], column.name)
                    except:
                        error_string = 'Unable to convert value for field %s' % (column.name)
                    raise ValueError(error_string)
                setattr(self,column.name,converted_value)
                del kwargs[column.name]
            else:
                setattr(self,column.name,None)
        if kwargs:
            raise TypeError('Unexpected argument(s) initializing %s: %s' % (self.__class__.__name__, str(kwargs)))
        for k, v in self.__class__.relationships.items():
            if v[1]:
                setattr(self,k,[])
            else:
                setattr(self,k,None)
    
    @classmethod
    def to_sqa_table(cls, metadata, name, **kwargs):
        '''
        Create a sqalchemy Table object corresponding to this class.  Use kwargs to 
        override any desired columns. 
        '''
        bad_kwargs = set(kwargs.keys()) - set(map(lambda col: col.name, cls.columns))
        if bad_kwargs:
            raise ValueError('Optional keyword argument names must correspond to field names.  The following names are not compliant: %s' % str(sorted(bad_kwargs)) )
        cols = [col.to_sqa() if col.name not in kwargs else kwargs[col.name] for col in cls.columns]
        return Table(name, metadata, *cols)
    
    def __getstate__(self):
        state = {}
        for col in self.columns:
            if hasattr(self, col.name):
                state[col.name] = getattr(self, col.name)
        for k in sorted(self.relationships.keys()):
            state[k] = getattr(self, k)
        return state
    
    def __setstate__(self, state):
        self.__dict__.update(state)
        
    def __eq__(self, other):
        # TODO: This could be made much faster by a custom implementation
        return self.__class__ is other.__class__ and self.__getstate__() == other.__getstate__()
    
    def __hash__(self):
        return hash((self.__class__, frozendict(valmap(freeze, self.__getstate__()).items())))
    
    def to_row(self):
        return [col.unconvert(getattr(self,col.name,None)) for col in self.columns]
    
    @classmethod
    def header(cls):
        return [col.name for col in cls.columns]
    
    @classmethod
    def init_schema(cls):
        '''
        Called after the schema has been added.  Allows class to do any necessary initialization steps that require
        schema information.
        '''
        pass
    
    @classmethod
    def init_relationships(cls):
        '''
        Called after a relationship is added.  Allows class to do any necessary initialization steps that require
        relationship information.
        '''
        pass
    
    relationships = FrozenOrderedDict()
    
    @classmethod
    def subtypes(cls):
        stack = [cls]
        result = []
        while stack:
            item = stack.pop()
            stack.extend(item.__subclasses__())
            result.append(item)
        return result
    
    @classmethod
    def typerank(cls):
        if cls.concrete():
            d = {cls:0}
        else:
            d = {t: i for i, t in enumerate(cls.subtypes())}
        def _typerank(obj):
            return d[obj]
        return _typerank
    
    @classmethod
    def objrank(cls):
        tr = cls.typerank()
        def _objrank(obj):
            return tr(type(obj))
        return _objrank
    
    @classmethod
    def relationship_sort_key(cls, relationship):
        klass = cls.relationships[relationship][0]
        typerank = klass.typerank()
        def sort_key(obj):
            return (typerank(type(obj)), obj.sort_key())
        return sort_key
    
    @classproperty
    @classmethod
    @abstractmethod
    def partition_attribute(cls):
        print(cls)
        raise NotImplementedError
    
    def set_container_key(self, key):
        translation = self.translate_container_key(key)
        for k, v in translation.items():
            setattr(self, k, v)
            
    @classmethod
    def translate_container_key(cls, key):
        result = {}
        for i, (k, v) in enumerate(cls.container_key_):
            result[v] = key[i]
        return result
    
    def set_identity_key(self, key):
        translation = self.translate_identity_key(key)
        for k, v in translation.items():
            setattr(self, k, v)
            
    @classmethod
    def translate_identity_key(cls, key):
        result = {}
        for i, (k, v) in enumerate(cls.identity_key_):
            result[v] = key[i]
        return result
    
    def container_key(self):
        return tuple(getattr(self, v) for k, v in self.container_key_)
    
    def identity_key(self):
        return tuple(getattr(self, v) for k, v in self.identity_key_)
    
    def sort_key(self):
        return tuple(getattr(self,key) for key in self.sort_key_)
    
    @classproperty
    @classmethod
    def sort_column_numbers(cls):
        return [cls.header().index(key) for key in cls.sort_key_]
    
    @classproperty
    @classmethod
    def sort_column_names(cls):
        return cls.sort_key_
    
#     def __richcmp__(DataObject self, DataObject other, int op):
#         if op == 0:# <
#             return self.sort_key() < other.sort_key()
#         elif op == 1:# ==
#             return self.__class__ is other.__class__ and self.__getstate__() == other.__getstate__()
#         elif op == 2:# >
#             return self.sort_key() > other.sort_key()
#         elif op == 3:# <= 
#             return self.sort_key() <= other.sort_key()
#         elif op == 4:# != 
#             return not (self.__class__ is other.__class__ and self.__getstate__() == other.__getstate__())
#         elif op == 5:# >=
#             return self.sort_key() >= other.sort_key()
        
    
    def __lt__(self, other):
        if not isinstance(other,DataObject):
            return NotImplemented
        try:
            return self.sort_key() < other.sort_key()
        except AttributeError:
            return NotImplemented
        
    def __le__(self, other):
        if not isinstance(other,DataObject):
            return NotImplemented
        try:
            return self.sort_key() <= other.sort_key()
        except AttributeError:
            return NotImplemented
    
    def __gt__(self, other):
        if not isinstance(other,DataObject):
            return NotImplemented
        try:
            return self.sort_key() > other.sort_key()
        except AttributeError:
            return NotImplemented
        
    def __ge__(self, other):
        if not isinstance(other,DataObject):
            return NotImplemented
        try:
            return self.sort_key() >= other.sort_key()
        except AttributeError:
            return NotImplemented
    
    @classmethod
    def concrete(cls):
        return not cls.__subclasses__()
    
    @classmethod
    def reader_class(cls, config):
        if cls.__subclasses__():
            assert cls not in config
            return PolymorphicReader
        elif cls.relationships:
            if cls in config:
                return CompoundReader
            else:
                return ImplicitReader
        else:
            assert cls in config
            assert isinstance(config[cls], SimpleReaderConfig), 'No config found for class %s' % cls.__name__
            return SimpleReader
    
    @classmethod
    def writer_class(cls, config):
        if cls.__subclasses__():
            assert cls not in config
            return PolymorphicWriter
        elif cls.relationships:
            if cls in config:
                return CompoundWriter
            else:
                return ImplicitWriter
        else:
            assert cls in config
            assert isinstance(config[cls], SimpleWriterConfig)
            return SimpleWriter
    
    @classmethod
    def reader(cls, config):
        return cls.reader_class(config)(cls,config)
    
    @classmethod
    def writer(cls, config):
        return cls.writer_class(config)(cls,config)
        