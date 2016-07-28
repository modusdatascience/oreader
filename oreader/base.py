from abc import abstractmethod
from collections import defaultdict
import pandas
import datetime
from readers import PolymorphicReader, CompoundReader, ImplicitReader,\
    SimpleReader
import random
from frozendict import frozendict
import pickle
from oreader.reader_configs import SimpleReaderConfig
from sqlalchemy.sql.sqltypes import String, Text, Float, Integer, Date, Boolean
from decimal import Decimal
from oreader.writers import SimpleWriter, PolymorphicWriter, CompoundWriter,\
    ImplicitWriter
from oreader.writer_configs import SimpleWriterConfig

class Interval(object):
    def __init__(self, lower=float('-inf'), lower_closed = True, upper = float('inf'), upper_closed=False):
        self.lower = lower
        self.lower_closed = lower_closed
        self.upper = upper
        self.upper_closed = upper_closed
        
    def __contains__(self, value):
        return (self.lower < value and self.uper > value) or \
            (self.lower_closed and self.value == self.lower) or \
            (self.upper_closed and self.value == self.upper)
    
positive = Interval(lower = 0, lower_closed=False)
nonnegative = Interval(lower = 0, lower_closed=True)
negative = Interval(upper = 0, upper_closed=False)
nonpositive = Interval(upper = 0, upper_closed=True)

def all_or_none(collection, attribute, ignore={}):
    if not collection:
        return None
    first = True
    result = None
    for item in collection:
        try:
            value = getattr(item,attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            first = False
        elif value != result:
            return None
    return result
        
def all_or_raise(collection, attribute, ignore={}):
    if not collection:
        return None
    first = True
    result = None
    for item in collection:
        try:
            value = getattr(item,attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            first = False
        elif value != result:
            raise ValueError
    return result

def mode(collection, attribute, ignore={None}):
    values = defaultdict(int)
    highest = 0
    mode = None
    for item in collection:
        try:
            value = getattr(item,attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        values[value] += 1
        if values[value] > highest:
            highest = values[value]
            mode = value
    return mode
    
def minimum(collection, attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item,attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            first = False
        elif value < result:
            result = value
    return result

def maximum(collection, attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item,attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            first = False
        elif value > result:
            result = value
    return result


def latest(collection, attribute, date_attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item, attribute)
        except AttributeError:
            continue
        try:
            date = getattr(item, date_attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            result_date = date
            first = False
        elif result_date < date:
            result = value
            result_date = date
    return result


def earliest(collection, attribute, date_attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item, attribute)
        except AttributeError:
            continue
        try:
            date = getattr(item, date_attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            result_date = date
            first = False
        elif result_date > date:
            result = value
            result_date = date
    return result

def total(collection, attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item, attribute)
        except AttributeError:
            continue
        if value in ignore:
            continue
        if first:
            result = value
            first = False
        else:
            result += value
    return result

def concatenation(collection, attribute, ignore={None}, default=None):
    first = True
    result = default
    for item in collection:
        try:
            value = getattr(item, attribute)
        except AttributeError:
            continue
        try:
            if value in ignore:
                continue
        except TypeError:
            pass
        if first:
            result = value
            first = False
        else:
            result += value
    return result

#
#class FileMapper(object):
#    '''Maps a data file (csv format or similar) to a DataObject class for the purpose of 
#    reading.'''

class classproperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()

#def noninherited(cls):
#    return lambda value: _noninherited(value, cls)
#
#class _noninherited():
#    def __init__(self, value, cls):
#        self.value = value
#        self.cls = cls
#    
#    def __get__(self, instance, owner):
#        if instance.__class__ is self.cls:
#            return self.value
#        else:
#            raise AttributeError
#    
#    def __set__(self, instance, value):
#        if instance.__class__ is self.cls:
#            self.value = value

def _backrelate(cls, relationships):
    for k, v in relationships.iteritems():
        d = {k: tuple([cls] + [v[i] for i in range(1,len(v))])}
        d.update(v[0].relationships)
        v[0].relationships = frozendict(d)
#         v[0].relationships[k] = tuple([cls] + [v[i] for i in range(1,len(v))])#tuple(list(cls,*v[1:]))
        v[0].init_relationships()
    return cls
        
def backrelate(relationships):
    return lambda cls: _backrelate(cls, relationships)

def _relate(cls, relationships):
    for k, v in relationships.iteritems():
        d = {k: v}
        d.update(cls.relationships)
        cls.relationships = frozendict(d)
    cls.init_relationships()
    return cls
        
def relate(relationships):
    return lambda cls: _relate(cls, relationships)
# 
# def _sub(cls, sub):
#     cls.subs = cls.subs + (sub,)
#  
# def sub(cls):
#     return lambda cls: _sub(cls, self)

# def sub(super_class):
#     def set_sub(klass):
#         super_class.subs = super_class.subs + (klass,)
#         return klass
#     return set_sub

class CsvColumn(object):
    def __init__(self, **kwargs):
        self.name = kwargs['name']
        if 'random' in kwargs:
            self.random_values = [self.convert(item) for item in kwargs['random'].split(';')]

    def draw(self):
        return random.choice(self.random_values)
    
class StringColumn(CsvColumn):
    def __init__(self, **kwargs):
        super(StringColumn,self).__init__(**kwargs)
        self.strip = True if kwargs.get('format', None) == 'strip' else False
        
    def convert(self, value):
        if value is None:
            return value
        return unicode(value).encode('utf8').strip() if self.strip else value

    def unconvert(self, value):
        if value is None:
            return None
        return str(value)
    
class RealColumn(CsvColumn):
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
    
class IntegerColumn(CsvColumn):
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

class DateColumn(CsvColumn):
    def __init__(self, **kwargs):
        super(DateColumn,self).__init__(**kwargs)
        self.format = kwargs.get('format', '%Y-%m-%d')
        
    def convert(self, value):
        if value is None:
            return value
        if type(value) is datetime.date:
            return value
        if type(value) is datetime.datetime:
            return value.date()
        try:
            value = value.strip()
        except AttributeError:
            pass
        try:
            return datetime.datetime.strptime(value,self.format).date()
        except ValueError:
            return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return datetime.datetime.strftime(value,self.format)

class DateTimeColumn(CsvColumn):
    def __init__(self, **kwargs):
        super(DateTimeColumn,self).__init__(**kwargs)
        self.format = kwargs.get('format', '%Y-%m-%d %H:%M:%S')
        
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
        try:
            return datetime.datetime.strptime(value,self.format)
        except ValueError:
            return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return datetime.datetime.strftime(value,self.format)

class BooleanColumn(CsvColumn):
    def convert(self, value):
        try:
            value = value.strip()
            if value == '':
                return None
        except AttributeError:
            pass
        try:
            return bool(value)
        except ValueError:
            return None
    
    def unconvert(self, value):
        if value is None:
            return None
        return str(value)
    
class EmptyColumn(CsvColumn):
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

def _schema(cls,columns,dx_groups=None,px_groups=None):
    cls.columns = tuple(columns)
    cls.columns_by_name = frozendict([(col.name, col) for col in cls.columns])
    cls._dx_groups = tuple(dx_groups) if dx_groups is not None else None
    cls._px_groups = tuple(px_groups) if px_groups is not None else None
    cls.init_schema()
    return cls
    
def schema(columns,dx_groups=None,px_groups=None):
    return lambda cls: _schema(cls,columns,dx_groups=dx_groups,px_groups=px_groups)

def process_groups(table):#names, group_members, groups):
    dx_groups_dict = {}
    px_groups_dict = {}
    for _, row in table.iterrows():
        try:
            name = row['name']
            member_type = row['member_type']
            if member_type:
                member_type = member_type.replace(' ','_')
            group_type = row['group_type']
            group_name = row['group']
            code_system = row['code_system']
        except KeyError:
            return [], []
        except:
            raise
        if group_type == 'procedure':
            groups_dict = px_groups_dict
        elif group_type == 'diagnosis':
            groups_dict = dx_groups_dict
        else:
            continue
        try:
            groups_dict[group_name][member_type] = name
        except KeyError:
            groups_dict[group_name] = {member_type: name}
        if code_system is not None:
            try:
                groups_dict[group_name]['default_code_system'] = code_system
            except KeyError:
                groups_dict[group_name] = {'default_code_system': code_system}
            
    dx_groups = []
    px_groups = []
    for d in dx_groups_dict.values():
        dx_groups.append(ColumnGroup(**d))
    for d in px_groups_dict.values():
        px_groups.append(ColumnGroup(**d))
    return dx_groups, px_groups
            

def csv_schema(filename):
    table = pandas.read_csv(filename,dtype=object)
    table = table.fillna('')
    columns = []
    for _, row in table.iterrows():
        columns.append(types[row['type']](**row))
    dx_groups, px_groups = process_groups(table)#['name'],table['group_member'],table['group'])
    return schema(columns, dx_groups=dx_groups, px_groups=px_groups)

def sqa_schema(table):
    columns = []
    for sqa_col in table.columns:
        columns.append(sqa_types[sqa_col.type.python_type](name=sqa_col.name, **sqa_args[sqa_col.type.python_type]))
    return schema(columns)

class DataObject(object):
    
    def __init__(self, **kwargs):
        for column in self.__class__.columns:
            if column.name in kwargs:
                setattr(self,column.name,column.convert(kwargs[column.name]))
                del kwargs[column.name]
        if kwargs:
            raise TypeError('Unexpected argument(s) initializing %s: %s' % (self.__class__.__name__, str(kwargs)))
        for k, v in self.__class__.relationships.iteritems():
            if v[1]:
                setattr(self,k,[])
            else:
                setattr(self,k,None)
    
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
        return hash(pickle.dumps(self))
    
    def to_row(self):
        return [col.unconvert(getattr(self,col.name,None)) for col in self.columns]
    
    @classmethod
    def header(cls):
        return [col.name for col in cls.columns]
    
#    @abstractmethod
#    @classmethod
#    def reader_class(cls):
#        pass
    
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
    
#     @abstractmethod
#     @classproperty
#     @classmethod
#     def columns(cls):
#         raise NotImplementedError
    
    relationships = frozendict()
#     @classproperty
#     @classmethod
#     def relationships(cls):#@NoSelf
#         try:
#             return cls._relationships[cls]
#         except KeyError:
#             cls._relationships[cls] = {}
#             return cls._relationships[cls]
    
#     subs = tuple()
    
    @abstractmethod
    @classproperty
    @classmethod
    def partition_attribute(cls):
        print cls
        raise NotImplementedError
    
    @abstractmethod
    def set_container_key(self, key):
        translation = self.translate_container_key(key)
        for k, v in translation.items():
            setattr(self, k, v)
            
    @classmethod
    def translate_container_key(cls, key):
        result = {}
        for k, v in cls.container_key_:
            result[v] =  key[k]
        return result
    
    @abstractmethod
    def set_identity_key(self, key):
        translation = self.translate_identity_key(key)
        for k, v in translation.items():
            setattr(self, k, v)
            
    @classmethod
    def translate_identity_key(cls, key):
        result = {}
        for k, v in cls.identity_key_:
            result[v] =  key[k]
        return result
    
    @abstractmethod
    def container_key(self):
        return {k: getattr(self, v) for k, v in self.container_key_}
#         print self
#         raise NotImplementedError
    
    @abstractmethod
    def identity_key(self):
        return {k: getattr(self, v) for k, v in self.identity_key_}
#         print self
#         raise NotImplementedError
    
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
#     @abstractmethod
#     def sort_key(self):
#         print self
#         raise NotImplementedError
    
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
            return self.sort_key() < other.sort_key()
        except AttributeError:
            return NotImplemented
        
    def __ge__(self, other):
        if not isinstance(other,DataObject):
            return NotImplemented
        try:
            return self.sort_key() <= other.sort_key()
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
            assert isinstance(config[cls], SimpleReaderConfig)
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
    
class ColumnGroup(object):
    def __init__(self, code=None, code_system=None, date=None, default_code_system=None):
        self.code_col = code
        self.code_system_col = code_system
        self.date_col = date
        self.default_code_system = default_code_system
        
    def emit(self, obj):
        result = {}
        if self.default_code_system is not None:
            val = self.default_code_system
            if val != '' and val is not None:
                result['code_system'] = val
        if self.code_col is not None:
            val = getattr(obj,self.code_col)
            if val != '' and val is not None:
                result['code'] = getattr(obj,self.code_col)
        if self.code_system_col is not None:
            val = getattr(obj,self.code_system_col)
            if val != '' and val is not None:
                result['code_system'] = getattr(obj,self.code_system_col)
        if self.date_col is not None:
            val = getattr(obj,self.date_col)
            if val != '' and val is not None:
                result['date'] = getattr(obj,self.date_col)
        return result

class DiagnosisContainerMixIn(object):
    @property
    def diagnoses(self):
        result = []
        for group in self._dx_groups:
            args = group.emit(self)
            if hasattr(self, '_dx_aliases'):
                if 'code_system' in args:
                    args['code_system'] = self._dx_aliases[args['code_system']]
            if hasattr(self, '_dx_transformers'):
                if 'code' in args and 'code_system' in args:
                    args['code'] = self._dx_transformers[args['code_system']](args['code'])
                else:
                    assert 'code' not in args#If there is no code_system then there should be no code
            if 'code' in args:
                result.append(self.dx_class(**args))
        return result

class ProcedureContainerMixIn(object):
    @property
    def procedures(self):
        result = []
        for group in self._px_groups:
            args = group.emit(self)
            if hasattr(self, '_px_aliases'):
                if 'code_system' in args:
                    args['code_system'] = self._px_aliases[args['code_system']]
            if hasattr(self, '_px_transformers'):
                if 'code' in args and 'code_system' in args:
                    args['code'] = self._px_transformers[args['code_system']](args['code'])
                else:
                    assert 'code' not in args#If there is no code_system then there should be no code
            if 'code' in args:
                result.append(self.px_class(**args))
        return result

# class RevenueSourceContainerMixIn(object):
#     @property
#     def revenue_sources(self):
#         result = []
#         for group in self._rev_groups:
#             args = group.emit(self)
#             if hasattr(self, '_rev_aliases'):
#                 if 'code_system' in args:
#                     args['code_system'] = self._rev_aliases[args['code_system']]
#             if hasattr(self, '_rev_transformers'):
#                 if 'code' in args and 'code_system' in args:
#                     args['code'] = self._rev_transformers[args['code_system']](args['code'])
#                 else:
#                     assert 'code' not in args#If there is no code_system then there should be no code
#             if 'code' in args:
#                 result.append(self.rev_class(**args))
#         return result
    
    