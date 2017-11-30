from frozendict import frozendict
from six import string_types

class BaseAttributeGroup(object):
    def create_property(self, name):
        def accessor(obj):
            return self.emit(obj)
        accessor.__name__ = name
        return property(accessor)

class AttributeGroup(BaseAttributeGroup):
    def __init__(self, klass, attributes):
        self.klass = klass
        self.attributes = attributes
    
    def emit(self, obj):
        args = {}
        for k, v in self.attributes.items():
            if isinstance(v, string_types):
                args[k] = getattr(obj, v, None)
            else:
                args[k] = v(obj)
        return self.klass(**args)
        
class AttributeGroupList(BaseAttributeGroup):
    def __init__(self, attribute_groups, filter_function = None):
        self.attribute_groups = attribute_groups
        self.filter_function = filter_function
        
    def emit(self, obj):
        full = [group.emit(obj) for group in self.attribute_groups]
        return list(filter(self.filter_function, full))
    
class BaseAtrributeGroupMixin(object):
    _attribute_groups = frozendict()

def create_attribute_group_mixin(name, groups):
    attributes = {}
    for attr_name, group in groups.items():
        attributes[attr_name] = group.create_property(attr_name)
    klass = type(name, (BaseAtrributeGroupMixin,), attributes)
    attribute_groups = dict(klass._attribute_groups)
    attribute_groups.update(groups)
    klass._attribute_groups = frozendict(attribute_groups)
    return klass
