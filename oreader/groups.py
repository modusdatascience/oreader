
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
        return self.klass(**{k: getattr(obj, v, None) for k, v in self.attributes.items()})
        
class AttributeGroupList(BaseAttributeGroup):
    def __init__(self, attribute_groups, filter_function = None):
        self.attribute_groups = attribute_groups
        self.filter_function = filter_function
        
    def emit(self, obj):
        return filter(self.filter_function, [group.emit(obj) for group in self.attribute_groups])
    
class BaseAtrributeGroupMixin(object):
    pass

def create_attribute_group_mixin(name, groups):
    properties = {}
    for attr_name, group in groups.items():
        properties[attr_name] = group.create_property(attr_name)
    return type(name, (BaseAtrributeGroupMixin,), properties)
