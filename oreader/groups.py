
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
# 
# def process_groups(table):#names, group_members, groups):
#     dx_groups_dict = {}
#     px_groups_dict = {}
#     for _, row in table.iterrows():
#         try:
#             name = row['name']
#             member_type = row['member_type']
#             if member_type:
#                 member_type = member_type.replace(' ','_')
#             group_type = row['group_type']
#             group_name = row['group']
#             code_system = row['code_system']
#         except KeyError:
#             return [], []
#         except:
#             raise
#         if group_type == 'procedure':
#             groups_dict = px_groups_dict
#         elif group_type == 'diagnosis':
#             groups_dict = dx_groups_dict
#         else:
#             continue
#         try:
#             groups_dict[group_name][member_type] = name
#         except KeyError:
#             groups_dict[group_name] = {member_type: name}
#         if code_system is not None:
#             try:
#                 groups_dict[group_name]['default_code_system'] = code_system
#             except KeyError:
#                 groups_dict[group_name] = {'default_code_system': code_system}
#             
#     dx_groups = []
#     px_groups = []
#     for d in dx_groups_dict.values():
#         dx_groups.append(ColumnGroup(**d))
#     for d in px_groups_dict.values():
#         px_groups.append(ColumnGroup(**d))
#     return dx_groups, px_groups
#             
# 
# 
# class ColumnGroup(object):
#     def __init__(self, code=None, code_system=None, date=None, default_code_system=None):
#         self.code_col = code
#         self.code_system_col = code_system
#         self.date_col = date
#         self.default_code_system = default_code_system
#         
#     def emit(self, obj):
#         result = {}
#         if self.default_code_system is not None:
#             val = self.default_code_system
#             if val != '' and val is not None:
#                 result['code_system'] = val
#         if self.code_col is not None:
#             val = getattr(obj,self.code_col)
#             if val != '' and val is not None:
#                 result['code'] = getattr(obj,self.code_col)
#         if self.code_system_col is not None:
#             val = getattr(obj,self.code_system_col)
#             if val != '' and val is not None:
#                 result['code_system'] = getattr(obj,self.code_system_col)
#         if self.date_col is not None:
#             val = getattr(obj,self.date_col)
#             if val != '' and val is not None:
#                 result['date'] = getattr(obj,self.date_col)
#         return result
# 
# class DiagnosisContainerMixIn(object):
#     @property
#     def diagnoses(self):
#         result = []
#         for group in self._dx_groups:
#             args = group.emit(self)
#             if hasattr(self, '_dx_aliases'):
#                 if 'code_system' in args:
#                     args['code_system'] = self._dx_aliases[args['code_system']]
#             if hasattr(self, '_dx_transformers'):
#                 if 'code' in args and 'code_system' in args:
#                     args['code'] = self._dx_transformers[args['code_system']](args['code'])
#                 else:
#                     assert 'code' not in args#If there is no code_system then there should be no code
#             if 'code' in args:
#                 result.append(self.dx_class(**args))
#         return result
# 
# class ProcedureContainerMixIn(object):
#     @property
#     def procedures(self):
#         result = []
#         for group in self._px_groups:
#             args = group.emit(self)
#             if hasattr(self, '_px_aliases'):
#                 if 'code_system' in args:
#                     args['code_system'] = self._px_aliases[args['code_system']]
#             if hasattr(self, '_px_transformers'):
#                 if 'code' in args and 'code_system' in args:
#                     args['code'] = self._px_transformers[args['code_system']](args['code'])
#                 else:
#                     assert 'code' not in args#If there is no code_system then there should be no code
#             if 'code' in args:
#                 result.append(self.px_class(**args))
#         return result
# 
# # class RevenueSourceContainerMixIn(object):
# #     @property
# #     def revenue_sources(self):
# #         result = []
# #         for group in self._rev_groups:
# #             args = group.emit(self)
# #             if hasattr(self, '_rev_aliases'):
# #                 if 'code_system' in args:
# #                     args['code_system'] = self._rev_aliases[args['code_system']]
# #             if hasattr(self, '_rev_transformers'):
# #                 if 'code' in args and 'code_system' in args:
# #                     args['code'] = self._rev_transformers[args['code_system']](args['code'])
# #                 else:
# #                     assert 'code' not in args#If there is no code_system then there should be no code
# #             if 'code' in args:
# #                 result.append(self.rev_class(**args))
# #         return result
#     
