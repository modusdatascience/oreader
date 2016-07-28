from interval import Interval, IntervalSet
from collections import defaultdict

positive = IntervalSet([Interval(lower = 0, lower_closed=False)])
nonnegative = IntervalSet([Interval(lower = 0, lower_closed=True)])
negative = IntervalSet([Interval(upper = 0, upper_closed=False)])
nonpositive = IntervalSet([Interval(upper = 0, upper_closed=True)])

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
