import json
import jsonpickle


def gather_attrs(self, *args, skip=None):
    """
    Create string with attributes of the self object, processing nested classes and lists

    :param self: class to process
    :param args: filter for keys, if empty - adds everything
    :param skip: list of keys to skip (used for not printing nested fields option)
    :return: string with key:value for self class or self if it's not a class
    """
    attrs = []
    if not hasattr(self, '__dict__'):  # If this is not class, return self
        return self
    for key in sorted(self.__dict__):  # If this is class, process keys
        if not len(args) or key in args:
            k_value = getattr(self, key)  # -- add to the end
            if skip and key in skip:
                attrs.insert(0, '{}=(skipped)'.format(key))  # -- add to the beginning
            elif hasattr(k_value, '__dict__'):  # If this key is a class, add its attribute string
                attrs.append('{}={}'.format(key, create_object_str(k_value, skip=skip)))  # -- add to the end
            elif isinstance(k_value, list):  # If this is a list, process each element
                k_value_list = []
                for item in k_value:
                    k_value_list.append(create_object_str(item, skip=skip))
                attrs.append('{}={}'.format(key, k_value_list))  # -- add to the end
            else:
                attrs.insert(0, '{}={}'.format(key, k_value))  # -- add to the beginning
    return ', '.join(attrs)


def create_object_str(self, *args, skip=None):
    """
    Creates object description string

    :param self: object to process
    :param args: filter for keys, if empty - adds everything
    :param skip: list of keys to skip (used for not printing nested fields option)
    :return: object description [ClassName: attr1 = value, attr2 = value, ...]
    """
    return '[{}: {}]'.format(self.__class__.__name__, gather_attrs(self, *args, skip=skip))


def print_object(self, *args, skip=None):
    """
    Creates object description string

    :param self: object to process
    :param args: filter for keys, if empty - print everything
    :param skip: list of keys to skip (used for not printing nested fields option)
    """
    print(create_object_str(self, *args, skip=skip))


def print_class(obj):
    """
    Print object using jsonpickle
    :param obj:
    :return: None
    """
    print(json.loads(jsonpickle.dumps(obj, unpicklable=False).encode('utf-8')))


def objects_by_key(objects, key, key_value):
    """
    Selects objects with key = value from the list
    :param objects: list of objects
    :param key: key
    :param key_value: key = key_value filter
    :return: filtered list of objects
    """
    if not objects:
        return None
    found = [obj for obj in objects if hasattr(obj, key) and getattr(obj, key) == key_value]
    return found if found else None


def object_by_id(objects, o_id):
    """
    Selects obj with id=id from the list

    :param objects: list of objects
    :param o_id: id to search for
    :return: form with the id=id
    """
    if not objects:
        return None
    found = [obj for obj in objects if obj.id == o_id]
    return found[0] if found else None


def object_by_name(objects, name):
    """
    Selects obj with name=name from the list

    :param objects: list of objects
    :param name: name to search for
    :return: form with the name=name
    """
    if not objects:
        return None
    found = [obj for obj in objects if obj.name == name]
    return found[0] if found else None
