
# Local imports
from . import url_stream

# Third party imports
import numpy as np

# Standard library imports
import io
import collections


_SEP = '-'


class Serializable(object):
    """
    A base class for serializable objects containing numpy arrays.

    Note that this class currently requires all sub objects to be constructable with argument-less constructors and
    their properties set thereafter. Perhaps this is bad design, and we should be able to specify constructor arguments.
    """
    # Types that can be serialized by this object. Currently, I'm pretty limited.
    _SERIALIZABLE_TYPES = [
        np.ndarray,
    ]

    def __init__(self, serializable_properties={}):
        """
        Constructor.

        Args:
            serializable_properties: dict - A dictionary of all properties of this class that are to be serialized.
            Each key describes the property name, and each value describes the type of that property.
        """
        self._serializable_properties = serializable_properties

    @classmethod
    def SerializableTypes(cls):
        """
        A method for retrieving all objects (derived from this class) that can be serialized.

        Return:
            dict - A dictionary of all objects that can be serialized, with class names as keys, and classes as values.
        """
        return {v.__name__: v for v in cls.__subclasses__()}

    @classmethod
    def _isSerializable(cls, arg):
        """
        Checks if a given object is serializable.

        Args:
            arg: object or class - An object or class to check if it is serializable.

        Return:
            bool - Is the provided object or class serializable?
        """
        return (arg.__class__ in cls._SERIALIZABLE_TYPES) or (arg.__class__ in cls.__subclasses__())

    def ToDict(self):
        """
        Converts all configured properties of this object to a dictionary. Those configured properties are those
        provided upon construction.

        Return:
            dict - A dictionary of all properties as key value pairs. Here the key formatting is in the following
            format as class name / property name pairs with dash separators, e.g.:
            <class name>-<property name>-<property's class name>-<property's class's attribute name>
        """
        cls_prefix = self.__class__.__name__
        save_params = {}
        for key in self._serializable_properties:
            if (self.__dict__.get(key, None) is not None) and self._isSerializable(self.__dict__[key]):
                if isinstance(self.__dict__[key], Serializable):
                    sub_object_dict = self.__dict__[key].ToDict()
                    sub_object_dict = {_SEP.join((cls_prefix, key, k)): v for k, v in sub_object_dict.items()}
                    save_params.update(sub_object_dict)
                else:
                    save_params[_SEP.join((cls_prefix, key))] = self.__dict__[key]
            else:
                raise TypeError('This item specifies unserializable property to serialize of type {}'.format(self.__dict__[key].__name__))
        return save_params

    @classmethod
    def FromDict(cls, class_data):
        """
        Takes a dictionary resulting from a previously serialized object and reconstructs the class instance.

        Args:
            class_data: dict(str, serializable primitive) - A dictionary of key value pairs describing a serialized
            object. Each key should be formatted in the following format as class name / property name pairs with
            dash separators, e.g.:
            <class name>-<property name>-<property's class name>-<property's class's attribute name>

        Return:
            Serializable - The constructed instance.
        """
        # TODO [matthew.mccallum 02.23.18]: Check all dictionary objects are from this class and the same instance here.

        # Construct the object
        this_obj = cls()

        # Either create the type, or save it to a list of sub objects
        sub_objs = collections.defaultdict(dict)
        for key, value in class_data.items():
            key_components = key.split(_SEP)
            if len(key_components) > 2:
                sub_objs[key_components[1]].update({_SEP.join(key_components[2:]): value})
            else:
                setattr(this_obj, key_components[1], value)

        # Deserialize the sub objects
        for key, value in sub_objs:
            class_name = value.keys()[0].split(_SEP)[0]
            this_class = cls.SerializableTypes()[class_name]
            sub_obj = this_class.FromDict(value)
            setattr(this_obj, key, sub_obj)

        return this_obj


def Serialize(objects, url):
    """
    Take a list of object instances and save them as a dictionary of numpy arrays.

    Args:
        objects: list(Serializable) - A list of serializable objects to save to file.

        url: str - The url to save the object data to
    """
    # For each class, do the below
    all_objs = {}
    instance_counter = 0
    for obj in objects:
        obj_data = obj.ToDict()
        obj_data = {_SEP.join((str(instance_counter), k)): v for k, v in obj_data.items()}
        all_objs.update(obj_data)

    bytes_stream = io.BytesIO()
    # TODO [matthew.mccallum 03.21.18]: This way of pickling data is restricted to numpy arrays. I should compare the
    # sizes of arrays pickled by a recent python3 implementation of pickle with the highest protocol and numpy's savez.
    np.savez(bytes_stream, **all_objs)
    data = bytes_stream.getvalue()
    with url_stream.get_stream(url, 'wb') as out_stream:
        out_stream.write(data)


def Deserialize(url):
    """
    Deserialize all objects from a saved file into a list of constructed objects.

    Args:
        url: str - A url pointing to a file containing a set of previously serialized objects.

    Return:
        list(Serializable) - A list of deserialized Serializable objects, read from the provided URL.
    """
    #TODO [matthew.mccallum 02.23.18]: Check all dictionary objects are from serializable classes here.

    #TODO [matthew.mccallum 02.23.18]: Load url here
    data = {}
    all_objs = []
    with url_stream.get_stream(url, 'rb') as in_stream:
        data = dict(np.load(in_stream))

    # Group relevant objects into instances...
    instances = collections.defaultdict(dict)
    for key, value in data.items():
        instances[key.split(_SEP)[0]].update({'-'.join(key.split(_SEP)[1:]): value})
    instances = instances.values()

    # Construct each instance
    for instance in instances:
        class_name = next(iter(instance)).split(_SEP)[0]
        this_class = Serializable.SerializableTypes()[class_name]
        all_objs += [this_class.FromDict(instance)]

    return all_objs

