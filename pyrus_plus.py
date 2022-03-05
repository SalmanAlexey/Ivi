"""
Functions for representing Fields as strings and for getting Fields value from the task in usable form
"""
from object_methods import create_object_str, object_by_name, object_by_id


def field_value_to_str(field):
    """
    Return string representation of a pyrus FormField value for future printing, inserting into log
    :param field: Field to convert to string
    :return: text string for given field
    """

    if field is None:
        return None
    if not field.value:  # Setting empty fields value to empty string
        text = '<None>'
    elif field.type == 'multiple_choice':  # Writing values in a proper format, depending of field type
        text = field.value.choice_names
    # elif field.type in ['date', 'due_date', 'creation_date', 'time', 'due_date_time']:
    #     text = str(field.value)
    elif field.type == 'person' or field.type == 'author':
        text = f'"{field.value.first_name}", "{field.value.last_name}", {field.value.email}'
    elif field.type == 'catalog':
        text = ', '.join(field.value.values)
    elif field.type == 'file':
        text = ''
        for file_description in field.value:
            text += f'Filename: {file_description.name} '
    elif field.type == 'form_link':
        text = field.value.subject
    elif field.type == 'title':
        text = field.name
    elif field.type == 'table':
        text = ''
        for row_id, row in enumerate(field.value):
            text += f'\n    Row {row_id}'
            for col_id, cell in enumerate(row.cells):
                text += f'\n        Column {col_id}, {field_to_str(cell, flat=True)}'
    else:
        text = field.value

    return text


def field_to_str(field, flat=False):
    """
    Text string for Field attributes (id, name) and value
    :param field: Field to process
    :param flat: If true, returns one string, otherwise split over several string
    :return: string
    """

    text = field_value_to_str(field)

    if text is None:
        return None

    if flat:
        return f'name: {field.name}, id: {field.id}, type: {field.type}, value: {text}'
    else:
        return f'name: {field.name}\n  id: {field.id:3}, type: {field.type:15}\n  value: {text}'


def print_field(field):
    """
    Printing field with standard function if not empty
    :param field: Field to print
    :return: None
    """

    field_str = field_to_str(field)
    if field_str is not None:
        print(field_str)


def extract_value(v):
    """Extract right value from given input."""
    return (
        None if (v is None or v.value is None) else
        v.value.choice_names[0] if v.type == 'multiple_choice' else
        v.value.first_name + ' ' + v.value.last_name if v.type == 'person' else
        v.value.values if v.type == 'catalog' else
        v.value)


class Fields:
    """
    Stores needed fields from form as class attributes
    Used in conjunction with get_fields_by_name() to get simple structure keeping fields from the task
    Attributes are created during initialization in two forms:
        class_variable.field_name - stores value
        class_variable._field_name - stores copy of the Field
    """

    def __init__(self, **kwargs):
        self.__dict__ = {k: extract_value(v) for k, v in kwargs.items()}
        self.__dict__.update({f'_{k}': v for k, v in kwargs.items()})

    def __str__(self):
        return create_object_str(self) + "\n"


def get_fields(fields_list, object_function, **kwargs):
    """
    Returns fields from the form task and attaches them as attributes to the field list structure
    Usage: first, create dictionary with attribute names as keys and field name as values, for example:

    Code example:
        f_dict = {"mode": "Working mode", "copy_source": "Number of a parent form"}
        fields = get_fields_by_name(self.task.flat_fields, **f_dict)

    Than you can use
        fields.mode (stores values of "Working mode" field)
        fields._mode (stores original "Working mode" field as pyrus-api structure)

    :param object_function: function to use (object_by_id or object_by_name)
    :param fields_list: list of fields (usually task.flat_fields)
    :param kwargs: dictionary, <key> = attribute name, <value> = field name
    :return: variable, which attributes set to the field values
    """

    return Fields(**{k: object_function(fields_list, v) for k, v in kwargs.items()})


def get_fields_by_name(fields_list, **kwargs):
    """ Uses get_fields with object_by_name"""

    return get_fields(fields_list, object_by_name, **kwargs)


def get_fields_by_id(fields_list, **kwargs):
    """ Uses get_fields with object_by_id"""

    return get_fields(fields_list, object_by_id, **kwargs)


def print_fields_type(form):
    """
    Print all fields in form with their types and type of their parent fields
    :param form:
    :return: None
    """

    print('Printing fields types...')
    for field in form.flat_fields:
        text = f'Field name/type: {field.name}/{field.type}'
        if field.parent_id is not None:
            parent_field = object_by_id(form.flat_fields, field.parent_id)
            text += f'\n  -->Parent field: {parent_field.name}/{parent_field.type}'
        print(text)
