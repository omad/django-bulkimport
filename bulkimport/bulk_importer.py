from openpyxl import load_workbook
from django import forms
from django.db.models import DateField, CharField, TextField
from dateutil.parser import parse
from collections import namedtuple
from django.core import management
import logging

logger = logging.getLogger(__name__)


class BulkImportForm(forms.Form):
    spreadsheet = forms.FileField()

ModelMapping = namedtuple('ModelMapping', ['model', 'mapping',
                          'unique_column', 'unique_field'])


class BulkImporterException(Exception):
    pass


class MissingUniqueHeaderException(BulkImporterException):
    pass

class MappingProcessor:
    """Mappings required for processing one row into one model
    """
    model = None
    mapping = None
    headers = None

class BulkDataImportHandler:
    """
    Example Usage:

    bi = BulkDataImportHandler()
    bi.add_mapping(DjangoModel, {"SpreadsheetHeader": "model_field", ...})
    imported_records = bi.process_spreadsheet(spreadsheet)


    """
    def __init__(self):
        self.mappings = []
        self.linking_func = None
        self.func_mappings = []
        self.header_row = 0
        self.first_data_row = 1

    def add_mapping(self, model, mapping, unique_column=None,
                    unique_field=None):
        """
        Specify a row <-> model mapping

        `mapping` should be a dictionary with keys as the headings of columns
        in the spreadsheet to be processed, and values as the names of
        matching fields on the supplied `model`.

        Processing rows of data can take two forms, if `unique_column` and
        `unique_field` are supplied, a lookup is performed which can update
        existing records.
        Otherwise, or if no record is found a new model of the supplied type
        is created, and appropriate data fields from the spreadsheet
        are set onto the model.

        The model is then saved into the database.

        :param model: Django database model that will be populated
        :param unique_column: (optional) The name of the unique column in
                                         the spreadsheet
        :param unique_field: (optional) The name of the unique column in the
                                        DB model
        """
        if unique_column:
            unique_column = unique_column.lower()
        self.mappings.append(ModelMapping(model, mapping, unique_column,
                             unique_field))

    def add_function_mapping(self, function):
        """
        Specify a function to run for each row in a spreadsheet

        Can be used for updating existing records, or some other purpose.

        Supplied function must take arguments:
            headers = list of header strings from spreadsheet
            values = list of values from a row of spreadsheet
        """
        self.func_mappings.append(function)

    def add_linking_function(self, function):
        """
        Add function called after each row

        Typically used to link models together if there
        are multiple created for each row of data

        Called with the created objects, in order based on added mappings.
        """
        self.linking_func = function

    def process_spreadsheet(self, spreadsheet, rebuild_search_index=False):
        """
        Open the spreadsheet file and process rows one at a time.

        Also flushes and rebuilds the search index
        """
        wb = load_workbook(spreadsheet)
        sheet = wb.get_sheet_by_name(wb.get_sheet_names()[0])

        data = sheet.range(sheet.calculate_dimension())
        headers = [v.value.lower() for v in data[self.header_row] if v.value]
        all_affected_records = []
        for row in data[self.first_data_row:]:
            vals = [v.value for v in row]
            if vals[0] == headers[0]:
                # repeated headers
                continue
            affected_records = self.process_row(headers, vals)
            all_affected_records.append(affected_records)

        # Update Search index
        if rebuild_search_index:
            management.call_command('rebuild_index', interactive=False)

        return all_affected_records

    def process_row(self, headers, vals):
        """
        Takes a list of headers and values, and turns them into a new model
        record and saves the model to the database.

        Looks up mapping data that has been added with `add_mapping`
        """
        affected_records = []
        for model, mapping, unique_column, unique_field in self.mappings:

            instance = self._find_or_create_record(
                model,
                unique_column,
                unique_field,
                headers,
                vals
            )

            for col, field in mapping.items():
                try:
                    value = vals[headers.index(col.lower())]
                    value = self.process_value(instance, field, value)
                    setattr(instance, field, value)
                except ValueError:
                    pass
            affected_records.append(instance)
            if self.linking_func and len(affected_records) > 1:
                self.linking_func(*affected_records)
            instance.save()

        for func_mapping in self.func_mappings:
            func_mapping(headers, vals)

        return affected_records

    @staticmethod
    def _find_or_create_record(model, unique_column, field_name, headers, vals):
        """Return an instance of model.

        Either find an extisting model based on unique column and field, or
        create a new one.
        """
        if unique_column:
            try:
                value = vals[headers.index(unique_column)]
            except ValueError:
                raise MissingUniqueHeaderException(
                    "Expected a unique column header '%s' to be in "
                    "the uploaded spreadsheet" % unique_column)

            try:
                instance = model.objects.get(**{field_name: value})
            except model.DoesNotExist:
                instance = model()
        else:
            instance = model()
        return instance

    @staticmethod
    def process_value(instance, field, value):
        field = instance._meta.get_field(field)
        if isinstance(field, DateField):
            try:
                value = parse(value, dayfirst=True)
            except:
                value = None
        if isinstance(field, CharField) or isinstance(field, TextField):
            if not value:
                value = ''
        return value
