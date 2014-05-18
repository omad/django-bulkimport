import unittest
import mock
from bulkimport import BulkDataImportHandler, MissingUniqueHeaderException
from django.db import models
from django.contrib import messages

class MyModel(models.Model):
    pass


class Person(models.Model):
    first_name = models.CharField(max_length=100, blank=True)
    last_name = models.CharField(max_length=100, blank=True)
    age = models.CharField(max_length=100, blank=True)
    extra = models.CharField(max_length=100, blank=True)

    def save(*args, **kwargs):
        pass

class SimpleTest(unittest.TestCase):


    def test_process_row_single(self):
        mapping = {'one': 'one'}

        save_mock = mock.Mock(return_value=None)

        model = mock.Mock(MyModel)
        model.return_value.save = save_mock

        bdih = BulkDataImportHandler()
        bdih.add_mapping(model, mapping)

        headers = ['one', 'two']
        vals = ['val1', 'spot']

        affected_records, used_cols = bdih.process_row(headers, vals)
        new_record = affected_records[0]

        # make sure class was created
        self.assertEqual(model.call_count, 1)

        # now make sure save was called once
        self.assertEqual(save_mock.call_count, 1)

        self.assertEqual('val1', new_record.one)

    def test_process_row_multi(self):
        mapping_1 = {'one': 'one'}
        save_mock_1 = mock.Mock(return_value=None)
        model_1 = mock.Mock(MyModel)
        model_1.return_value.save = save_mock_1

        mapping_2 = {'two': 'two'}
        save_mock_2 = mock.Mock(return_value=None)
        model_2 = mock.Mock(MyModel)
        model_2.return_value.save = save_mock_2

        linking_func = mock.Mock()

        bdih = BulkDataImportHandler()
        bdih.add_mapping(model_1, mapping_1)
        bdih.add_mapping(model_2, mapping_2)
        bdih.add_linking_function(linking_func)

        headers = ['one', 'two']
        vals = ['val1', 'spot']

        affected_records, stats = bdih.process_row(headers, vals)
        result_1, result_2 = affected_records

        # make sure one class each was created
        self.assertEqual(model_1.call_count, 1)
        self.assertEqual(model_2.call_count, 1)

        # now make sure each save was called once
        self.assertEqual(save_mock_1.call_count, 1)
        self.assertEqual(save_mock_2.call_count, 1)

        # check values were saved onto each instance
        self.assertEqual('val1', result_1.one)
        self.assertEqual('spot', result_2.two)

        # check the linking function was called
        self.assertTrue(linking_func.called)
        linking_func.assert_called_with(result_1, result_2)

    def test_read_simple_spreadsheet(self):
        """
        Load in a simple spreadsheet
        """
        spreadsheet = 'bulkimport/testdata/names.xlsx'

        bi = BulkDataImportHandler()
        bi.add_mapping(Person, {
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Age': 'age'
            })

        affected_records, stats = bi.process_spreadsheet(spreadsheet)

        self.assertEqual(3, len(affected_records))
        self.assertEqual('Bob', affected_records[0][0].first_name)
        self.assertEqual(50, affected_records[2][0].age)

    def test_unique_field(self):
        # Contains fields 'First Name', "Last Name', 'Age', 'ID'
        spreadsheet = 'bulkimport/testdata/names.xlsx'

        bi = BulkDataImportHandler()
        bi.add_mapping(Person, {
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Age': 'age',
            'ID': 'id'
            }, 'ID', 'id')

        affected_records, stats = bi.process_spreadsheet(spreadsheet)

        self.assertEqual(3, len(affected_records))
        self.assertEqual('Bob', affected_records[0][0].first_name)
        self.assertEqual(50, affected_records[2][0].age)

    def test_missing_unique_field(self):
        # Contains fields 'First Name', "Last Name', 'Age', 'ID'
        spreadsheet = 'bulkimport/testdata/names.xlsx'

        bi = BulkDataImportHandler()
        bi.add_mapping(Person, {
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Age': 'age'
            }, 'PersonID', 'id')

        with self.assertRaises(MissingUniqueHeaderException):
            affected_records, stats = bi.process_spreadsheet(spreadsheet)


    def test_read_spreadsheet_case_insensitive(self):
        """
        Test the column names to be mapped are case insensitive
        """
        spreadsheet = 'bulkimport/testdata/names.xlsx'

        bi = BulkDataImportHandler()
        bi.add_mapping(Person, {
            'First name': 'first_name',
            'Last NaMe': 'last_name',
            'Age': 'age'
            })

        affected_records, stats = bi.process_spreadsheet(spreadsheet)

        self.assertEqual(3, len(affected_records))
        self.assertEqual('Bob', affected_records[0][0].first_name)
        self.assertEqual(50, affected_records[2][0].age)


    def test_mapped_column_no_data(self):
        """
        Test the column names to be mapped are case insensitive
        """
        spreadsheet = 'bulkimport/testdata/names.xlsx'

        bi = BulkDataImportHandler()
        bi.add_mapping(Person, {
            'First name': 'first_name',
            'Last NaMe': 'last_name',
            'Age': 'age',
            'nonexistant column': 'extra'
            })

        affected_records, stats = bi.process_spreadsheet(spreadsheet)

        self.assertEqual(3, len(affected_records))
        self.assertEqual('Bob', affected_records[0][0].first_name)
        self.assertEqual(50, affected_records[2][0].age)



