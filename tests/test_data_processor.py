import unittest
import pandas as pd
from pandas.api import types as ptypes
from app.services.data_processor import DataProcessor


class TestDataProcessor(unittest.TestCase):
    def test_process_dataset_fills_missing_values(self):
        df = pd.DataFrame({
            'num': [1.0, None, 3.0],
            'cat': ['a', None, 'a'],
            'date_str': ['2024-01-01', '2024-01-02', None]
        })

        processor = DataProcessor()
        cleaned_df, lineage = processor.process_dataset(df, {})

        self.assertEqual(cleaned_df['num'].tolist(), [1.0, 2.0, 3.0])
        self.assertEqual(cleaned_df['cat'].tolist(), ['a', 'missing', 'a'])
        self.assertTrue(ptypes.is_datetime64_any_dtype(cleaned_df['date_str']))
        self.assertEqual(lineage['original_shape'], (3, 3))
        self.assertEqual(lineage['final_shape'], cleaned_df.shape)

    def test_process_dataset_copies_input(self):
        df = pd.DataFrame({'value': [None, 2, 3]}, dtype=object)
        processor = DataProcessor()

        _ = processor.process_dataset(df, {})
        self.assertEqual(df['value'].tolist(), [None, 2, 3])


if __name__ == '__main__':
    unittest.main()
