import pandas as pd

from src.movies.main import main


class TestMoviesPipeline:
    def test_integration_pipeline(self):
        """Integration test: Run the full pipeline and compare with expected output"""
        main('config/test.yaml')

        actual_df = pd.read_csv('target/test/gold/movies_unified.csv')
        expected_df = pd.read_csv('test/data/integration_test/expected_output.csv')

        pd.testing.assert_frame_equal(actual_df, expected_df)