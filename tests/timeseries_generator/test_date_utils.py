from datetime import datetime

import pytest

from lake_sandbox.timeseries_generator.date_utils import (
    group_dates_by_year,
    parse_date_range,
)


class TestParseDateRange:
    """Test cases for parse_date_range function using AAA pattern."""

    def test_parse_date_range_weekly_interval_single_month(self):
        """Test parsing date range with weekly intervals within a single month."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        expected_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 8),
            datetime(2023, 1, 15),
            datetime(2023, 1, 22),
            datetime(2023, 1, 29),
        ]

        # Act
        result = parse_date_range(start_date, end_date)

        # Assert
        assert result == expected_dates
        assert len(result) == 5

    def test_parse_date_range_weekly_interval_multiple_months(self):
        """Test parsing date range with weekly intervals across multiple months."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-03-01"

        # Act
        result = parse_date_range(start_date, end_date)

        # Assert
        assert result[0] == datetime(2023, 1, 1)
        assert result[-1] == datetime(2023, 2, 26)  # Last date within range
        assert len(result) == 9
        # Verify weekly intervals
        for i in range(1, len(result)):
            days_diff = (result[i] - result[i - 1]).days
            assert days_diff == 7

    def test_parse_date_range_custom_interval(self):
        """Test parsing date range with custom interval."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-01-15"
        interval_days = 3
        expected_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 4),
            datetime(2023, 1, 7),
            datetime(2023, 1, 10),
            datetime(2023, 1, 13),
        ]

        # Act
        result = parse_date_range(start_date, end_date, interval_days)

        # Assert
        assert result == expected_dates
        assert len(result) == 5

    def test_parse_date_range_single_day(self):
        """Test parsing date range for a single day."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-01-01"
        expected_dates = [datetime(2023, 1, 1)]

        # Act
        result = parse_date_range(start_date, end_date)

        # Assert
        assert result == expected_dates
        assert len(result) == 1

    def test_parse_date_range_large_interval(self):
        """Test parsing date range with interval larger than date range."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-01-03"
        interval_days = 30
        expected_dates = [datetime(2023, 1, 1)]

        # Act
        result = parse_date_range(start_date, end_date, interval_days)

        # Assert
        assert result == expected_dates
        assert len(result) == 1

    def test_parse_date_range_invalid_format_raises_error(self):
        """Test that invalid date format raises ValueError."""
        # Arrange
        start_date = "01-01-2023"  # Invalid format
        end_date = "2023-01-31"

        # Act & Assert
        with pytest.raises(ValueError):
            parse_date_range(start_date, end_date)

    def test_parse_date_range_end_before_start_returns_empty(self):
        """Test behavior when end date is before start date."""
        # Arrange
        start_date = "2023-01-31"
        end_date = "2023-01-01"
        expected_dates = []

        # Act
        result = parse_date_range(start_date, end_date)

        # Assert
        assert result == expected_dates
        assert len(result) == 0


class TestGroupDatesByYear:
    """Test cases for group_dates_by_year function using AAA pattern."""

    def test_group_dates_by_year_single_year(self):
        """Test grouping dates within a single year."""
        # Arrange
        dates = [
            datetime(2023, 1, 1),
            datetime(2023, 3, 15),
            datetime(2023, 6, 30),
            datetime(2023, 12, 31),
        ]
        expected = {
            2023: [
                datetime(2023, 1, 1),
                datetime(2023, 3, 15),
                datetime(2023, 6, 30),
                datetime(2023, 12, 31),
            ]
        }

        # Act
        result = group_dates_by_year(dates)

        # Assert
        assert result == expected
        assert len(result) == 1
        assert len(result[2023]) == 4

    def test_group_dates_by_year_multiple_years(self):
        """Test grouping dates across multiple years."""
        # Arrange
        dates = [
            datetime(2022, 12, 25),
            datetime(2022, 12, 31),
            datetime(2023, 1, 1),
            datetime(2023, 6, 15),
            datetime(2024, 1, 1),
            datetime(2024, 12, 31),
        ]
        expected = {
            2022: [datetime(2022, 12, 25), datetime(2022, 12, 31)],
            2023: [datetime(2023, 1, 1), datetime(2023, 6, 15)],
            2024: [datetime(2024, 1, 1), datetime(2024, 12, 31)],
        }

        # Act
        result = group_dates_by_year(dates)

        # Assert
        assert result == expected
        assert len(result) == 3
        assert len(result[2022]) == 2
        assert len(result[2023]) == 2
        assert len(result[2024]) == 2

    def test_group_dates_by_year_empty_list(self):
        """Test grouping empty list of dates."""
        # Arrange
        dates = []
        expected = {}

        # Act
        result = group_dates_by_year(dates)

        # Assert
        assert result == expected
        assert len(result) == 0

    def test_group_dates_by_year_single_date(self):
        """Test grouping single date."""
        # Arrange
        dates = [datetime(2023, 7, 4)]
        expected = {2023: [datetime(2023, 7, 4)]}

        # Act
        result = group_dates_by_year(dates)

        # Assert
        assert result == expected
        assert len(result) == 1
        assert len(result[2023]) == 1

    def test_group_dates_by_year_preserves_order_within_year(self):
        """Test that dates within each year maintain their chronological order."""
        # Arrange
        dates = [
            datetime(2023, 1, 1),
            datetime(2023, 3, 15),
            datetime(2023, 2, 14),  # Out of order
            datetime(2023, 12, 31),
        ]

        # Act
        result = group_dates_by_year(dates)

        # Assert
        assert result[2023] == [
            datetime(2023, 1, 1),
            datetime(2023, 3, 15),
            datetime(2023, 2, 14),
            datetime(2023, 12, 31),
        ]
        # Note: groupby preserves input order, doesn't sort


class TestIntegration:
    """Integration tests for date utility functions."""

    def test_parse_and_group_integration_weekly_data(self):
        """Test integration of parse_date_range and group_dates_by_year for weekly data."""
        # Arrange
        start_date = "2022-12-25"
        end_date = "2023-01-15"

        # Act
        dates = parse_date_range(start_date, end_date)
        grouped = group_dates_by_year(dates)

        # Assert
        assert 2022 in grouped
        assert 2023 in grouped
        assert len(grouped[2022]) == 1  # Only 2022-12-25
        assert len(grouped[2023]) == 3  # 2023-01-01, 2023-01-08, 2023-01-15
        assert grouped[2022][0] == datetime(2022, 12, 25)
        assert grouped[2023] == [
            datetime(2023, 1, 1),
            datetime(2023, 1, 8),
            datetime(2023, 1, 15),
        ]

    def test_parse_and_group_integration_full_year(self):
        """Test integration for a full year of weekly data."""
        # Arrange
        start_date = "2023-01-01"
        end_date = "2023-12-31"

        # Act
        dates = parse_date_range(start_date, end_date)
        grouped = group_dates_by_year(dates)

        # Assert
        assert len(grouped) == 1
        assert 2023 in grouped
        assert len(grouped[2023]) == 53  # 52 weeks + 1 for partial week
        assert grouped[2023][0] == datetime(2023, 1, 1)
        assert grouped[2023][-1] == datetime(2023, 12, 31)
