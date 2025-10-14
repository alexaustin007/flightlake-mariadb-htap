"""
Utility Functions

This module provides utility functions for the FlightLake project,
including time formatting, performance calculations, result comparisons,
and data processing helpers.
"""

from typing import List, Tuple, Any
import math
from datetime import datetime, timedelta


def format_time(seconds: float) -> str:
    """
    Format time duration in human-readable format.

    Args:
        seconds: Time duration in seconds

    Returns:
        Formatted string (e.g., "500ms", "2.3s", "1.5μs")

    Examples:
        >>> format_time(0.0005)
        '500μs'
        >>> format_time(0.5)
        '500ms'
        >>> format_time(2.345)
        '2.345s'
    """
    if seconds < 0.001:
        return f"{seconds * 1000000:.0f}μs"
    elif seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    else:
        return f"{seconds:.3f}s"


def calculate_speedup(time1: float, time2: float) -> float:
    """
    Calculate speedup ratio between two execution times.

    Args:
        time1: First execution time (baseline)
        time2: Second execution time (optimized)

    Returns:
        Speedup ratio (time1 / time2)

    Examples:
        >>> calculate_speedup(10.0, 2.0)
        5.0
        >>> calculate_speedup(5.0, 5.0)
        1.0
    """
    if time2 == 0:
        return float('inf')
    return time1 / time2


def compare_results(results1: List[Tuple], results2: List[Tuple],
                   tolerance: float = 0.001) -> bool:
    """
    Compare two result sets for equality (with floating point tolerance).

    Args:
        results1: First result set (list of tuples)
        results2: Second result set (list of tuples)
        tolerance: Tolerance for floating point comparison

    Returns:
        True if results match, False otherwise

    Examples:
        >>> compare_results([(1, 2.0)], [(1, 2.001)], tolerance=0.01)
        True
        >>> compare_results([(1, 'a')], [(2, 'a')])
        False
    """
    if results1 is None or results2 is None:
        return False

    if len(results1) != len(results2):
        return False

    # Sort both result sets to ensure order doesn't matter
    sorted1 = sorted(results1)
    sorted2 = sorted(results2)

    # Compare with tolerance for floating point values
    for row1, row2 in zip(sorted1, sorted2):
        if len(row1) != len(row2):
            return False

        for val1, val2 in zip(row1, row2):
            if isinstance(val1, float) and isinstance(val2, float):
                if abs(val1 - val2) > tolerance:
                    return False
            elif val1 != val2:
                return False

    return True


def format_bytes(bytes_size: int) -> str:
    """
    Format byte size in human-readable format.

    Args:
        bytes_size: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 KB", "500 MB", "2.3 GB")

    Examples:
        >>> format_bytes(1024)
        '1.0 KB'
        >>> format_bytes(1536)
        '1.5 KB'
        >>> format_bytes(1048576)
        '1.0 MB'
    """
    if bytes_size == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(bytes_size, 1024)))
    p = math.pow(1024, i)
    s = round(bytes_size / p, 1)

    return f"{s} {size_names[i]}"


def format_number(number: int) -> str:
    """
    Format large numbers with thousand separators.

    Args:
        number: Integer to format

    Returns:
        Formatted string with commas

    Examples:
        >>> format_number(1000)
        '1,000'
        >>> format_number(1234567)
        '1,234,567'
    """
    return f"{number:,}"


def calculate_haversine_distance(lat1: float, lon1: float,
                                 lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth.

    Uses the Haversine formula to calculate distance in kilometers.

    Args:
        lat1: Latitude of first point (degrees)
        lon1: Longitude of first point (degrees)
        lat2: Latitude of second point (degrees)
        lon2: Longitude of second point (degrees)

    Returns:
        Distance in kilometers

    Examples:
        >>> # Distance from New York to Los Angeles (approx 3944 km)
        >>> calculate_haversine_distance(40.7128, -74.0060, 34.0522, -118.2437)
        3944.4...
    """
    # Earth's radius in kilometers
    R = 6371.0

    # Convert degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))

    distance = R * c
    return round(distance, 2)


def get_quarter(month: int) -> int:
    """
    Get the quarter (1-4) for a given month.

    Args:
        month: Month number (1-12)

    Returns:
        Quarter number (1-4)

    Examples:
        >>> get_quarter(1)
        1
        >>> get_quarter(6)
        2
        >>> get_quarter(12)
        4
    """
    return (month - 1) // 3 + 1


def generate_date_range(start_date: datetime, end_date: datetime,
                        freq: str = 'MS') -> List[datetime]:
    """
    Generate a list of dates between start and end dates.

    Args:
        start_date: Start date
        end_date: End date
        freq: Frequency ('MS' for month start, 'D' for daily)

    Returns:
        List of datetime objects

    Examples:
        >>> from datetime import datetime
        >>> dates = generate_date_range(datetime(2023, 1, 1), datetime(2023, 3, 1))
        >>> len(dates)
        3
    """
    dates = []
    current = start_date

    if freq == 'MS':  # Month start
        while current <= end_date:
            dates.append(current)
            # Move to next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
    elif freq == 'D':  # Daily
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

    return dates


def get_distance_category(distance_km: float) -> str:
    """
    Categorize flight distance into short/medium/long/ultra-long haul.

    Args:
        distance_km: Distance in kilometers

    Returns:
        Category string

    Examples:
        >>> get_distance_category(400)
        'Short-haul (<500km)'
        >>> get_distance_category(3000)
        'Long-haul (1500-4000km)'
    """
    if distance_km < 500:
        return 'Short-haul (<500km)'
    elif distance_km < 1500:
        return 'Medium-haul (500-1500km)'
    elif distance_km < 4000:
        return 'Long-haul (1500-4000km)'
    else:
        return 'Ultra-long-haul (>4000km)'


def get_seat_capacity_for_distance(distance_km: float,
                                   min_seats: int = 100,
                                   max_seats: int = 550) -> int:
    """
    Estimate seat capacity based on flight distance.

    Longer flights typically use larger aircraft.

    Args:
        distance_km: Distance in kilometers
        min_seats: Minimum seat capacity
        max_seats: Maximum seat capacity

    Returns:
        Estimated seat capacity

    Examples:
        >>> get_seat_capacity_for_distance(500)  # Short haul
        100
        >>> get_seat_capacity_for_distance(10000)  # Ultra long haul
        550
    """
    import random

    if distance_km < 500:
        return random.randint(100, 180)
    elif distance_km < 1500:
        return random.randint(120, 220)
    elif distance_km < 4000:
        return random.randint(150, 300)
    else:
        return random.randint(250, max_seats)


def progress_bar(current: int, total: int, width: int = 50) -> str:
    """
    Generate a text-based progress bar.

    Args:
        current: Current progress value
        total: Total value
        width: Width of progress bar in characters

    Returns:
        Progress bar string

    Examples:
        >>> progress_bar(50, 100, 20)
        '[==========          ] 50%'
    """
    if total == 0:
        return '[' + ' ' * width + '] 0%'

    percent = current / total
    filled = int(width * percent)
    bar = '=' * filled + ' ' * (width - filled)
    return f"[{bar}] {int(percent * 100)}%"


def chunks(lst: List[Any], chunk_size: int):
    """
    Split a list into chunks of specified size.

    Args:
        lst: List to split
        chunk_size: Size of each chunk

    Yields:
        Chunks of the list

    Examples:
        >>> list(chunks([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


if __name__ == "__main__":
    # Test utility functions
    print("Testing Utility Functions")
    print("=" * 50)

    # Test time formatting
    print("\nTime Formatting:")
    print(f"  0.0005s -> {format_time(0.0005)}")
    print(f"  0.5s -> {format_time(0.5)}")
    print(f"  2.345s -> {format_time(2.345)}")

    # Test speedup calculation
    print("\nSpeedup Calculation:")
    print(f"  10s vs 2s -> {calculate_speedup(10.0, 2.0):.1f}x")
    print(f"  5s vs 5s -> {calculate_speedup(5.0, 5.0):.1f}x")

    # Test distance calculation
    print("\nHaversine Distance:")
    print(f"  NYC to LAX -> {calculate_haversine_distance(40.7128, -74.0060, 34.0522, -118.2437):.0f} km")

    # Test byte formatting
    print("\nByte Formatting:")
    print(f"  1024 bytes -> {format_bytes(1024)}")
    print(f"  1048576 bytes -> {format_bytes(1048576)}")

    # Test number formatting
    print("\nNumber Formatting:")
    print(f"  1234567 -> {format_number(1234567)}")

    # Test progress bar
    print("\nProgress Bar:")
    print(f"  {progress_bar(50, 100, 30)}")

    print("\nAll tests passed!")
