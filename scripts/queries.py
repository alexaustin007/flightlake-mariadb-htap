"""
Analytical Query Definitions

This module contains all the analytical queries used in the FlightLake project.
Each query is designed to demonstrate ColumnStore's performance advantages
for analytical workloads compared to InnoDB.

Query Categories:
- Hub analysis (busiest airports)
- Regional capacity distribution
- Route identification (underserved routes)
- Time series trends
- Market concentration analysis
- Distance-based segmentation
"""

# Query dictionary with metadata and SQL templates
QUERIES = {
    "top_10_hubs": {
        "name": "Top 10 Busiest Hubs",
        "description": "Identify airports handling the most seat capacity",
        "category": "Hub Analysis",
        "use_case": "Network planning - which hubs need expansion?",
        "sql": """
            SELECT
                origin_airport,
                origin_city,
                origin_country,
                COUNT(DISTINCT flight_number) AS num_routes,
                SUM(seats) AS total_seats,
                AVG(seats) AS avg_seats_per_flight
            FROM {table_name}
            WHERE flight_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
            GROUP BY origin_airport, origin_city, origin_country
            ORDER BY total_seats DESC
            LIMIT 10
        """
    },

    "regional_capacity": {
        "name": "Regional Capacity Distribution",
        "description": "Analyze airline capacity across world regions",
        "category": "Regional Analysis",
        "use_case": "Market analysis - where is capacity concentrated?",
        "sql": """
            SELECT
                origin_region,
                destination_region,
                COUNT(*) AS route_count,
                SUM(seats) AS total_capacity,
                AVG(distance_km) AS avg_distance,
                SUM(seats * distance_km) AS capacity_kilometers
            FROM {table_name}
            WHERE flight_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
            GROUP BY origin_region, destination_region
            ORDER BY total_capacity DESC
        """
    },

    "underserved_routes": {
        "name": "Underserved Routes",
        "description": "Find routes with low frequency or small aircraft",
        "category": "Route Analysis",
        "use_case": "Opportunity identification - routes needing more service",
        "sql": """
            SELECT
                origin_airport,
                destination_airport,
                distance_km,
                COUNT(*) AS flights_per_month,
                AVG(seats) AS avg_seats,
                SUM(seats) AS monthly_capacity
            FROM {table_name}
            WHERE flight_date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
            GROUP BY origin_airport, destination_airport, distance_km
            HAVING flights_per_month < 5 OR avg_seats < 150
            ORDER BY distance_km DESC
            LIMIT 50
        """
    },

    "capacity_trends": {
        "name": "Capacity Trends Over Time",
        "description": "Track how airline capacity changes month-over-month",
        "category": "Time Series Analysis",
        "use_case": "Trend analysis - identifying growth/decline patterns",
        "sql": """
            SELECT
                flight_year,
                flight_month,
                origin_region,
                COUNT(DISTINCT airline_code) AS num_airlines,
                COUNT(*) AS num_flights,
                SUM(seats) AS total_seats,
                AVG(distance_km) AS avg_distance
            FROM {table_name}
            WHERE flight_date >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)
            GROUP BY flight_year, flight_month, origin_region
            ORDER BY flight_year, flight_month, origin_region
        """
    },

    "distance_analysis": {
        "name": "Long-Haul vs Short-Haul Analysis",
        "description": "Compare capacity distribution by flight distance categories",
        "category": "Distance Segmentation",
        "use_case": "Fleet planning - understanding distance-based demand",
        "sql": """
            SELECT
                CASE
                    WHEN distance_km < 500 THEN 'Short-haul (<500km)'
                    WHEN distance_km < 1500 THEN 'Medium-haul (500-1500km)'
                    WHEN distance_km < 4000 THEN 'Long-haul (1500-4000km)'
                    ELSE 'Ultra-long-haul (>4000km)'
                END AS distance_category,
                COUNT(*) AS num_routes,
                AVG(seats) AS avg_seats,
                SUM(seats) AS total_seats,
                AVG(distance_km) AS avg_distance
            FROM {table_name}
            WHERE flight_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
            GROUP BY distance_category
            ORDER BY
                CASE distance_category
                    WHEN 'Short-haul (<500km)' THEN 1
                    WHEN 'Medium-haul (500-1500km)' THEN 2
                    WHEN 'Long-haul (1500-4000km)' THEN 3
                    ELSE 4
                END
        """
    }
}


def get_query(query_key: str, table_name: str) -> str:
    """
    Get a formatted SQL query for a specific table.

    Args:
        query_key: The key identifying the query (e.g., 'top_10_hubs')
        table_name: The table name to substitute in the query

    Returns:
        Formatted SQL query string

    Raises:
        KeyError: If query_key is not found
    """
    if query_key not in QUERIES:
        raise KeyError(f"Query '{query_key}' not found. Available queries: {list(QUERIES.keys())}")

    return QUERIES[query_key]['sql'].format(table_name=table_name)


def get_query_info(query_key: str) -> dict:
    """
    Get metadata about a query.

    Args:
        query_key: The key identifying the query

    Returns:
        Dictionary with query metadata (name, description, category, use_case)

    Raises:
        KeyError: If query_key is not found
    """
    if query_key not in QUERIES:
        raise KeyError(f"Query '{query_key}' not found")

    return {k: v for k, v in QUERIES[query_key].items() if k != 'sql'}


def list_queries() -> list:
    """
    Get a list of all available query keys.

    Returns:
        List of query keys
    """
    return list(QUERIES.keys())


def list_queries_by_category() -> dict:
    """
    Get queries organized by category.

    Returns:
        Dictionary mapping category names to lists of query keys
    """
    categories = {}
    for key, info in QUERIES.items():
        category = info.get('category', 'Uncategorized')
        if category not in categories:
            categories[category] = []
        categories[category].append(key)
    return categories


def print_query_catalog():
    """
    Print a formatted catalog of all available queries.
    """
    print("=" * 80)
    print("FlightLake Query Catalog")
    print("=" * 80)
    print()

    for idx, (key, info) in enumerate(QUERIES.items(), 1):
        print(f"{idx}. {info['name']} (Key: {key})")
        print(f"   Category: {info['category']}")
        print(f"   Description: {info['description']}")
        print(f"   Use Case: {info['use_case']}")
        print()


if __name__ == "__main__":
    # Print query catalog
    print_query_catalog()

    # Test query retrieval
    print("=" * 80)
    print("Testing Query Retrieval")
    print("=" * 80)
    print()

    test_key = "top_10_hubs"
    test_table = "routes_innodb"

    print(f"Query Key: {test_key}")
    print(f"Table: {test_table}")
    print()
    print("SQL:")
    print(get_query(test_key, test_table))
    print()

    # List queries by category
    print("=" * 80)
    print("Queries by Category")
    print("=" * 80)
    print()

    categories = list_queries_by_category()
    for category, query_keys in categories.items():
        print(f"{category}:")
        for key in query_keys:
            print(f"  - {key}: {QUERIES[key]['name']}")
        print()
