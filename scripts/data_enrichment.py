"""
Data Enrichment Module

This script downloads and enriches OpenFlights dataset for FlightLake analytics.
It transforms raw route data into analytics-ready format with:
- Geographic coordinates and distances
- Time series data (24 months historical)
- Regional mappings
- Seat capacity estimations
- Data quality filtering
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import random
from pathlib import Path
from typing import Dict, Tuple

from config import (
    RAW_DATA_DIR, PROCESSED_DATA_DIR, OPENFLIGHTS_URLS,
    ENRICHMENT_CONFIG, REGION_MAPPING, AIRCRAFT_SEAT_MAPPING
)
from utils import (
    calculate_haversine_distance, get_quarter,
    generate_date_range, get_seat_capacity_for_distance,
    progress_bar
)


class FlightDataEnricher:
    """
    Enriches raw OpenFlights data for HTAP analytics.

    This class handles downloading, parsing, enriching, and exporting
    airline route data with geographic, temporal, and capacity information.
    """

    def __init__(self):
        """Initialize the data enricher."""
        self.airports = None
        self.airlines = None
        self.routes = None
        self.enriched_data = []

        # Ensure directories exist
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def download_datasets(self) -> None:
        """
        Download OpenFlights datasets from GitHub.

        Downloads airlines.dat, airports.dat, and routes.dat files.
        """
        print("Downloading OpenFlights datasets...")
        print("-" * 50)

        for name, url in OPENFLIGHTS_URLS.items():
            output_path = RAW_DATA_DIR / f"{name}.dat"

            if output_path.exists():
                print(f"  {name}.dat already exists, skipping download")
                continue

            try:
                response = requests.get(url)
                response.raise_for_status()

                with open(output_path, 'wb') as f:
                    f.write(response.content)

                print(f"  Downloaded {name}.dat")
            except Exception as e:
                print(f"  Failed to download {name}.dat: {e}")
                raise

        print()

    def load_airports(self) -> pd.DataFrame:
        """
        Load and parse airports dataset.

        Returns:
            DataFrame with airport information
        """
        print("Loading airports dataset...")

        columns = [
            'airport_id', 'name', 'city', 'country', 'iata_code',
            'icao_code', 'latitude', 'longitude', 'altitude',
            'timezone', 'dst', 'tz_database', 'type', 'source'
        ]

        airports = pd.read_csv(
            RAW_DATA_DIR / 'airports.dat',
            header=None,
            names=columns,
            na_values=['\\N']
        )

        # Filter for commercial airports with IATA codes
        airports = airports[airports['iata_code'].notna()]
        airports = airports[airports['type'] == 'airport']

        print(f"  Loaded {len(airports)} airports")
        return airports

    def load_airlines(self) -> pd.DataFrame:
        """
        Load and parse airlines dataset.

        Returns:
            DataFrame with airline information
        """
        print("Loading airlines dataset...")

        columns = [
            'airline_id', 'name', 'alias', 'iata_code', 'icao_code',
            'callsign', 'country', 'active'
        ]

        airlines = pd.read_csv(
            RAW_DATA_DIR / 'airlines.dat',
            header=None,
            names=columns,
            na_values=['\\N']
        )

        # Filter for active airlines
        airlines = airlines[airlines['active'] == 'Y']

        print(f"  Loaded {len(airlines)} airlines")
        return airlines

    def load_routes(self) -> pd.DataFrame:
        """
        Load and parse routes dataset.

        Returns:
            DataFrame with route information
        """
        print("Loading routes dataset...")

        columns = [
            'airline_code', 'airline_id', 'origin_airport', 'origin_airport_id',
            'destination_airport', 'destination_airport_id', 'codeshare',
            'stops', 'equipment'
        ]

        routes = pd.read_csv(
            RAW_DATA_DIR / 'routes.dat',
            header=None,
            names=columns,
            na_values=['\\N']
        )

        print(f"  Loaded {len(routes)} routes")
        return routes

    def get_country_region(self, country_code: str) -> str:
        """
        Map country code to geographic region.

        Args:
            country_code: ISO country code

        Returns:
            Region name
        """
        for region, countries in REGION_MAPPING.items():
            if country_code in countries:
                return region
        return 'Other'

    def enrich_routes(self) -> pd.DataFrame:
        """
        Enrich route data with airports, airlines, and calculated fields.

        Returns:
            DataFrame with enriched route data
        """
        print("Enriching route data...")
        print("-" * 50)

        # Create airport lookup dictionaries
        airport_lookup = self.airports.set_index('iata_code').to_dict('index')
        airline_lookup = self.airlines.set_index('iata_code').to_dict('index')

        enriched_routes = []
        skipped = 0

        total = len(self.routes)

        for idx, route in self.routes.iterrows():
            if idx % 1000 == 0:
                print(f"  {progress_bar(idx, total, 30)} Processing routes...")

            # Get origin airport data
            origin_code = route['origin_airport']
            if origin_code not in airport_lookup:
                skipped += 1
                continue

            origin = airport_lookup[origin_code]

            # Get destination airport data
            dest_code = route['destination_airport']
            if dest_code not in airport_lookup:
                skipped += 1
                continue

            dest = airport_lookup[dest_code]

            # Get airline data
            airline_code = route['airline_code']
            airline = airline_lookup.get(airline_code, {})

            # Calculate distance
            distance = calculate_haversine_distance(
                origin['latitude'], origin['longitude'],
                dest['latitude'], dest['longitude']
            )

            # Generate seat capacity based on distance
            seats = get_seat_capacity_for_distance(distance)

            # Generate flight number (simplified)
            flight_number = f"{airline_code}{random.randint(100, 9999)}"

            # Build enriched record
            enriched_record = {
                'airline_code': airline_code,
                'airline_name': airline.get('name', 'Unknown'),
                'flight_number': flight_number,
                'origin_airport': origin_code,
                'origin_city': origin['city'],
                'origin_country': origin['country'],
                'origin_region': self.get_country_region(origin['country']),
                'origin_latitude': origin['latitude'],
                'origin_longitude': origin['longitude'],
                'destination_airport': dest_code,
                'destination_city': dest['city'],
                'destination_country': dest['country'],
                'destination_region': self.get_country_region(dest['country']),
                'destination_latitude': dest['latitude'],
                'destination_longitude': dest['longitude'],
                'distance_km': distance,
                'seats': seats,
                'aircraft_type': route['equipment'].split()[0] if pd.notna(route['equipment']) else 'Unknown',
                'codeshare': 1 if route['codeshare'] == 'Y' else 0,
                'stops': int(route['stops']) if pd.notna(route['stops']) else 0
            }

            enriched_routes.append(enriched_record)

        print(f"  {progress_bar(total, total, 30)} Processing routes...")
        print(f"\n  Enriched {len(enriched_routes)} routes")
        print(f"  Skipped {skipped} routes (missing airport data)")

        return pd.DataFrame(enriched_routes)

    def generate_time_series(self, routes_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate time series data for each route (24 months historical).

        Args:
            routes_df: DataFrame with enriched routes

        Returns:
            DataFrame with time series records
        """
        print("\nGenerating time series data...")
        print("-" * 50)

        # Generate date range
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=ENRICHMENT_CONFIG['history_months'])
        dates = generate_date_range(start_date, end_date, freq='MS')

        print(f"  Date range: {start_date.date()} to {end_date.date()}")
        print(f"  Months: {len(dates)}")

        time_series_data = []
        total = len(routes_df) * len(dates)
        processed = 0

        for _, route in routes_df.iterrows():
            for date in dates:
                record = route.to_dict()
                record.update({
                    'flight_date': date.date(),
                    'flight_year': date.year,
                    'flight_month': date.month,
                    'flight_quarter': get_quarter(date.month)
                })
                time_series_data.append(record)

                processed += 1
                if processed % 10000 == 0:
                    print(f"  {progress_bar(processed, total, 30)} Generating records...")

        print(f"  {progress_bar(total, total, 30)} Generating records...")
        print(f"\n  Generated {len(time_series_data)} time series records")

        return pd.DataFrame(time_series_data)

    def export_to_csv(self, df: pd.DataFrame, filename: str = 'routes_enriched.csv') -> Path:
        """
        Export enriched data to CSV.

        Args:
            df: DataFrame to export
            filename: Output filename

        Returns:
            Path to exported file
        """
        output_path = PROCESSED_DATA_DIR / filename

        print(f"\nExporting to {output_path}...")

        df.to_csv(output_path, index=False)

        # Print file info
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  Exported {len(df):,} rows")
        print(f"  File size: {file_size_mb:.1f} MB")

        return output_path

    def run(self) -> Path:
        """
        Execute the full data enrichment pipeline.

        Returns:
            Path to the enriched CSV file
        """
        print("=" * 70)
        print("FlightLake Data Enrichment Pipeline")
        print("=" * 70)
        print()

        # Step 1: Download datasets
        self.download_datasets()

        # Step 2: Load datasets
        self.airports = self.load_airports()
        self.airlines = self.load_airlines()
        self.routes = self.load_routes()
        print()

        # Step 3: Enrich routes
        enriched_routes = self.enrich_routes()

        # Step 4: Generate time series
        time_series_df = self.generate_time_series(enriched_routes)

        # Step 5: Export to CSV
        output_path = self.export_to_csv(time_series_df)

        print()
        print("=" * 70)
        print("Data enrichment complete!")
        print("=" * 70)
        print()
        print(f"Next steps:")
        print(f"  1. Load data into InnoDB:")
        print(f"     python scripts/load_data.py --engine innodb")
        print(f"  2. Load data into ColumnStore:")
        print(f"     python scripts/load_data.py --engine columnstore")
        print(f"  3. Run benchmarks:")
        print(f"     python scripts/benchmark.py")
        print()

        return output_path


def main():
    """Main entry point for data enrichment."""
    enricher = FlightDataEnricher()
    enricher.run()


if __name__ == "__main__":
    main()
