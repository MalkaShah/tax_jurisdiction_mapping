# test_setup.py
import geopandas as gpd
import pandas as pd
import psycopg2
from shapely.geometry import Point

# Test reading your shapefile
def test_gis_setup():
    try:
        # Update this path to your shapefile location
        states = gpd.read_file('path_to_your_states_shapefile.shp')
        print(f"Successfully loaded {len(states)} states")
        print("\nColumns in the dataset:")
        print(states.columns)
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_gis_setup()