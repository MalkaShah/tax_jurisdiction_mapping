import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import os

def analyze_states():
    try:
        # Path to your shapefile
        states_path = r"D:/projects/tax_jurisdiction_mapping/data/raw/tl_2023_us_state.shp"
        
        # Read the shapefile
        states = gpd.read_file(states_path)
        
        # 1. Basic Information
        print("\n=== Basic Dataset Information ===")
        print(f"Total number of states: {len(states)}")
        print(f"\nColumns available: {states.columns.tolist()}")
        
        # 2. State Analysis
        print("\n=== State Analysis ===")
        # Calculate areas in square kilometers
        states['area_km2'] = states['ALAND'].astype(float) / 1_000_000  # Convert to km²
        
        # Sort states by area
        largest_states = states.nlargest(5, 'area_km2')
        print("\nFive largest states by area:")
        print(largest_states[['NAME', 'area_km2']])
        
        # 3. Create Visualizations
        # Basic map
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Plot states
        states.plot(ax=ax1, edgecolor='black', facecolor='lightgray')
        ax1.set_title('US States')
        ax1.axis('off')
        
        # Choropleth map based on land area
        states.plot(column='area_km2', 
                   ax=ax2,
                   legend=True,
                   legend_kwds={'label': 'Area (km²)'},
                   cmap='YlOrRd')
        ax2.set_title('States by Area')
        ax2.axis('off')
        
        # Save the visualization
        output_path = r"D:/projects/tax_jurisdiction_mapping/documentation/state_analysis.png"
        plt.savefig(output_path)
        plt.close()
        
        # 4. Export summary statistics
        summary_stats = pd.DataFrame({
            'Total_States': [len(states)],
            'Total_Land_Area_km2': [states['area_km2'].sum()],
            'Average_State_Area_km2': [states['area_km2'].mean()],
            'Largest_State': [states.loc[states['area_km2'].idxmax(), 'NAME']],
            'Smallest_State': [states.loc[states['area_km2'].idxmin(), 'NAME']]
        })
        
        # Export to CSV
        stats_path = r"D:/projects/tax_jurisdiction_mapping/documentation/state_statistics.csv"
        summary_stats.to_csv(stats_path, index=False)
        
        return states
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_state_info(states_gdf, state_name):
    """Get detailed information for a specific state"""
    state_info = states_gdf[states_gdf['NAME'] == state_name].iloc[0]
    print(f"\n=== Information for {state_name} ===")
    print(f"FIPS Code: {state_info['STATEFP']}")
    print(f"Postal Code: {state_info['STUSPS']}")
    print(f"Area (km²): {state_info['area_km2']:.2f}")
    return state_info

if __name__ == "__main__":
    # Load and analyze states
    states_data = analyze_states()
    
    if states_data is not None:
        # Example: Get information for a specific state
        state_info = get_state_info(states_data, "California")