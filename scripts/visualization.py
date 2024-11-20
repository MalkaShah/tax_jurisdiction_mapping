import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from database_utils import create_db_connection
import pandas as pd
from sqlalchemy import create_engine

def create_state_choropleth():
    """Create a choropleth map of states colored by area"""
    try:
        # Connect to database
        engine = create_engine('postgresql://postgres:0000@localhost:5432/tax_jurisdictions')
        
        # Read data from PostGIS
        query = """
            SELECT 
                "NAME",
                "STUSPS",
                "ALAND"/1000000.0 as area_km2,
                geometry
            FROM us_states;
        """
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(15, 10))
        
        # Create choropleth map
        gdf.plot(column='area_km2',
                ax=ax,
                legend=True,
                legend_kwds={'label': 'Area (km²)'},
                cmap='YlOrRd')
        
        # Customize the map
        ax.set_title('US States by Area', fontsize=16)
        ax.axis('off')
        
        # Add state labels
        for idx, row in gdf.iterrows():
            ax.annotate(text=row['STUSPS'], 
                       xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                       ha='center',
                       va='center')
        
        # Save the map
        plt.savefig('documentation/state_area_map.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print("Choropleth map created successfully!")
        
    except Exception as e:
        print(f"Error creating choropleth map: {e}")

def create_regional_analysis_plots():
    """Create visualizations for regional analysis"""
    conn = create_db_connection()
    if conn:
        try:
            # Get regional data
            query = """
                SELECT 
                    "REGION",
                    COUNT(*) as state_count,
                    SUM("ALAND"/1000000.0) as total_area_km2
                FROM us_states
                GROUP BY "REGION"
                ORDER BY total_area_km2 DESC;
            """
            df = pd.read_sql(query, conn)
            
            # Create a figure with two subplots
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Bar plot of state counts by region
            sns.barplot(data=df, x='REGION', y='state_count', ax=ax1)
            ax1.set_title('Number of States by Region')
            ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
            ax1.set_ylabel('Number of States')
            
            # Bar plot of total area by region
            sns.barplot(data=df, x='REGION', y='total_area_km2', ax=ax2)
            ax2.set_title('Total Area by Region')
            ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
            ax2.set_ylabel('Total Area (km²)')
            
            plt.tight_layout()
            plt.savefig('documentation/regional_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            print("Regional analysis plots created successfully!")
            
        except Exception as e:
            print(f"Error creating regional plots: {e}")
        finally:
            conn.close()

def create_complexity_visualization():
    """Create visualization of boundary complexity"""
    conn = create_db_connection()
    if conn:
        try:
            # Get complexity metrics
            query = """
                WITH state_metrics AS (
                    SELECT 
                        "NAME",
                        "STUSPS",
                        ST_Perimeter(geometry::geography)/1000 as perimeter_km,
                        "ALAND"/1000000.0 as area_km2,
                        ST_NPoints(geometry) as boundary_points,
                        (ST_Perimeter(geometry::geography)/1000 / 
                         NULLIF(SQRT("ALAND"/1000000.0), 0)) as complexity_index
                    FROM us_states
                )
                SELECT *
                FROM state_metrics
                ORDER BY complexity_index DESC
                LIMIT 15;
            """
            df = pd.read_sql(query, conn)
            
            # Create visualization
            plt.figure(figsize=(12, 6))
            sns.barplot(data=df, x='STUSPS', y='complexity_index')
            plt.title('State Boundary Complexity Index')
            plt.xlabel('State')
            plt.ylabel('Complexity Index')
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            plt.savefig('documentation/boundary_complexity.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            print("Complexity visualization created successfully!")
            
        except Exception as e:
            print(f"Error creating complexity visualization: {e}")
        finally:
            conn.close()
            
def visualize_boundary_complexity():
    conn = create_db_connection()
    if conn:
        try:
            # Get data
            query = """
                SELECT 
                    "NAME",
                    "STUSPS",
                    ST_Perimeter(geometry::geography)/1000 as perimeter_km,
                    "ALAND"/1000000.0 as area_km2,
                    ST_NPoints(geometry) as boundary_points,
                    (ST_Perimeter(geometry::geography)/1000 / 
                     NULLIF(SQRT("ALAND"/1000000.0), 0)) as complexity_index,
                    geometry
                FROM us_states;
            """
            gdf = gpd.read_postgis(query, conn, geom_col='geometry')
            
            # Create visualization
            fig, ax = plt.subplots(figsize=(15, 10))
            gdf.plot(column='complexity_index', 
                    cmap='YlOrRd',
                    legend=True,
                    legend_kwds={'label': 'Boundary Complexity Index'},
                    ax=ax)
            
            # Add labels for top 3 complex states
            for idx, row in gdf.nlargest(3, 'complexity_index').iterrows():
                ax.annotate(row['STUSPS'], 
                          xy=row.geometry.centroid.coords[0],
                          ha='center')
            
            plt.title('State Boundary Complexity')
            plt.axis('off')
            plt.savefig('documentation/boundary_complexity_map.png')
            plt.close()
            
        except Exception as e:
            print(f"Error in visualization: {e}")
        finally:
            conn.close()

def visualize_tax_rates():
    conn = create_db_connection()
    if conn:
        try:
            # Get data for visualization
            query = """
                SELECT 
                    "NAME",
                    "STUSPS",
                    sales_tax_rate,
                    use_tax_rate,
                    geometry
                FROM us_states;
            """
            gdf = gpd.read_postgis(query, conn, geom_col='geometry')
            
            print(f"DataFrame shape: {gdf.shape}")
            print("\nSample of tax rates:")
            print(gdf[["NAME", "sales_tax_rate", "use_tax_rate"]].head())

            if len(gdf) > 0:
                # Create visualization with two maps instead of three
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))
                
                # Plot SALES_TAX_RATE
                gdf.plot(column='sales_tax_rate',
                        cmap='YlOrRd',
                        legend=True,
                        legend_kwds={'label': 'Sales Tax Rate (%)'},
                        ax=ax1)
                ax1.set_title('Sales Tax Rates')
                ax1.axis('off')
                
                # Plot USE_TAX_RATE
                gdf.plot(column='use_tax_rate',
                        cmap='YlOrRd',
                        legend=True,
                        legend_kwds={'label': 'Use Tax Rate (%)'},
                        ax=ax2)
                ax2.set_title('Use Tax Rates')
                ax2.axis('off')
                
                plt.tight_layout()
                plt.savefig('documentation/tax_rates_map.png')
                plt.close()
                print("Tax rate visualization created successfully!")
            else:
                print("No data available for visualization")
            
        except Exception as e:
            print(f"Error in visualization: {e}")
        finally:
            conn.close()

def export_visualizations():
    conn = create_db_connection()
    if conn:
        try:
            # Get data
            query = """
                SELECT 
                    "NAME",
                    "STUSPS",
                    sales_tax_rate,
                    use_tax_rate,
                    geometry
                FROM us_states;
            """
            gdf = gpd.read_postgis(query, conn, geom_col='geometry')

            # 1. PNG Format with State Labels
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
            
            # Sales Tax Map
            gdf.plot(column='sales_tax_rate',
                    cmap='YlOrRd',
                    legend=True,
                    legend_kwds={'label': 'Sales Tax Rate (%)'},
                    ax=ax1)
            # Add state labels
            for idx, row in gdf.iterrows():
                ax1.annotate(row['STUSPS'], 
                           xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                           ha='center', va='center',
                           fontsize=8)
            ax1.set_title('Sales Tax Rates by State')
            ax1.axis('off')
            
            # Use Tax Map
            gdf.plot(column='use_tax_rate',
                    cmap='YlOrRd',
                    legend=True,
                    legend_kwds={'label': 'Use Tax Rate (%)'},
                    ax=ax2)
            # Add state labels
            for idx, row in gdf.iterrows():
                ax2.annotate(row['STUSPS'], 
                           xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                           ha='center', va='center',
                           fontsize=8)
            ax2.set_title('Use Tax Rates by State')
            ax2.axis('off')
            
            plt.tight_layout()
            plt.savefig('documentation/tax_rates_labeled.png', dpi=300, bbox_inches='tight')
            plt.close()

            # 2. SVG Format (Vector Graphics)
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
            gdf.plot(column='sales_tax_rate',
                    cmap='YlOrRd',
                    legend=True,
                    legend_kwds={'label': 'Sales Tax Rate (%)'},
                    ax=ax1)
            ax1.set_title('Sales Tax Rates')
            ax1.axis('off')
            
            gdf.plot(column='use_tax_rate',
                    cmap='YlOrRd',
                    legend=True,
                    legend_kwds={'label': 'Use Tax Rate (%)'},
                    ax=ax2)
            ax2.set_title('Use Tax Rates')
            ax2.axis('off')
            
            plt.tight_layout()
            plt.savefig('documentation/tax_rates_vector.svg', format='svg', bbox_inches='tight')
            plt.close()

            # 3. Export to HTML (Interactive)
            import folium
            from branca.colormap import LinearColormap

            # Create base map
            m = folium.Map(location=[39.8283, -98.5795], zoom_start=4)

            # Add choropleth layers
            folium.Choropleth(
                geo_data=gdf,
                name='Sales Tax Rates',
                data=gdf,
                columns=['NAME', 'sales_tax_rate'],
                key_on='feature.properties.NAME',
                fill_color='YlOrRd',
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name='Sales Tax Rate (%)'
            ).add_to(m)

            # Add hover functionality
            style_function = lambda x: {'fillColor': '#ffffff', 
                                      'color':'#000000', 
                                      'fillOpacity': 0.1, 
                                      'weight': 0.1}
            highlight_function = lambda x: {'fillColor': '#000000', 
                                          'color':'#000000', 
                                          'fillOpacity': 0.50, 
                                          'weight': 0.1}

            # Add tooltips
            folium.GeoJson(
                gdf,
                style_function=style_function,
                control=False,
                highlight_function=highlight_function,
                tooltip=folium.GeoJsonTooltip(
                    fields=['NAME', 'sales_tax_rate', 'use_tax_rate'],
                    aliases=['State:', 'Sales Tax:', 'Use Tax:'],
                    style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
                )
            ).add_to(m)

            # Save to HTML
            m.save('documentation/tax_rates_interactive.html')

            # 4. Export data to Excel
            tax_data = gdf[['NAME', 'STUSPS', 'sales_tax_rate', 'use_tax_rate']]
            tax_data.to_excel('documentation/tax_rates_summary.xlsx', index=False)

            print("All visualizations exported successfully!")
            print("\nFiles created:")
            print("1. tax_rates_labeled.png - High-resolution map with state labels")
            print("2. tax_rates_vector.svg - Vector graphics format")
            print("3. tax_rates_interactive.html - Interactive web map")
            print("4. tax_rates_summary.xlsx - Data summary in Excel")

        except Exception as e:
            print(f"Error in export: {e}")
        finally:
            conn.close()




if __name__ == "__main__":
    print("Creating visualizations...")
    create_state_choropleth()
    create_regional_analysis_plots()
    create_complexity_visualization()
    visualize_boundary_complexity()
    visualize_tax_rates()
    
if __name__ == "__main__":
    export_visualizations()