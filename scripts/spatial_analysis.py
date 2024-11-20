# spatial_analysis.py
import psycopg2
from psycopg2 import sql
import geopandas as gpd

# Import database connection function from database_utils
from database_utils import create_db_connection

def analyze_state_boundaries():
    """Analyze state boundaries and relationships"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Find bordering states
            print("\nAnalyzing State Borders:")
            cur.execute("""
                SELECT 
                    a."NAME" as state1,
                    b."NAME" as state2
                FROM us_states a
                JOIN us_states b ON ST_Touches(a.geometry, b.geometry)
                WHERE a."NAME" < b."NAME"
                ORDER BY state1, state2;
            """)
            
            borders = cur.fetchall()
            current_state = ""
            for border in borders:
                if border[0] != current_state:
                    current_state = border[0]
                    print(f"\n{current_state} borders with:")
                print(f"  - {border[1]}")

            # Calculate state areas and sort by size
            print("\nState Size Analysis:")
            cur.execute("""
                SELECT 
                    "NAME",
                    "STUSPS",
                    "ALAND"/1000000.0 as area_km2
                FROM us_states
                ORDER BY "ALAND" DESC
                LIMIT 5;
            """)
            
            print("\nTop 5 Largest States:")
            for row in cur.fetchall():
                print(f"{row[0]} ({row[1]}): {row[2]:,.2f} km²")

            # Regional statistics
            print("\nRegional Analysis:")
            cur.execute("""
                SELECT 
                    "REGION",
                    COUNT(*) as state_count,
                    SUM("ALAND"/1000000.0) as total_area_km2,
                    AVG("ALAND"/1000000.0) as avg_area_km2
                FROM us_states
                GROUP BY "REGION"
                ORDER BY total_area_km2 DESC;
            """)
            
            for row in cur.fetchall():
                print(f"\nRegion: {row[0]}")
                print(f"Number of States: {row[1]}")
                print(f"Total Area: {row[2]:,.2f} km²")
                print(f"Average State Area: {row[3]:,.2f} km²")

        except Exception as e:
            print(f"Error in boundary analysis: {e}")
        finally:
            conn.close()

def calculate_tax_jurisdiction_metrics():
    """Calculate metrics relevant for tax jurisdiction analysis"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Calculate area proportions and complexity metrics
            cur.execute("""
                WITH state_metrics AS (
                    SELECT 
                        "NAME",
                        "STUSPS",
                        ST_Perimeter(geometry::geography)/1000 as perimeter_km,
                        "ALAND"/1000000.0 as area_km2,
                        ST_NPoints(geometry) as boundary_points
                    FROM us_states
                )
                SELECT 
                    "NAME",
                    "STUSPS",
                    perimeter_km,
                    area_km2,
                    boundary_points,
                    (perimeter_km / NULLIF(SQRT(area_km2), 0)) as complexity_index
                FROM state_metrics
                ORDER BY complexity_index DESC
                LIMIT 10;
            """)
            
            print("\nJurisdiction Complexity Analysis:")
            print("(Higher complexity index indicates more complex boundaries)")
            print("\nTop 10 States by Boundary Complexity:")
            for row in cur.fetchall():
                print(f"\nState: {row[0]} ({row[1]})")
                print(f"Perimeter: {row[2]:,.2f} km")
                print(f"Area: {row[3]:,.2f} km²")
                print(f"Boundary Points: {row[4]}")
                print(f"Complexity Index: {row[5]:.2f}")

        except Exception as e:
            print(f"Error in tax jurisdiction metrics: {e}")
        finally:
            conn.close()
            
def analyze_tax_jurisdictions():
    """Analyze state tax jurisdictions"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Add tax rate column if not exists
            cur.execute("""
                ALTER TABLE us_states 
                ADD COLUMN IF NOT EXISTS sales_tax_rate DECIMAL(4,2),
                ADD COLUMN IF NOT EXISTS use_tax_rate DECIMAL(4,2),
                ADD COLUMN IF NOT EXISTS tax_jurisdiction_type VARCHAR(50);
            """)
            
            # Example queries for tax analysis
            cur.execute("""
                WITH border_states AS (
                    SELECT 
                        a."NAME" as state1,
                        b."NAME" as state2,
                        ST_Length(ST_Intersection(a.geometry, b.geometry)::geography)/1000 as border_length_km
                    FROM us_states a
                    JOIN us_states b ON ST_Intersects(a.geometry, b.geometry)
                    WHERE a."NAME" < b."NAME"
                )
                SELECT 
                    state1,
                    state2,
                    border_length_km,
                    -- Tax jurisdiction complexity factors
                    CASE 
                        WHEN border_length_km > 500 THEN 'High'
                        WHEN border_length_km > 200 THEN 'Medium'
                        ELSE 'Low'
                    END as tax_complexity_level
                FROM border_states
                ORDER BY border_length_km DESC;
            """)
            
            results = cur.fetchall()
            print("\nTax Jurisdiction Analysis:")
            for row in results:
                print(f"Border Zone: {row[0]} - {row[1]}")
                print(f"Length: {row[2]:.2f} km")
                print(f"Tax Complexity: {row[3]}")
                print("---")
                
        except Exception as e:
            print(f"Error in tax analysis: {e}")
        finally:
            conn.close()
            
def check_table():
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'us_states'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            print("\nColumns in us_states:")
            for col in columns:
                print(f"{col[0]}: {col[1]}")
        finally:
            conn.close()

if __name__ == "__main__":
    check_table()

if __name__ == "__main__":
    print("Analyzing state boundaries...")
    analyze_state_boundaries()
    
    print("\nCalculating tax jurisdiction metrics...")
    calculate_tax_jurisdiction_metrics()