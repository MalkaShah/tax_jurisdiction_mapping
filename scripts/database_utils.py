import psycopg2
from psycopg2 import sql
import geopandas as gpd
from sqlalchemy import create_engine

def create_db_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            dbname="tax_jurisdictions",
            user="postgres",
            password="0000",  # Replace with your actual password
            host="localhost",
            port="5432"
        )
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def check_table_structure():
    """Check the column names in the us_states table"""
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
            print("\nTable Structure:")
            for col in columns:
                print(f"Column: {col[0]}, Type: {col[1]}")
            
        except Exception as e:
            print(f"Error checking table structure: {e}")
        finally:
            conn.close()

def create_spatial_table():
    """Create a spatial table for states"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Create states table with PostGIS geometry
            cur.execute("""
                CREATE TABLE IF NOT EXISTS us_states (
                    gid SERIAL PRIMARY KEY,
                    REGION text,
                    DIVISION text,
                    STATEFP text,
                    STATENS text,
                    GEOID text,
                    GEOIDFQ text,
                    STUSPS text,
                    NAME text,
                    LSAD text,
                    MTFCC text,
                    FUNCSTAT text,
                    ALAND bigint,
                    AWATER bigint,
                    INTPTLAT text,
                    INTPTLON text,
                    geometry geometry(MultiPolygon, 4269)
                );
            """)
            
            conn.commit()
            print("Spatial table created successfully!")
            
        except Exception as e:
            print(f"Error creating table: {e}")
        finally:
            conn.close()

def import_shapefile_to_postgis():
    """Import shapefile data to PostGIS"""
    try:
        # Read shapefile
        states_path = r"D:/projects/tax_jurisdiction_mapping/data/raw/tl_2023_us_state.shp"
        gdf = gpd.read_file(states_path)
        
        # Create SQLAlchemy engine
        engine = create_engine('postgresql://postgres:0000@localhost:5432/tax_jurisdictions')
        
        # Import to PostGIS
        gdf.to_postgis(
            name='us_states',
            con=engine,
            if_exists='replace',
            schema='public'
        )
        print("Data imported successfully!")
        
    except Exception as e:
        print(f"Error importing data: {e}")

def test_spatial_query():
    """Test a simple spatial query"""
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Using correct column name "ALAND" (uppercase)
            cur.execute("""
                SELECT 
                    COUNT(*) as state_count,
                    SUM("ALAND")/1000000.0 as total_area_km2
                FROM us_states;
            """)
            
            result = cur.fetchone()
            print(f"\nSpatial Query Results:")
            print(f"Total States: {result[0]}")
            print(f"Total Land Area (km²): {result[1]:,.2f}")
            
        except Exception as e:
            print(f"Error in spatial query: {e}")
        finally:
            conn.close()
            
  
def add_tax_columns():
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Add tax columns
            cur.execute("""
                -- Add new columns
                ALTER TABLE us_states 
                ADD COLUMN IF NOT EXISTS tax_rate DECIMAL(4,2),
                ADD COLUMN IF NOT EXISTS sales_tax_rate DECIMAL(4,2),
                ADD COLUMN IF NOT EXISTS use_tax_rate DECIMAL(4,2);
                
                -- Add sample tax rates
                UPDATE us_states
                SET 
                    tax_rate = CASE 
                        WHEN "NAME" = 'California' THEN 7.25
                        WHEN "NAME" = 'New York' THEN 4.00
                        WHEN "NAME" = 'Texas' THEN 6.25
                        ELSE 5.00
                    END,
                    sales_tax_rate = CASE 
                        WHEN "NAME" = 'California' THEN 7.25
                        WHEN "NAME" = 'New York' THEN 4.00
                        WHEN "NAME" = 'Texas' THEN 6.25
                        ELSE 5.00
                    END,
                    use_tax_rate = CASE 
                        WHEN "NAME" = 'California' THEN 7.25
                        WHEN "NAME" = 'New York' THEN 4.00
                        WHEN "NAME" = 'Texas' THEN 6.25
                        ELSE 5.00
                    END;
            """)
            conn.commit()
            print("Tax columns added and populated successfully!")
            
        except Exception as e:
            print(f"Error adding tax columns: {e}")
        finally:
            conn.close()  

          
def add_tax_rates():
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # Add tax rate columns
            cur.execute("""
                ALTER TABLE us_states 
                ADD COLUMN sales_tax_rate DECIMAL(4,2),
                ADD COLUMN use_tax_rate DECIMAL(4,2);
            """)
            
            # Update with sample tax rates
            cur.execute("""
                UPDATE us_states
                SET 
                    sales_tax_rate = 
                        CASE 
                            WHEN "NAME" = 'California' THEN 7.25
                            WHEN "NAME" = 'New York' THEN 4.00
                            WHEN "NAME" = 'Texas' THEN 6.25
                            WHEN "NAME" = 'Florida' THEN 6.00
                            WHEN "NAME" = 'Illinois' THEN 6.25
                            -- Add more states as needed
                        END,
                    use_tax_rate = 
                        CASE 
                            WHEN "NAME" = 'California' THEN 7.25
                            WHEN "NAME" = 'New York' THEN 4.00
                            WHEN "NAME" = 'Texas' THEN 6.25
                            WHEN "NAME" = 'Florida' THEN 6.00
                            WHEN "NAME" = 'Illinois' THEN 6.25
                            -- Add more states as needed
                        END;
            """)
            
            conn.commit()
            print("Tax rates added successfully!")
            
        except Exception as e:
            print(f"Error adding tax rates: {e}")
        finally:
            conn.close()
            
            

            
def update_tax_rates():
    conn = create_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            
            # First add the columns
            cur.execute("""
                ALTER TABLE us_states 
                ADD COLUMN IF NOT EXISTS sales_tax_rate numeric(4,2),
                ADD COLUMN IF NOT EXISTS use_tax_rate numeric(4,2);
            """)
            conn.commit()
            print("Tax rate columns added")

            # Then update the values
            cur.execute("""
                UPDATE us_states
                SET 
                    sales_tax_rate = CASE 
                        WHEN "NAME" = 'California' THEN 7.25
                        WHEN "NAME" = 'New York' THEN 4.00
                        WHEN "NAME" = 'Texas' THEN 6.25
                        WHEN "NAME" = 'Florida' THEN 6.00
                        WHEN "NAME" = 'Illinois' THEN 6.25
                        ELSE 5.00
                    END,
                    use_tax_rate = CASE 
                        WHEN "NAME" = 'California' THEN 7.25
                        WHEN "NAME" = 'New York' THEN 4.00
                        WHEN "NAME" = 'Texas' THEN 6.25
                        WHEN "NAME" = 'Florida' THEN 6.00
                        WHEN "NAME" = 'Illinois' THEN 6.25
                        ELSE 5.00
                    END;
            """)
            conn.commit()
            print("Tax rates updated")
            
            # Verify the update
            cur.execute("""
                SELECT "NAME", sales_tax_rate, use_tax_rate
                FROM us_states
                LIMIT 5;
            """)
            print("\nUpdated tax rates:")
            for row in cur.fetchall():
                print(f"State: {row[0]}, Sales Tax: {row[1]}, Use Tax: {row[2]}")
                
        except Exception as e:
            print(f"Error updating tax rates: {e}")
        finally:
            conn.close()




if __name__ == "__main__":
    print("Testing spatial query...")
    test_spatial_query()

if __name__ == "__main__":
    print("Checking table structure...")
    check_table_structure()
    
    print("\nCreating spatial table...")
    create_spatial_table()
    
    print("\nImporting shapefile to PostGIS...")
    import_shapefile_to_postgis()
    
    print("\nTesting spatial query...")
    test_spatial_query()
    
if __name__ == "__main__":
    add_tax_rates()
    
# Run this first to update the rates
if __name__ == "__main__":
    update_tax_rates()
