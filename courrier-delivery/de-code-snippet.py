# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration

LOG_FILE = 'info.log' 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()  
    ]
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
BASE_DIR = 'output_data'
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = os.path.join(BASE_DIR, 'weather_data.json')
OUTPUT_PATH = os.path.join(BASE_DIR, 'deliveries.csv')     


# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Random selection of package type and zone
        package_type = random.choices(
            package_types, 
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]
        
        delivery_zone = random.choice(delivery_zones)
        
        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            package_type,
            delivery_zone,
            random.randint(1, 100)  # fictional recipient_id
        ))
    
    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    os.makedirs(os.path.dirname(WEATHER_PATH), exist_ok=True)

    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS (to be completed)
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

def generate_tracking_logs(df_deliveries):
    """
    Generates tracking logs with start and end timestamps
    """
    logger.info("Generating tracking logs...")
    
    # Convert pickup_datetime to datetime if not already
    df_deliveries['pickup_datetime'] = pd.to_datetime(df_deliveries['pickup_datetime'])
    
    # Generate distance based on delivery zone
    zone_distance = {
        'Urban': (2, 15),         
        'Suburban': (10, 30),
        'Rural': (20, 100),
        'Industrial': (5, 25),
        'Shopping Center': (1, 10)
    }
    
    # Generate random distances based on zone
    df_deliveries['distance'] = df_deliveries['delivery_zone'].apply(
        lambda zone: round(random.uniform(*zone_distance[zone]), 1)
    )
    
    # Calculate theoretical delivery time based on the formula
    package_factors = {
        'Small': 1.0,
        'Medium': 1.2,
        'Large': 1.5,
        'X-Large': 2.0,
        'Special': 2.5
    }
    
    zone_factors = {
        'Urban': 1.2,
        'Suburban': 1.0,
        'Rural': 1.3,
        'Industrial': 0.9,
        'Shopping Center': 1.4
    }
    
    # Calculate actual delivery time with some random variation
    def calculate_actual_time(row):
        # Base time: 30 + distance * 0.8 minutes
        base_time = 30 + row['distance'] * 0.8
        
        # Apply package factor
        adjusted_time = base_time * package_factors[row['package_type']]
        
        # Apply zone factor
        adjusted_time = adjusted_time * zone_factors[row['delivery_zone']]
        
        # Apply time of day factor
        hour = row['pickup_datetime'].hour
        if 7 <= hour < 10:  # Morning peak
            adjusted_time *= 1.3
        elif 16 <= hour < 19:  # Evening peak
            adjusted_time *= 1.4
        
        # Apply day of week factor
        weekday = row['pickup_datetime'].weekday()
        if weekday == 0 or weekday == 4:  # Monday (0) or Friday (4)
            adjusted_time *= 1.2
        elif weekday >= 5:  # Weekend
            adjusted_time *= 0.9
        
        # Add some random variation (±20%)
        variation = random.uniform(0.8, 1.2)
        actual_time = adjusted_time * variation
        
        return round(actual_time, 1)
    

    df_deliveries['weekday'] = df_deliveries['pickup_datetime'].dt.day_name()
    df_deliveries['hour'] = df_deliveries['pickup_datetime'].dt.hour
    
    df_deliveries['actual_delivery_time'] = df_deliveries.apply(calculate_actual_time, axis=1)
    

    df_deliveries['delivery_end_time'] = df_deliveries['pickup_datetime'] + pd.to_timedelta(df_deliveries['actual_delivery_time'], unit='m')
    
    logger.info("Tracking logs generated")
    return df_deliveries

 # 4. TRANSFORMATION FUNCTIONS (to be completed by participants)

def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions
    """
    logger.info("Enriching with weather data...")
    
    # Debug: Check weather_data structure
    logger.info(f"Weather data structure: {type(weather_data)}")
    logger.info(f"Weather data keys sample: {list(weather_data.keys())[:3] if weather_data else 'Empty'}")
    
    # Function to get weather for a given timestamp
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        
        try:
            if date_str in weather_data and hour_str in weather_data[date_str]:
                return weather_data[date_str][hour_str]
            else:
                logger.warning(f"Missing weather data for {date_str}, hour {hour_str}")
                return 'Unknown'
        except Exception as e:
            logger.warning(f"Error getting weather for {date_str}, hour {hour_str}: {str(e)}")
            return 'Unknown'
    
    # Apply function to each row
    try:
        df['weather_condition'] = df['pickup_datetime'].apply(get_weather)
        logger.info("Weather enrichment complete")
        return df
    except Exception as e:
        logger.error(f"Error during weather enrichment: {str(e)}")
        # Fallback: Add empty weather column
        df['weather_condition'] = 'Unknown'
        return df

def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    """
    logger.info("Transforming data...")
    
    # 1. Enrich with tracking logs (add distance and delivery times)
    df_enriched = generate_tracking_logs(df_deliveries)
    print(df_enriched)
    # 2. Enrich with weather data
    df_enriched = enrich_with_weather(df_enriched, weather_data)
    print(df_enriched)
    # 3. Determine theoretical delivery time and threshold
    def calculate_threshold(row):
        # Base time: 30 + distance * 0.8 minutes
        base_time = 30 + row['distance'] * 0.8
        

        package_factors = {
            'Small': 1.0, 'Medium': 1.2, 'Large': 1.5, 
            'X-Large': 2.0, 'Special': 2.5
        }
        adjusted_time = base_time * package_factors[row['package_type']]

        zone_factors = {
            'Urban': 1.2, 'Suburban': 1.0, 'Rural': 1.3,
            'Industrial': 0.9, 'Shopping Center': 1.4
        }
        adjusted_time = adjusted_time * zone_factors[row['delivery_zone']]
  
        hour = row['hour']
        if 7 <= hour < 10:  
            adjusted_time *= 1.3
        elif 16 <= hour < 19:  
            adjusted_time *= 1.4
        

        weekday = row['weekday']
        if weekday in ['Monday', 'Friday']:
            adjusted_time *= 1.2
        elif weekday in ['Saturday', 'Sunday']:
            adjusted_time *= 0.9
            

        weather_factors = {
            'Sunny': 1.0, 'Cloudy': 1.05, 'Rainy': 1.2,
            'Windy': 1.1, 'Snowy': 1.8, 'Foggy': 1.3,
            'Unknown': 1.0
        }
        adjusted_time *= weather_factors.get(row['weather_condition'], 1.0)
        
        threshold = adjusted_time * 1.2
        
        return round(threshold, 1)
    
    df_enriched['threshold_time'] = df_enriched.apply(calculate_threshold, axis=1)
    
    df_enriched['status'] = df_enriched.apply(
        lambda row: 'Delayed' if row['actual_delivery_time'] > row['threshold_time'] else 'On-time',
        axis=1
    )
    
    df_cleaned = df_enriched.fillna({
        'weather_condition': 'Unknown',
        'actual_delivery_time': df_enriched['actual_delivery_time'].median()
    })
    
    final_df = df_cleaned[[
        'delivery_id', 
        'pickup_datetime', 
        'weekday', 
        'hour', 
        'package_type', 
        'distance', 
        'delivery_zone', 
        'weather_condition', 
        'actual_delivery_time', 
        'status'
    ]]
    
    final_df.columns = [
        'Delivery_ID', 
        'Pickup_DateTime', 
        'Weekday', 
        'Hour', 
        'Package_Type', 
        'Distance', 
        'Delivery_Zone', 
        'Weather_Condition', 
        'Actual_Delivery_Time', 
        'Status'
    ]
    
    logger.info("Data transformation complete")
    return final_df

# # 5. LOADING FUNCTION (to be completed)
def save_results(df):
    """
    Saves the final DataFrame to CSV with validation
    """
    logger.info("Validating and saving results...")
    
    # Basic validation
    # 1. Check for missing values
    missing_count = df.isnull().sum().sum()
    if missing_count > 0:
        logger.warning(f"Found {missing_count} missing values. Filling with defaults.")
        df = df.fillna({
            'Weather_Condition': 'Unknown',
            'Actual_Delivery_Time': df['Actual_Delivery_Time'].median(),
            'Status': 'Unknown'
        })
    
    # 2. Check for outliers in delivery time
    q1 = df['Actual_Delivery_Time'].quantile(0.25)
    q3 = df['Actual_Delivery_Time'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers = df[(df['Actual_Delivery_Time'] < lower_bound) | 
                 (df['Actual_Delivery_Time'] > upper_bound)]
    
    if len(outliers) > 0:
        logger.warning(f"Found {len(outliers)} outliers in delivery time.")
    
    # Generate simple statistics
    stats = {
        'total_deliveries': len(df),
        'delayed_deliveries': len(df[df['Status'] == 'Delayed']),
        'on_time_deliveries': len(df[df['Status'] == 'On-time']),
        'avg_delivery_time': round(df['Actual_Delivery_Time'].mean(), 2),
        'min_delivery_time': df['Actual_Delivery_Time'].min(),
        'max_delivery_time': df['Actual_Delivery_Time'].max()
    }
    
    # Print statistics
    logger.info("Dataset Statistics:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Save to CSV
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    
    # Save statistics to a separate file
    stats_df = pd.DataFrame([stats])
    # stats_df.to_csv('output_data/statistics.csv', index=False)
    
    logger.info(f"Results saved to {OUTPUT_PATH}")
    return True


# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()
        weather_data = generate_weather_data()
        
        #  Step 2: Extraction
        df_deliveries = extract_sqlite_data()
        
        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)
        print(df_transformed)
        #  # Step 4: Loading
        save_results(df_transformed)
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
         logger.error(f"Error during pipeline execution: {str(e)}")
         return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()
