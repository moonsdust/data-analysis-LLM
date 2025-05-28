import pandas as pd
import os
import glob

def process_appointments_data():
    """
    Process appointment CSV files by combining them and separating by year.
    
    Reads CSV files from ../precleaning directory, extracts year from posted_date,
    and creates separate CSV files for each year (2013-2024) in ../raw_data directory.
    """
    
    # Define paths
    input_dir = "precleaning"
    output_dir = "raw_data"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all CSV files in the input directory and sort them by name
    csv_files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Dictionary to store data by year
    yearly_data = {}
    target_years = set(range(2013, 2025))  # 2013 to 2024 inclusive
    
    # Process each CSV file in sequence
    for i, csv_file in enumerate(csv_files, 1):
        print(f"Processing file {i}/{len(csv_files)}: {os.path.basename(csv_file)}")
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Extract year from posted_date
        df['year'] = pd.to_datetime(df['posted_date']).dt.year
        
        # Group by year and add to yearly_data
        for year in df['year'].unique():
            year = int(year)
            if year in target_years:
                year_data = df[df['year'] == year].drop('year', axis=1)
                
                if year not in yearly_data:
                    yearly_data[year] = []
                
                yearly_data[year].append(year_data)
                print(f"  - Added {len(year_data)} records for year {year}")
    
    # Combine data for each year and save to separate CSV files
    print("\nCombining and saving data by year...")
    
    for year in sorted(yearly_data.keys()):
        # Combine all dataframes for this year
        combined_df = pd.concat(yearly_data[year], ignore_index=True)
        
        # Sort the data by posted_date, then by name
        combined_df = combined_df.sort_values(['posted_date', 'name'])
        
        # Reset index
        combined_df = combined_df.reset_index(drop=True)
        
        # Save to CSV
        output_file = os.path.join(output_dir, f"appointments_{year}.csv")
        combined_df.to_csv(output_file, index=False)
        
        print(f"Saved {len(combined_df)} records for year {year} to {output_file}")
    
    # Summary
    print(f"\nProcessing complete!")
    print(f"Created {len(yearly_data)} yearly datasets:")
    for year in sorted(yearly_data.keys()):
        total_records = sum(len(df) for df in yearly_data[year])
        print(f"  - {year}: {total_records} records")

if __name__ == "__main__":
    process_appointments_data()