import os
import pandas as pd
import json

# Function to normalize JSON columns
def normalize_json_column(df, column):
    df[column] = df[column].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
    return df

# Function to serialize JSON columns for duplicate checking
def serialize_json_columns(df, json_columns):
    for col in json_columns:
        df[col] = df[col].apply(json.dumps)
    return df

# Function to compare two tables (CSV and CSV)
def compare_tables(csv_file1, csv_file2, output_file):
    # Load the CSV files into Pandas DataFrames
    df1 = pd.read_csv(csv_file1)
    df2 = pd.read_csv(csv_file2)

    # Identify the primary key (first column)
    primary_key = df1.columns[0]

    # Sort the tables based on the primary key
    df1 = df1.sort_values(by=primary_key).reset_index(drop=True)
    df2 = df2.sort_values(by=primary_key).reset_index(drop=True)

    # Check for JSON columns and normalize them
    json_columns = []
    for col in df1.columns:
        if df1[col].dtype == 'object':
            try:
                if df1[col].str.startswith('{').all():
                    json_columns.append(col)
            except AttributeError:
                continue

    for col in json_columns:
        df1 = normalize_json_column(df1, col)
        df2 = normalize_json_column(df2, col)

    # Serialize JSON columns for duplicate checking
    df1 = serialize_json_columns(df1, json_columns)
    df2 = serialize_json_columns(df2, json_columns)

    # Merge DataFrames on the primary key to ensure they have the same rows
    merged_df = pd.merge(df1, df2, on=primary_key, how='outer', suffixes=('_source', '_comparison'), indicator=True)

    # Separate the merged DataFrame back into two DataFrames
    df1_aligned = merged_df.filter(like='_source').rename(columns=lambda x: x.replace('_source', ''))
    df2_aligned = merged_df.filter(like='_comparison').rename(columns=lambda x: x.replace('_comparison', ''))

    # Ensure the primary key is included in both DataFrames
    df1_aligned[primary_key] = merged_df[primary_key]
    df2_aligned[primary_key] = merged_df[primary_key]

    # Align columns and indexes before comparison
    df1_aligned, df2_aligned = df1_aligned.align(df2_aligned, join='outer', axis=1, fill_value=None)
    df1_aligned, df2_aligned = df1_aligned.align(df2_aligned, join='outer', axis=0, fill_value=None)

    # Initialize a dictionary to hold comparison results
    comparison_results = {}

    # Check if the columns are the same
    comparison_results['columns_match'] = list(df1.columns) == list(df2.columns)
    comparison_results['num_columns_match'] = len(df1.columns) == len(df2.columns)

    # Align data types before comparison
    df1_dtypes = df1.dtypes.align(df2.dtypes, join='outer', axis=0, fill_value=None)[0]
    df2_dtypes = df1.dtypes.align(df2.dtypes, join='outer', axis=0, fill_value=None)[1]

    # Check data types consistency
    comparison_results['data_types_match'] = all(df1_dtypes == df2_dtypes)

    # Check row count
    comparison_results['row_count_match'] = len(df1) == len(df2)

    # Check data integrity
    comparison_results['data_integrity'] = df1_aligned.equals(df2_aligned)

    # Check primary key consistency
    comparison_results['primary_key_consistency'] = df1_aligned[primary_key].equals(df2_aligned[primary_key])

    # Check null values consistency
    comparison_results['null_values_consistency'] = df1_aligned.isnull().equals(df2_aligned.isnull())

    # Align descriptive statistics before comparison
    df1_desc = df1_aligned.describe().align(df2_aligned.describe(), join='outer', axis=1, fill_value=None)[0]
    df2_desc = df1_aligned.describe().align(df2_aligned.describe(), join='outer', axis=1, fill_value=None)[1]

    # Check range and distribution consistency
    comparison_results['range_distribution_consistency'] = all(df1_desc == df2_desc)

    # Check order of rows (if significant)
    comparison_results['order_of_rows'] = df1_aligned.equals(df2_aligned)

    # Check for duplicate rows
    comparison_results['duplicate_rows'] = df1_aligned.duplicated().equals(df2_aligned.duplicated())

    # Save the comparison results to a text file
    with open(output_file.replace('.csv', '_comparison_summary.txt'), 'w') as file:
        for key, value in comparison_results.items():
            file.write(f"{key}: {value}\n")

    # Compare the entire DataFrames for detailed differences
    comparison_df = df1_aligned.compare(df2_aligned, keep_equal=True)

    # Highlight differences
    diff_mask = df1_aligned.ne(df2_aligned)
    mismatched_values = df1_aligned.where(diff_mask, other=None)

    # Drop rows and columns with no mismatched values
    mismatched_values = mismatched_values.dropna(how='all', axis=0).dropna(how='all', axis=1)

    # Create a more readable detailed comparison DataFrame
    detailed_comparison_list = []

    for index, row in mismatched_values.iterrows():
        for col in mismatched_values.columns:
            if pd.notna(row[col]):
                issue = 'DIFFERENT'
                if pd.isna(df2_aligned.at[index, col]):
                    issue = 'MISSING in Comparison'
                elif pd.isna(df1_aligned.at[index, col]):
                    issue = 'MISSING in Source'
                detailed_comparison_list.append({
                    'Primary Key': df1_aligned.at[index, primary_key],
                    'Column': col,
                    'Source Value': df1_aligned.at[index, col],
                    'Comparison Value': df2_aligned.at[index, col],
                    'Issue': issue
                })

    # Convert list to DataFrame
    detailed_comparison = pd.DataFrame(detailed_comparison_list)

    # Save the detailed comparison to the output file
    detailed_output_file = output_file.replace('.csv', '_detailed.csv')
    detailed_comparison.to_csv(detailed_output_file, index=False)

    # Save only the mismatched rows to the main comparison output file
    mismatched_rows = comparison_df.dropna(how='all')
    mismatched_rows.to_csv(output_file, index=False)

    # Highlight missing data in the console output
    print("\n=== Mismatched Rows ===")
    print(mismatched_rows)
    print("\n=== Mismatched Values ===")
    print(detailed_comparison)

def main():
    # Paths for the source, comparison, and output folders
    source_folder = 'data/source_csv'  # CSV files
    comparison_folder = 'data/comparison_csv'  # CSV files
    output_folder = 'data/output'  # Output files


    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # List all files in the source and comparison folders
    source_files = sorted([f for f in os.listdir(source_folder) if f.endswith('.csv')])
    comparison_files = sorted([f for f in os.listdir(comparison_folder) if f.endswith('.csv')])

    # Iterate through the files and compare them
    for source_file in source_files:
        comparison_file = source_file
        if comparison_file in comparison_files:
            source_path = os.path.join(source_folder, source_file)
            comparison_path = os.path.join(comparison_folder, comparison_file)
            output_file = os.path.join(output_folder, f'comparison_{source_file}')

            compare_tables(source_path, comparison_path, output_file)

    print("Comparison complete. Check the output folder for results.")

if __name__ == "__main__":
    main()
