# 📊 CSV Diff Checker

A Python-based tool to **compare two CSV files** and generate detailed reports highlighting mismatches, missing values, and data inconsistencies. This tool ensures data integrity across environments or systems, such as ETL pipelines and data migrations.

## 🚀 Features

- **Compares two CSV files** and identifies:
  - Differences in rows and columns
  - Missing values across datasets
  - Inconsistent data types
  - Data integrity and primary key validation
  - JSON field normalization for accurate comparison

- **Generates Reports:**
  - **Summary Report:** Key statistics saved as a `.txt` file
  - **Detailed Report:** A CSV output highlighting mismatches and missing data

## 📂 Folder Structure

CSV-Diff-Checker/
├── csv_diff_checker.py          # Main script to compare CSVs
├── requirements.txt             # Dependencies
├── data/                        # Data folder
│   ├── source_csv/              # Place source CSV files here
│   ├── comparison_csv/          # Place comparison CSV files here
│   └── output/                  # Reports and comparison results
└── README.md                    # Documentation

## 🛠️ Setup and Usage

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/CSV-Diff-Checker.git
cd CSV-Diff-Checker

2. Install Dependencies
pip install -r requirements.txt

3. Add Your CSV Files
Place source CSV files in the data/source_csv/ folder.
Place comparison CSV files in the data/comparison_csv/ folder.

4. Run the Script
python csv_diff_checker.py

5. View the Output
Summary Report: Found in data/output/ with the suffix _comparison_summary.txt.
Detailed Report: A CSV highlighting mismatches will also be generated in data/output/.
---

#### **Example Output**
```markdown
## 📋 Example Output

### Summary Report (`comparison_summary.txt`):

columns_match: True num_columns_match: True data_types_match: False row_count_match: True data_integrity: False primary_key_consistency: True null_values_consistency: True range_distribution_consistency: True order_of_rows: False duplicate_rows: True

### Detailed Report (CSV Sample):
| Primary Key | Column     | Source Value  | Comparison Value | Issue                  |
|-------------|------------|---------------|------------------|------------------------|
| 101         | Name       | John          | Jon              | DIFFERENT              |
| 102         | Age        | 25            | NaN              | MISSING in Comparison  |
| 103         | Address    | NaN           | 123 Main St      | MISSING in Source      |

Use Cases
## 🔄 Use Cases

- **ETL Pipeline Validation:** Ensure data transformations are correct between stages.
- **Data Migration Verification:** Check consistency when migrating data between systems.
- **Environment Comparison:** Validate differences between staging and production data.

Configuration
## ⚙️ Configuration

To modify **input/output directories**, update these paths in `csv_diff_checker.py`:
```python
source_folder = 'data/source_csv'
comparison_folder = 'data/comparison_csv'
output_folder = 'data/output'


---

#### **Error Handling**
```markdown
## 🛑 Error Handling

- **Missing Files:** Ensure both source and comparison files exist in their respective directories.
- **JSON Columns:** JSON fields are normalized automatically for accurate comparisons.


Contributing

## 🤝 Contributing
We welcome contributions! Feel free to open an issue or submit a pull request to enhance the tool.

