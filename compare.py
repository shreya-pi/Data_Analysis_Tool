from log import log_dq_info, log_dq_error
import pandas as pd



class Compare:  

    def __init__(self):
        self.all_comparisons = []

    def compare_results(self, df1, df2, snowflake_name, sqlserver_name,entity_type):
        comparison_data = []
    
        # Compare entity names
        comparison_data.append({
            "Attribute": "Executed Entity",
            "Snowflake Output": snowflake_name,
            "SQL Server Output": sqlserver_name,
            "Comparison": "Same" if snowflake_name.lower() == sqlserver_name.lower() else "Different"
        })
    
        # Compare number of rows
        count_df1 = len(df1)
        count_df2 = len(df2)
        comparison_data.append({
            "Attribute": "Number of Rows",
            "Snowflake Output": count_df1,
            "SQL Server Output": count_df2,
            "Comparison": "Same" if count_df1 == count_df2 else "Different"
        })
    
        # Standardize column names
        # df1.columns = df1.columns.str.lower()
        # df2.columns = df2.columns.str.lower()
        # Ensure column names are strings before converting to lowercase
        df1.columns = df1.columns.astype(str).str.lower()
        df2.columns = df2.columns.astype(str).str.lower()
    
        # Compare column names
        col_df1 = set(df1.columns)
        col_df2 = set(df2.columns)

        comparison_data.append({
            "Attribute": "Column Names",
            "Snowflake Output": ", ".join(col_df1),
            "SQL Server Output": ", ".join(col_df2),
            "Comparison": "Same" if col_df1 == col_df2 else "Different"
        })

        # Find common columns
        common_columns = list(col_df1.intersection(col_df2))

        # if no common columns are found, log an error
        if not common_columns:
            log_dq_error("No common columns found for merging!")
            comparison_data.append({
                "Attribute": "Data Comparison",
                "Snowflake Output": "N/A",
                "SQL Server Output": "N/A",
                "Comparison": "No common columns to compare"
            })
        else:
        #if some common columns are found, proceed with the comparison
            try:
                # Compare data types
                dtype_df1 = {col: str(df1[col].dtype) for col in common_columns}
                dtype_df2 = {col: str(df2[col].dtype) for col in common_columns}
                comparison_data.append({
                    "Attribute": "Data Types",
                    "Snowflake Output": ", ".join(f"{col}: {dtype}" for col, dtype in dtype_df1.items()),
                    "SQL Server Output": ", ".join(f"{col}: {dtype}" for col, dtype in dtype_df2.items()),
                    "Comparison": "Same" if dtype_df1 == dtype_df2 else "Different"
                })
            
                # Align column orders and convert types
                # df2 = df2[df1.columns]  # Align column order
    
                # df1 = df1[df1.columns].copy()
                df2 = df2[common_columns].copy()
                for col in common_columns:
                    if df1[col].dtype != df2[col].dtype:
    
                        df1[col] = df1[col].astype(str)
                        df2[col] = df2[col].astype(str)
            
        
                    # Check if DataFrames are exactly equal
                if df1.equals(df2):
                    log_dq_info("The results match perfectly!")
                    comparison_data.append({
                        "Attribute": "Data Comparison",
                        "Snowflake Output": "Exact Match",
                        "SQL Server Output": "Exact Match",
                        "Comparison": "Same"
                    })
                else:
                    log_dq_error("The results do not match!")
            
                    # Perform an outer merge to detect differences
                    merged_df = pd.merge(df1, df2, how='outer', indicator=True, on=common_columns)
            
                    # Extract differing rows
                    diff = merged_df[merged_df['_merge'] != 'both'].copy()
            
                    # Handle categorical columns properly
                    for col in diff.select_dtypes(['category']).columns:
                        diff[col] = diff[col].astype('category')
                        if 'NaN' not in diff[col].cat.categories:
                            diff[col] = diff[col].cat.add_categories(['NaN'])
                    
                    diff = diff.fillna('NaN')

                    if entity_type == 'table':
                        # Save differences to CSV
                        diff.to_csv(f"Dq_analysis/Tables/{sqlserver_name}differences.csv", index=True)
                        log_dq_info(f"Differences saved to Dq_analysis/Tables/{sqlserver_name}differences.csv")
                    elif entity_type == 'view':
                        diff.to_csv(f"Dq_analysis/Views/{sqlserver_name}differences.csv", index=True)
                        log_dq_info(f"Differences saved to Dq_analysis/Views/{sqlserver_name}differences.csv")
                    elif entity_type == 'function':
                        diff.to_csv(f"Dq_analysis/Functions/{sqlserver_name}differences.csv", index=True)
                        log_dq_info(f"Differences saved to Dq_analysis/Functions/{sqlserver_name}differences.csv")
                    elif entity_type == 'procedure':
                        diff.to_csv(f"Dq_analysis/Procedures/{sqlserver_name}differences.csv", index=True)
                        log_dq_info(f"Differences saved to Dq_analysis/Procedures/{sqlserver_name}differences.csv")
            
                    count_diff = len(diff)
                    log_dq_info(f"Number of differences detected in {sqlserver_name}, {entity_type}: {count_diff}")
            
                    comparison_data.append({
                        "Attribute": "Data Comparison",
                        "Snowflake Output": count_diff,
                        "SQL Server Output": count_diff,
                        "Comparison": "Data mismatch detected"
                    })


            # Log the column differences
            except Exception as e:
                log_dq_error(f"Exception during comparison in {sqlserver_name},{entity_type}: {str(e)}")
                comparison_data.append({
                    "Attribute": "Data Comparison",
                    "Snowflake Output": "Error",
                    "SQL Server Output": "Error",
                    "Comparison": f"Error - {str(e)}"
                })
                
    
        # Log the results
        for row in comparison_data:
            log_dq_info(f"{row['Attribute']}: {row['Snowflake Output']} | {row['SQL Server Output']} | {row['Comparison']}")

        self.all_comparisons.extend(comparison_data)
        
        if entity_type == 'table':
           output_html_file = f"Dq_analysis/Tables/Table_comparison_report.html"
           # Generate HTML Report
           self.generate_comparison_html(output_html_file)
        elif entity_type == 'view':
            output_html_file = f"Dq_analysis/Views/View_comparison_report.html"
            # Generate HTML Report
            self.generate_comparison_html(output_html_file)
        elif entity_type == 'function':
            output_html_file = f"Dq_analysis/Functions/Function_comparison_report.html"
            # Generate HTML Report
            self.generate_comparison_html(output_html_file)
        elif entity_type == 'procedure':
            output_html_file = f"Dq_analysis/Procedures/Procedure_comparison_report.html"
            # Generate HTML Report
            self.generate_comparison_html(output_html_file)
    
        return 
    



    def generate_comparison_html(self, output_filename):
        html_content = """
        <html>
        <head>
            <title>Comparison Report</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 20px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }
                th, td {
                    border: 1px solid black;
                    padding: 8px;
                    text-align: left;
                }
                th {
                    background-color: #4CAF50;
                    color: white;
                }
                .mismatch {
                    background-color: #ffcccc;
                }
                .entity-header {
                    font-size: 18px;
                    font-weight: bold;
                    background-color: #ddd;
                    padding: 10px;
                    margin-top: 20px;
                    border: 1px solid black;
                    text-align: center;
                }
            </style>
        </head>
        <body>
            <h2>Comparison Report</h2>
        """
    
        entity = {}  # Dictionary to store grouped attributes under each entity name
        current_entity_name = None  # Track the active entity section
    
        for row in self.all_comparisons:
            if row["Attribute"] == "Executed Entity":
                # Extract entity name from Snowflake output
                current_entity_name = row["SQL Server Output"]
                entity[current_entity_name] = []  # Initialize list for this entity
            else:
                if current_entity_name:
                    entity[current_entity_name].append(row)  # Store attributes under this entity
    
        # Generate HTML content for each entity
        for entity_name, rows in entity.items():
            html_content += f'<div class="entity-header">{entity_name}</div>'
            
            if rows:
                table_df = pd.DataFrame(rows)
                html_content += table_df.to_html(index=False, escape=False)
            else:
                html_content += "<p>No additional attributes for this entity.</p>"
    
        html_content += """
        </body>
        </html>
        """
    
        # Write to an HTML file
        with open(output_filename, "w", encoding="utf-8") as file:
            file.write(html_content)
    
        log_dq_info(f"HTML report generated: {output_filename}")