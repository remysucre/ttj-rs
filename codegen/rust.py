import json
import sqlglot
from sqlglot import exp
import os
import glob

def format_expression_to_dict(expression):
    """
    Recursively formats a sqlglot expression into a dictionary
    that matches the desired JSON structure for filters.
    """
    if isinstance(expression, exp.Binary):
        return {
            "operator": expression.key.upper(),
            "left": format_expression_to_dict(expression.left),
            "right": format_expression_to_dict(expression.right)
        }
    else:
        # For literals, columns, or other expressions, convert to SQL string
        return expression.sql()

def process_query_and_stats(sql_query, stats_filepath, output_filepath):
    """
    Parses an SQL query, extracts metadata for each table, combines it
    with statistics from a file, and saves the result to a JSON file.

    Args:
        sql_query (str): The SQL query string to process.
        stats_filepath (str): Path to the JSON statistics file.
        output_filepath (str): Path to save the output JSON file.
    """
    # Load the statistics from the provided JSON file
    try:
        with open(stats_filepath, 'r') as f:
            stats_data = json.load(f)
        relation_sizes = stats_data.get("Aggregation Stats", {}).get("relationSizes", {})
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing statistics file: {e}")
        return

    # Parse the SQL query using sqlglot
    parsed_query = sqlglot.parse_one(sql_query)

    # Extract all tables and their aliases from the FROM and JOIN clauses
    tables_in_query = parsed_query.find_all(exp.Table)
    table_details = {table.alias_or_name: table.this.this for table in tables_in_query}

    # Extract all conditions from the WHERE clause
    where_clause = parsed_query.find(exp.Where)
    all_conditions = []
    if where_clause:
        # The `flatten()` method correctly breaks down a chain of ANDs into a list of individual conditions.
        all_conditions = list(where_clause.this.flatten())


    final_output = []

    # Process each table found in the query
    for alias, name in table_details.items():
        table_info = {
            "relation_name": name,
            "alias": alias,
            "size_after_filters": 0,
            "filters": [],
            "join_cond": []
        }

        # Find the corresponding size from the statistics file.
        # The keys in the stats file have prefixes (e.g., 'imdb.q9b_'), so we search for a key
        # that ends with the table name, preceded by either a '_' or '.' character.
        for stats_key, size in relation_sizes.items():
            if name in stats_key:
                table_info["size_after_filters"] = size
                break

        # Separate filter conditions and join conditions from the WHERE clause
        filters = []
        for cond in all_conditions:
            column_aliases = {c.table for c in cond.find_all(exp.Column)}

            if len(column_aliases) == 1 and alias in column_aliases:
                # This is a filter condition for the current table
                filters.append(cond)
            elif len(column_aliases) > 1 and alias in column_aliases:
                # This is a join condition involving the current table
                if isinstance(cond, exp.EQ): # Ensure it's an equality join
                    left_col = cond.left
                    right_col = cond.right

                    # Determine which side is local and which is foreign
                    if left_col.table == alias:
                        local_col, foreign_col = left_col, right_col
                    else:
                        local_col, foreign_col = right_col, left_col

                    table_info["join_cond"].append({
                        "local_column": local_col.this.this,
                        "foreign_table": {
                            "alias": foreign_col.table,
                            "column": foreign_col.this.this
                        }
                    })

        # Combine multiple filter conditions with AND
        if len(filters) > 1:
            # Reconstruct the filter structure for JSON output
            # This logic creates a nested AND structure from a flat list of filters
            filter_structure = {"operator": "AND", "left": format_expression_to_dict(filters[0])}
            current_level = filter_structure
            for i in range(1, len(filters) - 1):
                new_level = {"operator": "AND", "left": format_expression_to_dict(filters[i])}
                current_level["right"] = new_level
                current_level = new_level
            current_level["right"] = format_expression_to_dict(filters[-1])
            table_info["filters"] = filter_structure
        elif filters:
            table_info["filters"] = format_expression_to_dict(filters[0])
        else:
             table_info["filters"] = None # No filters for this table


        final_output.append(table_info)

    # Save the processed data to the output JSON file
    try:
        with open(output_filepath, 'w') as f:
            json.dump(final_output, f, indent=4)
        print(f"Successfully processed query and saved output to '{output_filepath}'")
    except IOError as e:
        print(f"Error writing to output file: {e}")


def main():
    """
    Main function to process all .sql files in a directory.
    """
    # Directory containing the SQL query files
    sql_dir = 'join-order-benchmark/'
    # Directory containing the statistics JSON files
    stats_dir = 'stats_jsons/'
    # Directory to save the output JSON files
    output_dir = 'jsons'

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Find all .sql files in the specified directory
    sql_files = glob.glob(os.path.join(sql_dir, '*.sql'))

    if not sql_files:
        print(f"No .sql files found in '{sql_dir}'")
        return

    # Process each SQL file
    for sql_file_path in sql_files:
        print(f"Processing {sql_file_path}...")
        try:
            query_name = os.path.basename(sql_file_path).replace('.sql', '')
            
            # Find the corresponding stats file
            stats_file_path = None
            # The query name in stats file might have different capitalization
            # e.g. 9b vs Query9b. So we search case-insensitively.
            for f in os.listdir(stats_dir):
                if query_name.lower() in f.lower():
                    stats_file_path = os.path.join(stats_dir, f)
                    break
            
            if not stats_file_path:
                print(f"Warning: No stats file found for query '{query_name}' in '{stats_dir}'. Skipping.")
                continue

            with open(sql_file_path, 'r') as f:
                sql_query = f.read()
            
            # Construct the output file path
            output_file_path = os.path.join(output_dir, f'{query_name}.json')

            process_query_and_stats(sql_query, stats_file_path, output_file_path)

        except IOError as e:
            print(f"Error reading SQL file {sql_file_path}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing {sql_file_path}: {e}")

if __name__ == '__main__':
    main()
