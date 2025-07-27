"""
Generate Rust implementations inside https://github.com/remysucre/ttj-rs/
Steps:
1. Use sqlglot to parse JOB queries and also parse stats files to extract
   necessary information. All this information is combined into a json file.
2. Generate query implementation template, which is based on the above json file.
3. Render the template to generate query implementation.
"""

import glob
import json
import os
import re
from typing import Tuple

import sqlglot
from jinja2 import Environment, FileSystemLoader
from sqlglot import exp

ALIAS_TO_TABLE = {
    "an": "aka_name",
    "at": "aka_title",
    "ci": "cast_info",
    "chn": "char_name",
    "cct": "comp_cast_type",
    "cn": "company_name",
    "ct": "company_type",
    "cc": "complete_cast",
    "it": "info_type",
    "k": "keyword",
    "kt": "kind_type",
    "lt": "link_type",
    "mc": "movie_companies",
    "mi_idx": "movie_info_idx",
    "mk": "movie_keyword",
    "ml": "movie_link",
    "mi": "movie_info",
    "n": "name",
    "pi": "person_info",
    "rt": "role_type",
    "t": "title",
}


def format_expression_to_dict(expression):
    """
    Recursively formats a sqlglot expression into a dictionary
    that matches the desired JSON structure for filters.
    """
    if isinstance(expression, exp.Not):
        return {
            "operator": "NOT",
            "left": format_expression_to_dict(expression.this)
        }
    if isinstance(expression, exp.In):
        return {
            "operator": "IN",
            "left": format_expression_to_dict(expression.this),
            "right": [format_expression_to_dict(e) for e in expression.expressions]
        }
    if isinstance(expression, exp.Binary):
        return {
            "operator": expression.key.upper(),
            "left": format_expression_to_dict(expression.left),
            "right": format_expression_to_dict(expression.right)
        }
    else:
        # For literals, columns, or other expressions, convert to SQL string
        return expression.sql()

def process_query_and_stats(sql_query, stats_filepath, output_filepath, pks, fks):
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
        raise ValueError(f"Error reading or parsing statistics file: {e}")

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


    final_output = {}

    # Process each table found in the query
    for alias, name in table_details.items():
        table_info = {
            "relation_name": name,
            "alias": alias,
            "size_after_filters": -1,
            "filters": [],
            "join_cond": []
        }

        # Find the corresponding size from the statistics file using a "longest match" strategy.
        # This prevents incorrect matches for tables with similar names (e.g., 'info_type' vs 'movie_info_type').
        best_match_key = ""
        for stats_key in relation_sizes.keys():
            if name in stats_key and len(stats_key) > len(best_match_key):
                best_match_key = stats_key
        
        if best_match_key:
            table_info["size_after_filters"] = relation_sizes[best_match_key]

        if table_info["size_after_filters"] == -1:
            raise ValueError(f"Size for table '{name}' not found in statistics file: {stats_filepath}")

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
                    
                    local_table_name = ALIAS_TO_TABLE.get(local_col.table)
                    foreign_table_name = ALIAS_TO_TABLE.get(foreign_col.table)
                    
                    local_key = None
                    if pks.get(local_table_name) == local_col.this.this:
                        local_key = "PK"
                    elif fks.get(local_table_name, {}).get(local_col.this.this):
                        local_key = "FK"

                    foreign_key = None
                    if pks.get(foreign_table_name) == foreign_col.this.this:
                        foreign_key = "PK"
                    elif fks.get(foreign_table_name, {}).get(foreign_col.this.this):
                        foreign_key = "FK"

                    table_info["join_cond"].append({
                        "local_column": local_col.this.this,
                        "key": local_key,
                        "foreign_table": {
                            "alias": foreign_col.table,
                            "column": foreign_col.this.this,
                            "key": foreign_key
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


        final_output[alias] = table_info

    # Save the processed data to the output JSON file
    try:
        with open(output_filepath, 'w') as f:
            json.dump(final_output, f, indent=4)
        print(f"Successfully processed query and saved output to '{output_filepath}'")
    except IOError as e:
        raise ValueError(f"Error writing to output file: {e}")


def main():
    """
    Main function to process all .sql files in a directory.
    """
    # Directory containing the SQL query files
    # sql_dir = 'join-order-benchmark/'
    sql_dir = 'junk/'
    # Directory containing the statistics JSON files
    # stats_dir = 'stats_jsons/'
    stats_dir = 'junk/'
    # Directory to save the output JSON files
    # output_dir = 'jsons'
    output_dir = 'junk/'

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pks, fks = parse_sql_schema("imdb-original-mysql.sql")

    # Find all .sql files in the specified directory
    sql_files = glob.glob(os.path.join(sql_dir, '*.sql'))

    if not sql_files:
        raise ValueError(f"No .sql files found in '{sql_dir}'")

    if not os.path.exists("expected_results.json"):
        raise ValueError("expected_results.json is missing! Run extract_results.py to create one.")

    # Process each SQL file
    for sql_file_path in sql_files:
        print(f"Processing {sql_file_path}...")
        try:
            sql_query_name = os.path.basename(sql_file_path).replace('.sql', '')
            
            # Find the corresponding stats file using the specified tokenization logic.
            stats_file_path = None
            for stats_filename in os.listdir(stats_dir):
                try:
                    # Split the filename by '.' to get tokens
                    tokens = stats_filename.split('.')
                    if len(tokens) < 3:  # Expecting at least 'name.qualifier.json'
                        continue

                    # Extract the second-to-last token
                    second_to_last_token = tokens[-2]

                    # Find the substring before "OptJoinTreeOptOrdering"
                    sentinel = "OptJoinTreeOptOrdering"
                    idx = second_to_last_token.find(sentinel)
                    if idx == -1:
                        continue
                    query_part = second_to_last_token[:idx]

                    # Extract the identifier after "Query"
                    query_marker = "Query"
                    marker_idx = query_part.rfind(query_marker)
                    if marker_idx == -1:
                        continue
                    
                    stats_query_name = query_part[marker_idx + len(query_marker):]

                    # Check if the extracted name matches the SQL file's name
                    if sql_query_name.lower() == stats_query_name.lower():
                        stats_file_path = os.path.join(stats_dir, stats_filename)
                        break
                except Exception:
                    # Ignore files that don't match the expected format
                    continue
            
            if not stats_file_path:
                raise ValueError(f"Warning: No stats file found for query '{sql_query_name}' in '{stats_dir}'. Skipping.")

            with open(sql_file_path, 'r') as f:
                sql_query = f.read()
            
            # Construct the output file path
            output_file_path = os.path.join(output_dir, f'{sql_query_name}.json')

            process_query_and_stats(sql_query, stats_file_path, output_file_path, pks, fks)

            optimization(sql_query_name)

        except IOError as e:
            raise ValueError(f"Error reading SQL file {sql_file_path}: {e}")
        except Exception as e:
            raise ValueError(f"An unexpected error occurred while processing {sql_file_path}: {e}")

def parse_sql_schema(sql_file_path):
    with open(sql_file_path, 'r') as f:
        content = f.read()

    pks = {}
    fks = {}

    create_table_blocks = content.split('CREATE TABLE')
    for block in create_table_blocks[1:]:
        table_name_match = re.search(r'`?(\w+)`?\s*\(', block)
        if not table_name_match:
            continue
        table_name = table_name_match.group(1)
        
        # Find PKs
        pk_matches = re.findall(r'(\w+)\s+integer\s+primary\s+key', block)
        if pk_matches:
            pks[table_name] = pk_matches[0]

        # Find FKs from comments
        fk_matches = re.findall(r'--\s*FOREIGN KEY\s*\((\w+)\)\s*REFERENCES\s*(\w+)\s*\((\w+)\)', block)
        if table_name not in fks:
            fks[table_name] = {}
        for fk_col, ref_table, ref_col in fk_matches:
            fks[table_name][fk_col] = (ref_table, ref_col)

    return pks, fks


def _result_output_and_expected_result_set(sql_query_name: str) -> Tuple[str, str]:
    try:
        with open("expected_results.json", 'r') as f:
            stats_data = json.load(f)
        result_set = stats_data.get(sql_query_name, {})
        if len(result_set) == 1:
            result_output = f"Option<{', '.join(['&str'] * len(result_set))}>"
            expected_result_set = f"\"{result_set[0]}\""
        else:
            result_output = f"Option<({', '.join(['&str'] * len(result_set))})>"
            expected_result_set = "(" + ", ".join([f'"{element}"' for element in result_set]) + ")"
        return result_output, expected_result_set
    except (IOError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading or parsing statistics file: {e}")


def optimization(sql_query_name) -> None:
    """
    Generate query implementation based on base.jinja
    """
    result_output, expected_result_set = _result_output_and_expected_result_set(sql_query_name)
    template_data = {"result_output": result_output,
                     "expected_result_set": expected_result_set,
                     "query_name": "q" + sql_query_name}
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("base.jinja")
    query_implementation = template.render(template_data)
    output_dir = "junk"
    output_file_path = os.path.join(output_dir, f'o{sql_query_name}.rs')
    try:
        with open(output_file_path, 'w') as f:
            f.write(query_implementation)
        print(f"Successfully processed query and saved query implementation to '{output_file_path}'")
    except IOError as e:
        raise ValueError(f"Error writing to output file: {e}")

            
if __name__ == '__main__':
    main()
