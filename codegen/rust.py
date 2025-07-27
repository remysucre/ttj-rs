import json
import sqlglot
from sqlglot import exp

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


if __name__ == '__main__':
    # The SQL query provided by the user
    sql = """
    SELECT MIN(an.name) AS alternative_name,
           MIN(chn.name) AS voiced_character,
           MIN(n.name) AS voicing_actress,
           MIN(t.title) AS american_movie
    FROM aka_name AS an,
         char_name AS chn,
         cast_info AS ci,
         company_name AS cn,
         movie_companies AS mc,
         name AS n,
         role_type AS rt,
         title AS t
    WHERE ci.note = '(voice)'
      AND cn.country_code ='[us]'
      AND mc.note LIKE '%(200%)%'
      AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')
      AND n.gender ='f'
      AND n.name LIKE '%Angel%'
      AND rt.role ='actress'
      AND t.production_year BETWEEN 2007 AND 2010
      AND ci.movie_id = t.id
      AND t.id = mc.movie_id
      AND ci.movie_id = mc.movie_id
      AND mc.company_id = cn.id
      AND ci.role_id = rt.id
      AND n.id = ci.person_id
      AND chn.id = ci.person_role_id
      AND an.person_id = n.id
      AND an.person_id = ci.person_id;
    """

    # Path to the uploaded statistics file and the desired output file
    # NOTE: You must have the 'sqlglot' library installed: pip install sqlglot
    stats_file = '/Users/niyixuan/projects/treetracker-ubuntu/results/others/simple-cost-model-with-predicates/hj_ordering_hj/job/TTJHP_org.zhu45.treetracker.benchmark.job.q9.Query9bOptJoinTreeOptOrderingShallowHJOrdering.json'
    output_file = 'query_plan_details.json'

    process_query_and_stats(sql, stats_file, output_file)
