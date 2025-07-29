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
import typing
from dataclasses import dataclass
from typing import Tuple

import sqlglot
from jinja2 import Environment, FileSystemLoader
from num2words import num2words
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

@dataclass(frozen=True)
class Attribute:
    attr: str
    alias: str

@dataclass(frozen=True)
class Relation:
    """
    Used to model hyperedge as well.
    """
    alias: str
    relation_name: str
    attributes: typing.Tuple[Attribute, ...]
    size: int

@dataclass
class SemiJoin:
    """
    score is used to implement different optimization idea.
    For example, score could be the size of parent after this semijoin.
    Or, score could be the size of the ear relation.
    """
    ear: Relation
    parent: Relation
    score: int

@dataclass
class MergedSemiJoin:
    ears: typing.List[Relation]
    parent: Relation
    score: int

class Level:
    def __init__(self):
        self.level = []

    def __iter__(self):
        return iter(self.level)

    def append(self, semi_join: SemiJoin):
        if semi_join not in self.level and semi_join.ear not in [sj.ear for sj in self.level]:
            # if semi_join not in self.program:
            self.level.append(semi_join)

    def get_parent(self, relation: Relation) -> Relation:
        """
        Find the parent of the given semi_join
        """
        for sj in self.level:
            if sj.ear == relation:
                # Found a semi-join where the given relation is the ear.
                # Return the parent from that semi-join.
                return sj.parent
        # If the relation is not an ear in any semi-join, it's the root.
        return relation

    # def merge(self):
    #     parent_groups = {}
    #     for sj in self.level:
    #         if sj.parent not in parent_groups:
    #             parent_groups[sj.parent] = []
    #         parent_groups[sj.parent].append(sj)
    #
    #     new_level = MergedLevel()
    #     for parent, semijoins in parent_groups.items():
    #         ears = [sj.ear for sj in semijoins]
    #         total_score = sum(sj.score for sj in semijoins)
    #         new_level.append(MergedSemiJoin(ears=ears, parent=parent, score=total_score))
    #
    #     # Sort by score in non-decreasing order
    #     new_level.level.sort(key=lambda x: x.score)
    #     return new_level

    def __str__(self):
        if not self.level:
            return "SemiJoinProgram is empty."

        output_lines = []
        for sj in self.level:
            output_lines.append(f"ear: {sj.ear.alias}, parent: {sj.parent.alias}, score: {sj.score}")
        return "\n".join(output_lines)

# class MergedLevel:
#     def __init__(self):
#         self.level = []
#
#     def append(self, merged_semi_join : MergedSemiJoin):
#         if merged_semi_join not in self.level:
#             self.level.append(merged_semi_join)
#
#     def __str__(self):
#         if not self.level:
#             return "MergedSemiJoinProgram is empty."
#
#         output_lines = []
#         for sj in self.level:
#             output_lines.append(f"ears: {[ear.alias for ear in sj.ears]}, parent: {sj.parent.alias}, score: {sj.score}")
#         return "\n".join(output_lines)

class SemiJoinProgram:
    def __init__(self):
        self.program = []

    def append(self, level : Level):
        self.program.append(level)

    # def merge(self):
    #     parent_groups = {}
    #     merged_semijoins = MergedSemiJoinProgram()
    #
    #     for level in self.program:
    #         for sj in level:
    #             if sj.parent not in parent_groups:
    #                 parent_groups[sj.parent] = []
    #             parent_groups[sj.parent].append(sj)
    #
    #         new_level = Level()
    #         for parent, semijoins in parent_groups.items():
    #             ears = [sj.ear for sj in semijoins]
    #             total_score = sum(sj.score for sj in semijoins)
    #             new_level.append_merged(MergedSemiJoin(ears=ears, parent=parent, score=total_score))
    #
    #         # Sort by score in non-decreasing order
    #         new_level.level.sort(key=lambda x: x.score)
    #         merged_semijoins.append(new_level)
    #     return merged_semijoins

    def last_level(self):
        return self.program[-1]

    def __str__(self):
        if not self.program:
            return "SemiJoinProgram is empty."
        
        output_lines = []
        for i, level in enumerate(self.program):
            output_lines.append(f"level: {i}")
            for sj in level:
                output_lines.append(f"ear: {sj.ear.alias}, parent: {sj.parent.alias}, score: {sj.score}")
        return "\n".join(output_lines)

# class MergedSemiJoinProgram:
#     def __init__(self):
#         self.program = []
#
#     def append(self, level):
#         self.program.append(level)
#
#     def __str__(self):
#         if not self.program:
#             return "MergedSemiJoinProgram is empty."
#
#         output_lines = []
#         for i, level in enumerate(self.program):
#             output_lines.append(f"level: {i}")
#             for sj in level:
#                 output_lines.append(f"ears: {[ear.alias for ear in sj.ears]}, parent: {sj.parent.alias}, score: {sj.score}")
#         return "\n".join(output_lines)


class UnionFind:
    """
    A Union-Find (or Disjoint Set Union) data structure.

    This implementation uses path compression and union by size/rank optimizations
    to achieve near-constant time complexity for its operations on average.
    It is designed to work with any hashable objects.
    """

    def __init__(self):
        """
        Initializes the UnionFind structure.
        `parent` stores the parent of each element in the set.
        `size` stores the size of the set for union-by-size optimization.
        """
        self.parent = {}
        self.size = {}

    def find(self, item):
        """
        Finds the representative (root) of the set containing the given item.
        Implements path compression for optimization.

        Args:
            item: The item to find.

        Returns:
            The representative of the set containing the item.
        """
        if item not in self.parent:
            self.parent[item] = item
            self.size[item] = 1
            return item

        # Path compression
        if self.parent[item] == item:
            return item
        self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, item1, item2):
        """
        Merges the sets containing item1 and item2.
        Implements union by size to keep the tree structure flat.

        Args:
            item1: The first item.
            item2: The second item.
        """
        root1 = self.find(item1)
        root2 = self.find(item2)

        if root1 != root2:
            # Union by size: attach smaller tree under root of larger tree
            if self.size[root1] < self.size[root2]:
                root1, root2 = root2, root1  # Ensure root1 is the larger set
            self.parent[root2] = root1
            self.size[root1] += self.size[root2]

    def connected(self, item1, item2) -> bool:
        """
        Checks if two items are in the same set.

        Args:
            item1: The first item.
            item2: The second item.

        Returns:
            True if item1 and item2 are in the same set, False otherwise.
        """
        return self.find(item1) == self.find(item2)
    
    def __str__(self) -> str:
        """
        Returns a string representation of the sets in the Union-Find structure.
        Groups elements by their set representative.
        """
        sets = {}
        if not self.parent:
            return "UnionFind is empty."

        for item in self.parent:
            root = self.find(item)
            if root not in sets:
                sets[root] = []
            sets[root].append(item)

        output_lines = []
        for i, (root, members) in enumerate(sets.items()):
            # Sort members for consistent output, converting to string for safety
            sorted_members = sorted(map(str, members))
            output_lines.append(f"Group {i+1} (root: {root}): {{{', '.join(sorted_members)}}}")

        return "\n".join(output_lines)
    
    def num_sets(self) -> int:
        """
        Returns the number of disjoint sets (groups).
        """
        if not self.parent:
            return 0
        
        # Each root of a tree represents a unique set.
        # We can find the number of sets by counting the number of unique roots.
        return len({self.find(item) for item in self.parent})
    
    def get_all_elements(self) -> typing.List:
        """
        Returns a list of all elements in the Union-Find structure.
        """
        return list(self.parent.keys())
    
    def get_set_size(self, item) -> int:
        """
        Returns the size of the set containing the given item.

        Args:
            item: The item whose set size is to be found.

        Returns:
            The size of the set containing the item.
        """
        root = self.find(item)
        return self.size[root]
    
    def get_representatives(self) -> typing.List:
        """
        Returns a list of the representatives (roots) of all sets.
        """
        if not self.parent:
            return []
        
        return list({self.find(item) for item in self.parent})
    
    def remove(self, item):
        """
        Removes an item from the Union-Find structure.

        This operation can be complex. This implementation takes a simplified
        approach: it removes the item and re-parents any direct children to the
        item's parent (or root), ensuring the tree structure remains connected.
        The size of the set is updated accordingly.

        Args:
            item: The item to remove.
        """
        if item not in self.parent:
            return  # Item does not exist

        root = self.find(item)
        
        # Find the parent of the item to be removed.
        # If the item is a root, its parent is itself.
        item_parent = self.parent[item]

        # Find all items that are direct children of the item being removed.
        children = [i for i, p in self.parent.items() if p == item]

        # Re-parent the children to the item's parent.
        for child in children:
            self.parent[child] = item_parent

        # If the item being removed was a root and had children,
        # a new root needs to be established for the set.
        # We've already re-parented the children to the old root (item_parent which is item).
        # We need to pick a new root from the children and re-parent again.
        if item == root and children:
            new_root = children[0]
            # The new root's parent becomes itself.
            self.parent[new_root] = new_root
            # Re-parent the other children and the original parent (if it was a child) to the new root.
            for child in children[1:]:
                self.parent[child] = new_root
            # Update the size map. The new root inherits the size of the set, minus the removed item.
            self.size[new_root] = self.size[item] - 1
            del self.size[item]
        else:
            # If the item was not a root, or was a root with no children,
            # just decrement the size of the set's root.
            if root in self.size:
                 self.size[root] -= 1

        # Finally, remove the item itself from the parent and size maps.
        del self.parent[item]
        if item in self.size and item != root:
             del self.size[item]

def format_expression_to_dict(expression):
    """
    Recursively formats a sqlglot expression into a dictionary
    that matches the desired JSON structure for filters.
    """
    if isinstance(expression, exp.Not):
        return {"operator": "NOT", "left": format_expression_to_dict(expression.this)}
    if isinstance(expression, exp.In):
        return {
            "operator": "IN",
            "left": format_expression_to_dict(expression.this),
            "right": [format_expression_to_dict(e) for e in expression.expressions],
        }
    if isinstance(expression, exp.Binary):
        return {
            "operator": expression.key.upper(),
            "left": format_expression_to_dict(expression.left),
            "right": format_expression_to_dict(expression.right),
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
        with open(stats_filepath, "r") as f:
            stats_data = json.load(f)
        relation_sizes = stats_data.get("Aggregation Stats", {}).get(
            "relationSizes", {}
        )
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
            "join_cond": [],
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
            raise ValueError(
                f"Size for table '{name}' not found in statistics file: {stats_filepath}"
            )

        # Separate filter conditions and join conditions from the WHERE clause
        filters = []
        for cond in all_conditions:
            column_aliases = {c.table for c in cond.find_all(exp.Column)}

            if len(column_aliases) == 1 and alias in column_aliases:
                # This is a filter condition for the current table
                filters.append(cond)
            elif len(column_aliases) > 1 and alias in column_aliases:
                # This is a join condition involving the current table
                if isinstance(cond, exp.EQ):  # Ensure it's an equality join
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

                    table_info["join_cond"].append(
                        {
                            "local_column": local_col.this.this,
                            "key": local_key,
                            "foreign_table": {
                                "alias": foreign_col.table,
                                "column": foreign_col.this.this,
                                "key": foreign_key,
                            },
                        }
                    )

        # Combine multiple filter conditions with AND
        if len(filters) > 1:
            # Reconstruct the filter structure for JSON output
            # This logic creates a nested AND structure from a flat list of filters
            filter_structure = {
                "operator": "AND",
                "left": format_expression_to_dict(filters[0]),
            }
            current_level = filter_structure
            for i in range(1, len(filters) - 1):
                new_level = {
                    "operator": "AND",
                    "left": format_expression_to_dict(filters[i]),
                }
                current_level["right"] = new_level
                current_level = new_level
            current_level["right"] = format_expression_to_dict(filters[-1])
            table_info["filters"] = filter_structure
        elif filters:
            table_info["filters"] = format_expression_to_dict(filters[0])
        else:
            table_info["filters"] = None  # No filters for this table

        final_output[alias] = table_info

    # Save the processed data to the output JSON file
    try:
        with open(output_filepath, "w") as f:
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
    sql_dir = "junk/"
    # Directory containing the statistics JSON files
    # stats_dir = 'stats_jsons/'
    stats_dir = "junk/"
    # Directory to save the output JSON files
    # output_dir = 'jsons'
    output_dir = "junk/"

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pks, fks = parse_sql_schema("imdb-original-mysql.sql")

    # Find all .sql files in the specified directory
    sql_files = glob.glob(os.path.join(sql_dir, "*.sql"))

    if not sql_files:
        raise ValueError(f"No .sql files found in '{sql_dir}'")

    if not os.path.exists("expected_results.json"):
        raise ValueError(
            "expected_results.json is missing! Run extract_results.py to create one."
        )

    # Process each SQL file
    for sql_file_path in sql_files:
        print(f"Processing {sql_file_path}...")
        try:
            sql_query_name = os.path.basename(sql_file_path).replace(".sql", "")

            # Find the corresponding stats file using the specified tokenization logic.
            stats_file_path = None
            for stats_filename in os.listdir(stats_dir):
                try:
                    # Split the filename by '.' to get tokens
                    tokens = stats_filename.split(".")
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

                    stats_query_name = query_part[marker_idx + len(query_marker) :]

                    # Check if the extracted name matches the SQL file's name
                    if sql_query_name.lower() == stats_query_name.lower():
                        stats_file_path = os.path.join(stats_dir, stats_filename)
                        break
                except Exception:
                    # Ignore files that don't match the expected format
                    continue

            if not stats_file_path:
                raise ValueError(
                    f"Warning: No stats file found for query '{sql_query_name}' in '{stats_dir}'. Skipping."
                )

            with open(sql_file_path, "r") as f:
                sql_query = f.read()

            # Construct the output file path
            output_file_path = os.path.join(output_dir, f"{sql_query_name}.json")

            process_query_and_stats(
                sql_query, stats_file_path, output_file_path, pks, fks
            )

            optimization(sql_query_name, output_file_path)

        except IOError as e:
            raise ValueError(f"Error reading SQL file {sql_file_path}: {e}")
        except Exception as e:
            raise ValueError(
                f"An unexpected error occurred while processing {sql_file_path}: {e}"
            )
    os.system(f"cargo fmt -- {os.path.join(output_dir, '*.rs')}")


def parse_sql_schema(sql_file_path):
    with open(sql_file_path, "r") as f:
        content = f.read()

    pks = {}
    fks = {}

    create_table_blocks = content.split("CREATE TABLE")
    for block in create_table_blocks[1:]:
        table_name_match = re.search(r"`?(\w+)`?\s*\(", block)
        if not table_name_match:
            continue
        table_name = table_name_match.group(1)

        # Find PKs
        pk_matches = re.findall(r"(\w+)\s+integer\s+primary\s+key", block)
        if pk_matches:
            pks[table_name] = pk_matches[0]

        # Find FKs from comments
        fk_matches = re.findall(
            r"--\s*FOREIGN KEY\s*\((\w+)\)\s*REFERENCES\s*(\w+)\s*\((\w+)\)", block
        )
        if table_name not in fks:
            fks[table_name] = {}
        for fk_col, ref_table, ref_col in fk_matches:
            fks[table_name][fk_col] = (ref_table, ref_col)

    return pks, fks


def _result_output_and_expected_result_set(sql_query_name: str) -> Tuple[str, str]:
    try:
        with open("expected_results.json", "r") as f:
            stats_data = json.load(f)
        result_set = stats_data.get(sql_query_name, {})
        if len(result_set) == 1:
            result_output = f"Option<{', '.join(['&str'] * len(result_set))}>"
            expected_result_set = f'"{result_set[0]}"'
        else:
            result_output = f"Option<({', '.join(['&str'] * len(result_set))})>"
            expected_result_set = (
                "(" + ", ".join([f'"{element}"' for element in result_set]) + ")"
            )
        return result_output, expected_result_set
    except (IOError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading or parsing statistics file: {e}")


def _initialize_relation_block(output_file_path: str, exclude_relations: typing.List) -> str:
    """
    exclude_relations is used to implement FK-PK optimization.
    """
    try:
        with open(output_file_path, "r") as f:
            query_data = json.load(f)
        aliases = []
        seen_relations = set()
        for alias, info in query_data.items():
            relation = info.get("relation_name")
            if relation not in seen_relations and relation not in exclude_relations:
                aliases.append(re.sub(r"\d+", "", alias))
                seen_relations.add(relation)
        return "\n".join([f"let {alias} = &db.{alias};" for alias in aliases])
    except (IOError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading or parsing statistics file: {e}")

def decide_join_tree(output_file_path):
    def check_ear_consume(one: Relation, two: Relation, pure: bool) -> typing.Union[Tuple[Relation, Relation], Tuple[None, None]]:
        """
        Check if one relation is an ear and is consumed by the other.
        
        If pure is False:
        - Check if one relation's attributes either appear in itself only (set size 1) or appear in the other relation
        - Return [ear, parent] if check passes, [None, None] if it fails
        
        If pure is True:
        - Check if one relation has all its attributes appearing in the other relation
        - Return [ear, parent] where ear is the relation with all attributes in the other, [None, None] otherwise
        """
        def check_one_is_ear(candidate: Relation, other: Relation) -> bool:
            if pure:
                # For pure mode: all attributes of candidate must appear in other
                for attr in candidate.attributes:
                    appears_in_other = any(
                        attributes.connected(attr, other_attr) 
                        for other_attr in other.attributes
                    )
                    if not appears_in_other:
                        return False
                return True
            else:
                # For non-pure mode: attributes either appear in itself only (size 1) or in the other relation
                for attr in candidate.attributes:
                    # set_size = attributes.get_set_size(attr)
                    set_size = len(attribute_alias[attr.attr])
                    if set_size == 1:
                        # Attribute appears in itself only
                        continue
                    else:
                        # Check if it appears in the other relation
                        appears_in_other = any(
                            attributes.connected(attr, other_attr) 
                            for other_attr in other.attributes
                        )
                        if not appears_in_other:
                            return False
                return True
        
        # Check if 'one' is an ear consumed by 'two'
        if check_one_is_ear(one, two):
            # # Remove all attributes of the ear from the attributes UnionFind
            # # TODO: we need to maintain multiset of copies of attributes
            # for attr in one.attributes:
            #     attributes.remove(attr)
            for attr in one.attributes:
                print(f"remove {one.alias} from attribute_alias[{attr.attr}]: {attribute_alias[attr.attr]}")
                attribute_alias[attr.attr].remove(one.alias)
            return one, two
        
        # Check if 'two' is an ear consumed by 'one'
        if check_one_is_ear(two, one):
            # # Remove all attributes of the ear from the attributes UnionFind
            # for attr in two.attributes:
            #     attributes.remove(attr)
            for attr in two.attributes:
                print(f"remove {two.alias} from attribute_alias[{attr.attr}]: {attribute_alias[attr.attr]}")
                attribute_alias[attr.attr].remove(two.alias)
            return two, one
        
        return None, None

    attributes = UnionFind()
    hypergraph = UnionFind()
    attribute_alias = dict()
    try:
        with open(output_file_path, "r") as f:
            query_data = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading or parsing query data file: {e}")

    for alias, info in query_data.items():
        relation_attributes = []
        for join_cond in info.get("join_cond", []):
            local_attr = Attribute(attr=join_cond["local_column"], alias=alias)
            if local_attr not in relation_attributes:
                relation_attributes.append(local_attr)
            # if local_attr not in attribute_alias:
            #     attribute_alias[join_cond["local_column"]] = [alias]
            # elif alias not in attribute_alias[join_cond["local_column"]]:
            #     attribute_alias[join_cond["local_column"]].append(alias)
            foreign_table_info = join_cond["foreign_table"]
            foreign_attr = Attribute(
                attr=foreign_table_info["column"], alias=foreign_table_info["alias"]
            )
            # if foreign_table_info["column"] not in attribute_alias:
            #     attribute_alias[foreign_table_info["column"]] = [alias]
            # elif alias not in attribute_alias[foreign_table_info["column"]]:
            #     attribute_alias[foreign_table_info["column"]].append(alias)
            attributes.union(local_attr, foreign_attr)
        relation_obj = Relation(
            alias=alias,
            relation_name=info["relation_name"],
            attributes=tuple(relation_attributes),
            size=info["size_after_filters"]
        )
        print(f"relation: {relation_obj}")
        hypergraph.find(relation_obj)
        for attr in relation_obj.attributes:
            if attr.attr not in attribute_alias:
                attribute_alias[attr.attr] = [alias]
            else:
                attribute_alias[attr.attr].append(alias)
    print(f"attribute_alias: {attribute_alias}")
    num_relations = len(query_data.items())
    # print(attributes)
    # print(attributes.num_sets())
    # print(hypergraph)
    # print(hypergraph.num_sets())
    semijoin_program = SemiJoinProgram()
    removed_ear = []
    last_level = None
    while hypergraph.num_sets() > 1:
        level = Level()
        if last_level is None:
            last_level = level
        all_representatives = hypergraph.get_representatives()
        all_parent_repr = [last_level.get_parent(repr) for repr in all_representatives]
        num_representatives = len(all_parent_repr)
        for i in range(num_representatives):
            for j in range(num_representatives):
                if i != j and all_parent_repr[i] not in removed_ear and all_parent_repr[j] not in removed_ear:
                    print(f"call check_ear_consume({all_parent_repr[i]}, {all_parent_repr[j]}, {num_relations == num_representatives})")
                    ear, parent = check_ear_consume(all_parent_repr[i], all_parent_repr[j], num_relations == num_representatives)
                    if ear is not None and parent is not None and ear != parent:
                        print(
                            f"{ear.alias}, {parent.alias} = check_ear_consume({all_parent_repr[i]}, {all_parent_repr[j]}, {num_relations == num_representatives})")
                        level.append(SemiJoin(ear=ear, parent=parent, score=ear.size))
                        hypergraph.union(ear, parent)
                        removed_ear.append(ear)
        print(level)
        # print(f"merged level: {level.merge()}")
        print(hypergraph)
        # if num_relations == num_representatives:
        #     level = level.merge()
        semijoin_program.append(level)
        print(semijoin_program)
        last_level = level
    print(f"semijoin_program before merge: \n{semijoin_program}")
    # merged_semijoin_program = semijoin_program.merge()
    # print(f"merged_semijoin_program: \n{merged_semijoin_program}")
    # todo: implement the special optimization logic (idea2 in google doc) using score
    #  the idea is to first merge semijoins in semijoin_program whenever a pair of semijoins
    #  shares the same parent. Then, we update the score by the sum of filters size (note
    #  this is not what we have in idea2 but we stick with this for now). Then, we sort the
    #  semijoins in after-merged semijoin program by score in non-decreasing order.
    # return merged_semijoin_program
    return semijoin_program

def generate_main_block(merged_semijoin_program, output_file_path) -> str:
    with open(output_file_path, "r") as f:
        query_data = json.load(f)
    def find_right_values(node):
        # Helper function to recursively find 'right' values in the filter structure
        values = []
        if isinstance(node, dict):
            if "right" in node:
                # If the right part is a list (like in 'IN' clauses), extend values
                if isinstance(node["right"], list):
                    values.extend(find_right_values(item) for item in node["right"])
                else:
                    values.extend(find_right_values(node["right"]))
            if "left" in node:
                values.extend(find_right_values(node["left"]))
        elif isinstance(node, str):
            # This is a leaf node, which could be a value we are looking for
            # Simple heuristic: if it's quoted, it's likely a literal value.
            if (node.startswith("'") and node.endswith("'")) or \
               (node.startswith('"') and node.endswith('"')) or \
               node.isdigit():
                values.append(node)
        return values

    all_filter_values = {}
    for alias, info in query_data.items():
        filters = info.get("filters")
        if filters:
            all_filter_values[alias] = find_right_values(filters)
    print(all_filter_values)
    main_block = ""
    # At this point, all_filter_values contains the collected 'right' values,
    # for example: {'t': ["'movie'"], 'mi': ["'rating'"]}
    for alias, values in all_filter_values.items():
        if any(isinstance(el, list) for el in values):
            flat_list = [item[0] for item in values]
            content = (",").join([ '"' + value.strip("'") + '"' for value in flat_list])
            main_block += f"""let target_keywords: HashSet<&str> = [{content}].into_iter().collect();"""
        else:
            for value in values:
                raw_val = value.strip("'").strip("%")
                if '%' in raw_val:
                    extra_vals = raw_val.split("%")
                    for val in extra_vals:
                        if val.isnumeric():
                            target = num2words(val).lower().replace(", ", "_").replace(" ", "_").replace("-", "_")
                            main_block += f"""let {target} = memmem::Finder::new("{val}");"""
                        else:
                            main_block += f"""let {val.lower()} = memmem::Finder::new("{val}");"""
                else:
                    if raw_val.isnumeric():
                        target = num2words(raw_val).lower().replace(", ", "_").replace(" ", "_").replace("-", "_")
                        main_block += f"""let {target} = memmem::Finder::new("{raw_val}");"""
                    else:
                        main_block += f"""let {raw_val.lower()} = memmem::Finder::new("{raw_val}");"""
    main_block += "let start = Instant::now();"
    return main_block

def optimization(sql_query_name, output_file_path) -> None:
    """
    Generate query implementation based on base.jinja
    """
    merged_semijoin_program = decide_join_tree(output_file_path)
    main_block = generate_main_block(merged_semijoin_program, output_file_path)
    result_output, expected_result_set = _result_output_and_expected_result_set(
        sql_query_name
    )
    initialize_relation_block = _initialize_relation_block(
        output_file_path, []
    )
    template_data = {
        "result_output": result_output,
        "expected_result_set": expected_result_set,
        "query_name": "q" + sql_query_name,
        "initialize_relation_block": initialize_relation_block,
        "main_block": main_block,
    }
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("base.jinja")
    query_implementation = template.render(template_data)
    output_dir = "junk"
    output_file_path = os.path.join(output_dir, f"o{sql_query_name}.rs")
    try:
        with open(output_file_path, "w") as f:
            f.write(query_implementation)
        print(
            f"Successfully processed query and saved query implementation to '{output_file_path}'"
        )
    except IOError as e:
        raise ValueError(f"Error writing to output file: {e}")

if __name__ == "__main__":
    main()
