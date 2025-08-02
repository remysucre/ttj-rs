"""
A bare-bone query compilation engine for Join Ordering Benchmark queries

The engine generates Rust implementations inside ../src/ in the following steps:
1. Use sqlglot to parse JOB queries and also parse stats files to extract
   necessary information. All this information is combined into a json file.
2. Generate query implementation based on the json file.

Author: Zeyuan Hu (zeyuan.zack.hu@gmail.com)
"""

import glob
import json
import os
import pathlib
import re
import typing
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from functools import reduce
from typing import Tuple

import sqlglot
from jinja2 import Environment, FileSystemLoader, Template
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

@dataclass
class TemplateData:
    template: Template
    data: dict

def check_argument(predicate, message):
    if predicate:
        return
    else:
        raise ValueError(message)


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


class Type(Enum):
    numeric = "numeric"
    set = "set"
    map = "map"
    string = "string"
    map_vec = "map_vec"
    not_need = "not_need"  # happens for the min_loop relation


@dataclass
class Variable:
    """
    Variable used in query implementation
    """

    name: str
    type: Type


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
        if semi_join not in self.level and semi_join.ear not in [
            sj.ear for sj in self.level
        ]:
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

    def merge(self):
        parent_groups = OrderedDict()
        for sj in self.level:
            if sj.parent not in parent_groups:
                parent_groups[sj.parent] = []
            parent_groups[sj.parent].append(sj)

        merged_semijoins = MergedLevel()
        for parent, semijoins in parent_groups.items():
            ears = [sj.ear for sj in semijoins]
            total_score = sum(sj.score for sj in semijoins)
            merged_semijoins.append(
                MergedSemiJoin(ears=ears, parent=parent, score=total_score)
            )

        # Cannot sort by score here due to semijoin order matters!
        return merged_semijoins

    def __str__(self):
        if not self.level:
            return "SemiJoinProgram is empty."

        output_lines = []
        for sj in self.level:
            output_lines.append(
                f"ear: {sj.ear.alias}, parent: {sj.parent.alias}, score: {sj.score}"
            )
        return "\n".join(output_lines)


class MergedLevel:
    def __init__(self):
        self.level = []

    def __iter__(self):
        return iter(self.level)

    def append(self, merged_semi_join: MergedSemiJoin):
        if merged_semi_join not in self.level:
            self.level.append(merged_semi_join)

    def __str__(self):
        if not self.level:
            return "MergedLevel is empty."

        output_lines = []
        for sj in self.level:
            output_lines.append(
                f"ears: {[ear.alias for ear in sj.ears]}, parent: {sj.parent.alias}, score: {sj.score}"
            )
        return "\n".join(output_lines)

    def get_parents(self):
        return [sj.parent for sj in self.level]

    def is_in_level(self, relation: Relation) -> bool:
        for sj in self.level:
            if sj.parent == relation or relation in sj.ears:
                return True
        return False


class SemiJoinProgram:
    def __init__(self):
        self.program = []
        self.parent_child_columns: typing.List[ParentChildColumns] = []

    def append(self, level: MergedLevel):
        self.program.append(level)

    def has_last_level(self):
        if len(self.program) > 0:
            return self.program[-1]
        else:
            return None

    def merge_up(self, level: Level):
        """
        Convert
        semijoin_program:
        level: 0
        ears: ['n', 'chn'], parent: ci, score: 60
        ears: ['cct2', 'cct1'], parent: cc, score: 4
        ears: ['k'], parent: mk, score: 4523930
        ears: ['kt'], parent: t, score: 1
        level: 1
        ears: ['ci'], parent: cc, score: 36244344
        ears: ['cc'], parent: mk, score: 135086
        ears: ['mk'], parent: t, score: 4523930
        into
        semijoin_program:
        level: 0
        ears: ['cct1', 'cct2', 't'], parent: cc, score: 2287275
        ears: ['n', 'chn', 'cc'], parent: ci, score: 135146
        ears: ['k', 'ci'], parent: mk, score: 40768274
        """
        merged_level = MergedLevel()
        assert len(self.program) == 1
        found_sj = []
        for merged_sj in self.program[0]:
            found = False
            for sj in level:
                if sj.parent == merged_sj.parent:
                    found_sj.append(sj)
                    ears = [rel for rel in merged_sj.ears]
                    ears.append(sj.ear)
                    merged_level.append(
                        MergedSemiJoin(
                            ears=ears,
                            parent=sj.parent,
                            score=merged_sj.score + sj.score,
                        )
                    )
                    found = True
                    break
            if not found:
                merged_level.append(merged_sj)
        for sj in level:
            if sj not in found_sj:
                merged_level.append(
                    MergedSemiJoin(ears=[sj.ear], parent=sj.parent, score=sj.score)
                )
        self.program[0] = merged_level

    def __str__(self):
        if not self.program:
            return "SemiJoinProgram is empty."

        output_lines = []
        for i, level in enumerate(self.program):
            output_lines.append(f"level: {i}")
            output_lines.append(str(level))
        return "\n".join(output_lines)

    def get_generation_order(self):
        assert len(self.program) == 1
        level = self.program[0]
        orders = []
        alias_sj = dict()
        for sj in level:
            alias_sj[sj.parent.alias] = sj
            for ear in sj.ears:
                if ear.alias not in orders:
                    orders.append(ear.alias)
            orders.append(sj.parent.alias)
        return orders, alias_sj

    def size(self):
        if not self.program:
            return 0

        # Since program contains at most one MergedLevel
        level = self.program[0]
        relations = set()

        for merged_sj in level:
            # Add the parent relation
            relations.add(merged_sj.parent)
            # Add all ear relations
            for ear in merged_sj.ears:
                relations.add(ear)

        return len(relations)

    def find_parent(self, alias: str) -> typing.Union[Relation, None]:
        assert len(self.program) == 1
        for merged_sj in self.program[0]:
            for ear in merged_sj.ears:
                if ear.alias == alias:
                    return merged_sj.parent
        return None

    def get_root(self) -> Relation:
        assert len(self.program) == 1
        return self.program[0].level[-1].parent

    def get_all_ears(self, alias: str) -> typing.List[str]:
        assert len(self.program) == 1
        for merged_sj in self.program[0]:
            if merged_sj.parent.alias == alias:
                return [ear.alias for ear in merged_sj.ears]


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
            output_lines.append(
                f"Group {i + 1} (root: {root}): {{{', '.join(sorted_members)}}}"
            )

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

        return [self.find(item) for item in self.parent]

    def get_group_members(self, item) -> typing.List[Attribute]:
        """
        Returns a list of all elements in the same group as the given item.

        Args:
            item: The item whose group members are to be found.

        Returns:
            A list of all elements in the same group as the item.
        """
        if item not in self.parent:
            return [item]

        root = self.find(item)
        group_members = []

        for element in self.parent:
            if self.find(element) == root:
                group_members.append(element)

        return group_members


@dataclass
class CodeBlock:
    alias: str
    type: Type
    join_column: str
    zip_columns: typing.List[str]
    nullable_columns: typing.List[str]


@dataclass
class Field:
    alias: str
    nullable: bool
    type: Type
    column: str


@dataclass(frozen=True)
class ParentChildColumns:
    parent_alias: str
    child_alias: str
    parent_column: str
    child_column: str


@dataclass
class ParentChildPhysicalColumns:
    parent_field: Field
    child_field: Field


class ProgramContext:
    def __init__(self, query_data, semijoin_program: SemiJoinProgram):
        self.query_data = query_data
        self.semijoin_program = semijoin_program
        self.selected_fields = self.__construct_selected_fields(query_data)
        self.attributes = self.__build_attributes_union_find(query_data)
        self.parent_child_physical_columns: typing.List[ParentChildPhysicalColumns] = (
            self.__construct_parent_child_physical_columns(semijoin_program, query_data)
        )

    def __build_attributes_union_find(self, query_data) -> UnionFind:
        attributes = UnionFind()
        for alias in sorted(query_data.keys()):
            info = query_data[alias]
            relation_attributes = []
            for join_cond in info.get("join_cond", []):
                local_attr = Attribute(attr=join_cond["local_column"], alias=alias)
                if local_attr not in relation_attributes:
                    relation_attributes.append(local_attr)
                foreign_table_info = join_cond["foreign_table"]
                foreign_attr = Attribute(
                    attr=foreign_table_info["column"], alias=foreign_table_info["alias"]
                )
                attributes.union(local_attr, foreign_attr)
        return attributes

    def __construct_selected_fields(self, query_data) -> typing.List[Field]:
        select_fields = []
        for alias, item in query_data.items():
            column = item["min_select"]
            if column is not None:
                select_fields.append(
                    Field(
                        alias,
                        item["columns"][column]["nullable"],
                        Type(item["columns"][column]["type"]),
                        column,
                    )
                )
        return select_fields

    def __construct_parent_child_physical_columns(
        self, semijoin_program: SemiJoinProgram, query_data
    ) -> typing.List[ParentChildPhysicalColumns]:
        parent_child_physical_columns = []
        for parent_child_column in semijoin_program.parent_child_columns:
            parent_child_physical_columns.append(
                ParentChildPhysicalColumns(
                    parent_field=Field(
                        alias=parent_child_column.parent_alias,
                        column=parent_child_column.parent_column,
                        nullable=query_data[parent_child_column.parent_alias][
                            "columns"
                        ][parent_child_column.parent_column]["nullable"],
                        type=Type(
                            query_data[parent_child_column.parent_alias]["columns"][
                                parent_child_column.parent_column
                            ]["type"]
                        ),
                    ),
                    child_field=Field(
                        alias=parent_child_column.child_alias,
                        column=parent_child_column.child_column,
                        nullable=query_data[parent_child_column.child_alias]["columns"][
                            parent_child_column.child_column
                        ]["nullable"],
                        type=Type(
                            query_data[parent_child_column.child_alias]["columns"][
                                parent_child_column.child_column
                            ]["type"]
                        ),
                    ),
                )
            )
        return parent_child_physical_columns


@dataclass
class CodeGenContext:
    alias_variable: dict
    alias_sj: dict
    template_data: TemplateData
    finders: typing.Set[str] = field(default_factory=set)


def format_expression_to_dict(expression):
    """
    Recursively formats a sqlglot expression into a dictionary
    that matches the desired JSON structure for filters.
    """
    if isinstance(expression, exp.Not):
        # Get the nested expression
        nested_expr = expression.this
        # If the nested expression is a binary operation, merge NOT with its operator
        if isinstance(nested_expr, exp.Binary):
            return {
                "operator": f"NOT {nested_expr.key.upper()}",
                "left": format_expression_to_dict(nested_expr.left),
                "right": format_expression_to_dict(nested_expr.right),
            }
        else:
            # For non-binary expressions, keep the original NOT structure
            return {"operator": "NOT", "left": format_expression_to_dict(nested_expr)}
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
    elif isinstance(expression, exp.Column):
        # For column expressions, return just the column name without the table alias
        if hasattr(expression.this, "this"):
            return expression.this.this
        else:
            return expression.this
    else:
        # For literals or other expressions, convert to SQL string
        return expression.sql()


def process_query_and_stats(
    sql_query, stats_filepath, output_filepath, pks, fks, table_columns
):
    """
    Parses an SQL query, extracts metadata for each table, combines it
    with statistics from a file, and saves the result to a JSON file.

    Args:
        sql_query (str): The SQL query string to process.
        stats_filepath (str): Path to the JSON statistics file.
        output_filepath (str): Path to save the output JSON file.
        pks (dict): Primary key information for tables.
        fks (dict): Foreign key information for tables.
        table_columns (dict): Column information for tables.
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

    # Extract aggregation functions from SELECT clause
    select_clause = parsed_query.find(exp.Select)
    aggregation_map = {}  # Maps alias to column name for aggregations
    if select_clause:
        for expression in select_clause.expressions:
            # Check if this is an aggregation function (MIN, MAX, COUNT, SUM, AVG)
            # or an aliased aggregation function
            actual_expr = expression

            # If it's an alias, get the underlying expression
            if isinstance(expression, exp.Alias):
                actual_expr = expression.this

            if isinstance(actual_expr, exp.Min):
                # Extract the column reference
                column = actual_expr.this
                if isinstance(column, exp.Column) and column.table:
                    aggregation_map[column.table] = column.this.this
            elif isinstance(actual_expr, exp.Max):
                column = actual_expr.this
                if isinstance(column, exp.Column) and column.table:
                    aggregation_map[column.table] = column.this.this
            elif isinstance(actual_expr, exp.Count):
                column = actual_expr.this
                if isinstance(column, exp.Column) and column.table:
                    aggregation_map[column.table] = column.this.this
            elif isinstance(actual_expr, exp.Sum):
                column = actual_expr.this
                if isinstance(column, exp.Column) and column.table:
                    aggregation_map[column.table] = column.this.this
            elif isinstance(actual_expr, exp.Avg):
                column = actual_expr.this
                if isinstance(column, exp.Column) and column.table:
                    aggregation_map[column.table] = column.this.this

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
            "min_select": aggregation_map.get(alias, None),
            "columns": table_columns.get(name, {}),
        }

        # Find the corresponding size from the statistics file.
        # For aliases with numbers (like cct1, cct2), match both table name and alias suffix.
        best_match_key = ""

        # Extract numeric suffix from alias if present
        alias_match = re.search(r"(\D+)(\d*)$", alias)
        if alias_match:
            alias_base, alias_suffix = alias_match.groups()
            # Try to find a key that matches both the table name and the alias pattern
            for stats_key in relation_sizes.keys():
                # Look for patterns like "q20a_comp_cast_type1" for alias "cct1"
                if name in stats_key:
                    if alias_suffix:  # If alias has a number suffix
                        # Check if the stats key ends with the same number
                        if stats_key.endswith(alias_suffix) and len(stats_key) > len(
                            best_match_key
                        ):
                            best_match_key = stats_key
                    else:  # If alias has no number suffix
                        # Prefer keys without numbers, or if no such key exists, take any match
                        if not re.search(r"\d+$", stats_key):
                            if len(stats_key) > len(best_match_key):
                                best_match_key = stats_key
                        elif not best_match_key:  # Fallback to any match
                            best_match_key = stats_key

        # Fallback to original logic if no specific match found
        if not best_match_key:
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

                    # Strip trailing numbers from alias to handle cases like cct1, cct2, etc.
                    local_alias_base = re.sub(r"\d+$", "", local_col.table)
                    foreign_alias_base = re.sub(r"\d+$", "", foreign_col.table)

                    local_table_name = ALIAS_TO_TABLE.get(local_alias_base)
                    foreign_table_name = ALIAS_TO_TABLE.get(foreign_alias_base)

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

    pks, fks, table_columns = parse_sql_schema("imdb-original-mysql.sql")

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
                sql_query, stats_file_path, output_file_path, pks, fks, table_columns
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
    columns = {}

    create_table_blocks = content.split("CREATE TABLE")
    for block in create_table_blocks[1:]:
        table_name_match = re.search(r"`?(\w+)`?\s*\(", block)
        if not table_name_match:
            continue
        table_name = table_name_match.group(1)

        # Initialize columns dictionary for this table
        columns[table_name] = {}

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

        # Extract column definitions
        # Split by lines and look for column definitions
        lines = block.split("\n")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("--") or line.startswith(")"):
                continue

            # Match column definitions: column_name data_type [NOT NULL|NULL]
            column_match = re.match(
                r"(\w+)\s+(integer|text|character\s+varying\(\d+\))\s*(.*)",
                line,
                re.IGNORECASE,
            )
            if column_match:
                col_name = column_match.group(1)
                col_type_raw = column_match.group(2).lower()
                col_modifiers = column_match.group(3).lower()

                # Determine simplified type
                if "integer" in col_type_raw:
                    col_type = "numeric"
                elif "text" in col_type_raw or "character varying" in col_type_raw:
                    col_type = "string"
                else:
                    col_type = "string"  # default fallback

                # Determine nullability
                # If 'not null' is explicitly specified, it's not nullable
                # If 'primary key' is specified, it's not nullable
                # Otherwise, it's nullable by default
                nullable = True
                if "not null" in col_modifiers or "primary key" in col_modifiers:
                    nullable = False

                columns[table_name][col_name] = {"type": col_type, "nullable": nullable}

    return pks, fks, columns


def get_expected_result_set(sql_query_name: str, program_context: ProgramContext) -> str:
    types = get_rust_types_for_fields(program_context.selected_fields)
    try:
        with open("expected_results.json", "r") as f:
            stats_data = json.load(f)
        result_set = stats_data.get(sql_query_name, {})
        if len(result_set) == 1:
            if types[0] == "&str":
                expected_result_set = f'"{result_set[0]}"'
            elif types[0] == "&i32":
                expected_result_set = f'{result_set[0]}'
        else:
            expected_result_set = []
            for i, type in enumerate(types):
                if type == "&str":
                    expected_result_set.append(f'"{result_set[i]}"')
                elif type == "&i32":
                    expected_result_set.append(f'{result_set[i]}')
        return "(" + ", ".join(expected_result_set) + ")"
    except (IOError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading or parsing statistics file: {e}")




def _initialize_relation_block(
    output_file_path: str, exclude_relations: typing.List
) -> str:
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
    def build_parent_child_columns(
        semijoin_program: SemiJoinProgram, attributes: UnionFind
    ):
        assert len(semijoin_program.program) == 1
        merged_level = semijoin_program.program[0]
        for merged_sj in merged_level:
            for ear in merged_sj.ears:
                for attr in ear.attributes:
                    for attr2 in merged_sj.parent.attributes:
                        if attributes.connected(attr, attr2):
                            semijoin_program.parent_child_columns.append(
                                ParentChildColumns(
                                    parent_alias=merged_sj.parent.alias,
                                    child_alias=ear.alias,
                                    parent_column=attr2.attr,
                                    child_column=attr.attr,
                                )
                            )

    def check_ear_consume(
        one: Relation, two: Relation, pure: bool
    ) -> typing.Union[Tuple[Relation, Relation], Tuple[None, None]]:
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
                    if attributes.get_set_size(attr) > 2:
                        # Handle the case of 1a where title is considered as a filter relation,
                        # which could lead to less ideal join ordering.
                        return False
                return True
            else:
                # For non-pure mode: attributes either appear in itself only (size 1) or in the other relation
                unique_attrs = []
                for attr in candidate.attributes:
                    # set_size = attributes.get_set_size(attr)
                    set_size = len(attribute_alias[attr.attr])
                    if set_size == 1:
                        # Attribute appears in itself only
                        unique_attrs.append(attr)
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
            for attr in one.attributes:
                print(
                    f"remove {one.alias} from attribute_alias[{attr.attr}]: {attribute_alias[attr.attr]}"
                )
                attribute_alias[attr.attr].remove(one.alias)
            return one, two

        # Check if 'two' is an ear consumed by 'one'
        if check_one_is_ear(two, one):
            for attr in two.attributes:
                print(
                    f"remove {two.alias} from attribute_alias[{attr.attr}]: {attribute_alias[attr.attr]}"
                )
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

    # Sort by alias to ensure deterministic ordering
    for alias in sorted(query_data.keys()):
        info = query_data[alias]
        relation_attributes = []
        for join_cond in info.get("join_cond", []):
            local_attr = Attribute(attr=join_cond["local_column"], alias=alias)
            if local_attr not in relation_attributes:
                relation_attributes.append(local_attr)
            foreign_table_info = join_cond["foreign_table"]
            foreign_attr = Attribute(
                attr=foreign_table_info["column"], alias=foreign_table_info["alias"]
            )
            attributes.union(local_attr, foreign_attr)
        relation_obj = Relation(
            alias=alias,
            relation_name=info["relation_name"],
            attributes=tuple(relation_attributes),
            size=info["size_after_filters"],
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
    semijoin_program = SemiJoinProgram()
    removed_ear = []
    while hypergraph.num_sets() > 1:
        level = Level()
        last_level = semijoin_program.has_last_level()
        if last_level is None:
            last_level = level
            all_representatives = sorted(
                hypergraph.get_representatives(), key=lambda x: x.alias
            )
            all_parent_repr = [
                last_level.get_parent(repr) for repr in all_representatives
            ]
        else:
            all_parent_repr = sorted(last_level.get_parents(), key=lambda x: x.alias)
            for repr in sorted(hypergraph.get_representatives(), key=lambda x: x.alias):
                if repr not in all_parent_repr and not last_level.is_in_level(repr):
                    all_parent_repr.append(repr)
            print(f"all_parent_repr (not pure): {all_parent_repr}")
        num_representatives = len(all_parent_repr)
        # Sort representatives for deterministic ordering
        all_parent_repr = sorted(all_parent_repr, key=lambda x: x.alias)
        if num_relations == num_representatives:
            for i in range(num_representatives):
                for j in range(num_representatives):
                    if (
                        i != j
                        and all_parent_repr[i] not in removed_ear
                        and all_parent_repr[j] not in removed_ear
                    ):
                        print(
                            f"call check_ear_consume({all_parent_repr[i]}, {all_parent_repr[j]}, {num_relations == num_representatives})"
                        )
                        ear, parent = check_ear_consume(
                            all_parent_repr[i],
                            all_parent_repr[j],
                            num_relations == num_representatives,
                        )
                        if ear is not None and parent is not None and ear != parent:
                            print(
                                f"{ear.alias}, {parent.alias} = check_ear_consume({all_parent_repr[i]}, {all_parent_repr[j]}, {num_relations == num_representatives})"
                            )
                            level.append(
                                SemiJoin(ear=ear, parent=parent, score=ear.size)
                            )
                            hypergraph.union(ear, parent)
                            removed_ear.append(ear)
        else:
            queue = deque()
            # Sort to ensure deterministic order
            sorted_parent_repr = sorted(all_parent_repr, key=lambda x: x.alias)
            queue.extend(sorted_parent_repr)
            while len(queue) > 0:
                relation1 = queue.popleft()
                relation2 = queue.popleft()
                # todo: Here, we can implement some tiebreaking rule such as set relation1 with
                #  with the relation that has the smallest size filter relations. We need tiebreaking
                #  because relation1 could be ear and relation2 could be parent and vice versa.
                print(
                    f"call check_ear_consume({relation1}, {relation2}, {num_relations == num_representatives})"
                )
                ear, parent = check_ear_consume(
                    relation1,
                    relation2,
                    num_relations == num_representatives,
                )
                if ear is not None and parent is not None and ear != parent:
                    print(
                        f"{ear.alias}, {parent.alias} = check_ear_consume({relation1}, {relation2}, {num_relations == num_representatives})"
                    )
                    level.append(SemiJoin(ear=ear, parent=parent, score=ear.size))
                    hypergraph.union(ear, parent)
                    removed_ear.append(ear)
                    if len(queue) > 0:
                        queue.appendleft(parent)

        print(level)
        print(hypergraph)
        if semijoin_program.has_last_level() is None:
            semijoin_program.append(level.merge())
        else:
            semijoin_program.merge_up(level)
        print(f"semijoin_prorgam (iteration): \n{semijoin_program}")
    print(f"semijoin_program: \n{semijoin_program}")
    assert num_relations == semijoin_program.size()
    build_parent_child_columns(semijoin_program, attributes)
    # todo: implement the special optimization logic (idea2 in google doc) using score
    #  the idea is to first merge semijoins in semijoin_program whenever a pair of semijoins
    #  shares the same parent. Then, we update the score by the sum of filters size (note
    #  this is not what we have in idea2 but we stick with this for now). Then, we sort the
    #  semijoins in after-merged semijoin program by score in non-decreasing order.
    return semijoin_program


def process_filters(
    alias,
    filter_dict,
    code_gen_context: CodeGenContext,
    program_context: ProgramContext,
) -> typing.List[str]:
    if not isinstance(filter_dict, dict):
        # Base case: it's a string value
        if (
            filter_dict in program_context.query_data[alias]["columns"]
            and not program_context.query_data[alias]["columns"][filter_dict][
                "nullable"
            ]
        ):
            return [f"*{filter_dict}"]
        return [filter_dict]

    operator = filter_dict["operator"]
    left = filter_dict["left"]
    right = filter_dict["right"]

    # Process left and right operands
    if isinstance(left, dict):
        left_expr = process_filters(alias, left, code_gen_context, program_context)
    else:
        # Check if left operand needs dereference
        if (
            left in program_context.query_data[alias]["columns"]
            and not program_context.query_data[alias]["columns"][left]["nullable"]
        ):
            left_expr = [f"*{left}"]
        else:
            left_expr = [left]

    if isinstance(right, dict):
        right_expr = process_filters(alias, right, code_gen_context, program_context)
    else:
        right_expr = [right]

    if operator == "LIKE":
        search_terms = []
        for term in right_expr:
            if isinstance(term, str) and term.startswith("'") and term.endswith("'"):
                clean_term = term.strip("'").strip("%")
                if clean_term and "%" not in clean_term:
                    search_terms.append(clean_term)
                elif "%" in clean_term:
                    search_terms.extend(clean_term.split("%"))

        conditions = []
        for term in search_terms:
            finder_var_name = term.lower().replace(" ", "_").replace("-", "_").strip(')').strip('(')
            finder_declaration = (
                f"""let {finder_var_name} = memmem::Finder::new("{term}");"""
            )
            code_gen_context.finders.add(finder_declaration)
            conditions.append(
                f"{finder_var_name}.find({left_expr[0].replace('*', '')}.as_bytes()).is_some()"
            )

        if conditions:
            if len(conditions) == 1:
                return [conditions[0]]
            return ["(" + "&&".join(conditions) + ")"]
        return ["true"]

    elif operator == "NOT LIKE":
        search_terms = []
        for term in right_expr:
            if isinstance(term, str) and term.startswith("'") and term.endswith("'"):
                clean_term = term.strip("'").strip("%")
                if clean_term and "%" not in clean_term:
                    search_terms.append(clean_term)
                elif "%" in clean_term:
                    search_terms.extend(clean_term.split("%"))

        conditions = []
        for term in search_terms:
            finder_var_name = term.lower().replace(" ", "_").replace("-", "_").strip('(').strip(')')
            finder_declaration = (
                f"""let {finder_var_name} = memmem::Finder::new("{term}");"""
            )
            code_gen_context.finders.add(finder_declaration)
            conditions.append(
                f"{finder_var_name}.find({left_expr[0].replace('*', '')}.as_bytes()).is_none()"
            )

        if conditions:
            if len(conditions) == 1:
                return [conditions[0]]
            return ["(" + "||".join(conditions) + ")"]
        return ["true"]

    elif operator == "OR":
        return [f"({left_expr[0]} || {right_expr[0]})"]

    elif operator == "AND":
        return [f"({left_expr[0]} && {right_expr[0]})"]

    elif operator == "IN":
        # Convert IN operator to a set-based lookup
        values = [ele.strip("'") for ele in right_expr[0]]
        if len(values) == 1:
            return [f'({left_expr[0]} == "{values[0]}")']
        else:
            # Create a set lookup for multiple values
            values_str = ", ".join([f'"{v}"' for v in values])
            return [
                f"[{values_str}].contains(&{left_expr[0].replace('*', '')}.as_str())"
            ]

    elif operator == "GT":
        return [f"({left_expr[0]} > {right_expr[0]})"]

    elif operator == "EQ":
        return [f"({left_expr[0]} == {right_expr[0]})"]

    elif operator == "NEQ":
        return [f"({left_expr[0]} != {right_expr[0]})"]


def format_zip_column(zip_columns, alias):
    output = ""
    base_table = re.sub(r"\d+", "", alias)
    output += f"{base_table}.{zip_columns[0]}.iter()"
    for column in zip_columns[1:]:
        output += f".zip({base_table}.{column}.iter())"
    return output


def build_filter_columns(filter_dict):
    columns = set()

    def collect_columns(filter_dict):
        if not isinstance(filter_dict, dict):
            return

        left = filter_dict.get("left")
        right = filter_dict.get("right")

        # Collect column from left operand
        if isinstance(left, str):
            columns.add(left)
        elif isinstance(left, dict):
            collect_columns(left)

        # Collect columns from right operand
        if isinstance(right, dict):
            collect_columns(right)
        elif isinstance(right, list):
            for item in right:
                if isinstance(item, dict):
                    collect_columns(item)

    if filter_dict:
        collect_columns(filter_dict)

    return list(columns)


def build_filter_map(zip_columns):
    if not zip_columns:
        return None
    if len(zip_columns) == 1:
        return f"{zip_columns[0]}"
    initial_tuple = (f"{zip_columns[0]}", f"{zip_columns[1]}")
    return reduce(
        lambda accumulator, item: (accumulator, f"{item}"),
        zip_columns[2:],
        initial_tuple,
    )


def build_old_filter_map(zip_columns):
    if not zip_columns:
        return None
    if len(zip_columns) == 1:
        return f"old_{zip_columns[0]}"
    initial_tuple = (f"old_{zip_columns[0]}", f"old_{zip_columns[1]}")
    return reduce(
        lambda accumulator, item: (accumulator, f"old_{item}"),
        zip_columns[2:],
        initial_tuple,
    )


def build_some_conditions(zip_columns, nullable_columns) -> str:
    zip_nullable_columns = [x for x in nullable_columns if x in zip_columns]
    return "&&".join(
        [f"let Some({column}) = {column}" for column in zip_nullable_columns]
    )


def build_code_block(alias, query_item, program_context: ProgramContext) -> CodeBlock:
    def build_zip(query_item):
        zip_columns = []
        for item in query_item["join_cond"]:
            if item["local_column"] not in zip_columns:
                zip_columns.append(item["local_column"])
        filter_columns = build_filter_columns(query_item["filters"])
        for column in filter_columns:
            if column not in zip_columns:
                zip_columns.append(column)
        if (
            query_item["min_select"] is not None
            and query_item["min_select"] not in zip_columns
        ):
            zip_columns.append(query_item["min_select"])
        return zip_columns

    def get_nullable_columns(query_item) -> typing.List[str]:
        nullable_columns = []
        for column_name, column_item in query_item["columns"].items():
            if column_item["nullable"]:
                nullable_columns.append(column_name)
        return nullable_columns

    def join_column(
        alias, query_item, program_context: ProgramContext
    ) -> Tuple[typing.Union[str, None], typing.Union[str, None]]:
        parent_relation: Relation = program_context.semijoin_program.find_parent(alias)
        child_column = ""
        if not parent_relation:
            # can happen for the root
            return None, None
        for (
            parent_child_column
        ) in program_context.semijoin_program.parent_child_columns:
            check_argument(parent_relation is not None, f"alias: {alias}")
            if (
                parent_child_column.parent_alias == parent_relation.alias
                and parent_child_column.child_alias == alias
            ):
                child_column = parent_child_column.child_column
        for join_cond in query_item["join_cond"]:
            if join_cond["local_column"] == child_column:
                return child_column, join_cond["key"]

    def decide_type(
        alias, query_item, program_context: ProgramContext, key: str
    ) -> Type:
        if query_item["size_after_filters"] == 1:
            return Type.numeric
        elif not query_item["min_select"]:
            return Type.set
        else:
            parent_alias = program_context.semijoin_program.find_parent(alias)
            if not parent_alias:
                return Type.not_need
            else:
                if key == "PK":
                    return Type.map
                else:
                    return Type.map_vec

    join_column, key = join_column(alias, query_item, program_context)
    return CodeBlock(
        alias=alias,
        zip_columns=build_zip(query_item),
        nullable_columns=get_nullable_columns(query_item),
        join_column=join_column,
        type=decide_type(alias, query_item, program_context, key),
    )

def get_rust_types_for_fields(fields: typing.List[Field]) -> typing.List[str]:
    types = []
    for selected_field in fields:
        if selected_field.type == Type.string:
            types.append("&str")
        elif selected_field.type == Type.numeric:
            types.append("&i32")
    return types


def format_result_output(program_context: ProgramContext) -> str:
    types = get_rust_types_for_fields(program_context.selected_fields)
    if len(types) == 1:
        result_output = f"Option<{', '.join(types)}>"
    else:
        result_output = f"Option<({', '.join(types)})>"
    return result_output


def generate_code_block(
    code_block: CodeBlock,
    program_context: ProgramContext,
    code_gen_context: CodeGenContext,
) -> str:
    def build_res_match(program_context: ProgramContext) -> str:
        res_match = Template("""
        res = match res {
            Some({{ old_filter_map_closure|replace("'","")}}) => Some((
                {{ comparison }}
            )),
            None => Some({{ min_none_arm|replace("'","")}}),
        };
        """)
        field_columns = [field.column for field in program_context.selected_fields]
        data["old_filter_map_closure"] = build_old_filter_map(field_columns)
        data["filter_map_closure"] = build_filter_map(field_columns)
        comparison = []
        min_none_arm = []
        for selected_field in program_context.selected_fields:
            if not selected_field.nullable:
                comparison.append(
                    f"{selected_field.column}.min(&old_{selected_field.column})"
                )
                min_none_arm.append(selected_field.column)
            else:
                comparison.append(f"{selected_field.column}.iter().min().unwrap().min(&old_{selected_field.column})")
                min_none_arm.append(f"{selected_field.column}.iter().min().unwrap()")
        data["comparison"] = ",".join(comparison)
        data["min_none_arm"] = ",".join(min_none_arm)
        return res_match.render(data)

    def form_join_conds(query_item, code_gen_context: CodeGenContext) -> str:
        join_conds = []
        merged_sj: MergedSemiJoin = code_gen_context.alias_sj[code_block.alias]
        for ear in merged_sj.ears:
            for join_cond in query_item["join_cond"]:
                foreign_table_alias = join_cond["foreign_table"]["alias"]
                if foreign_table_alias == ear.alias:
                    if (
                        code_gen_context.alias_variable[foreign_table_alias].type
                        == Type.numeric
                    ):
                        join_conds.append(
                            f"*{join_cond['local_column']} == {code_gen_context.alias_variable[foreign_table_alias].name}"
                        )
                    elif (
                        code_gen_context.alias_variable[foreign_table_alias].type
                        == Type.set
                    ):
                        join_conds.append(
                            f"{code_gen_context.alias_variable[foreign_table_alias].name}.contains(&{join_cond['local_column']})"
                        )
                    elif (
                        code_gen_context.alias_variable[foreign_table_alias].type
                        == Type.map
                    ):
                        join_conds.append(
                            f"{code_gen_context.alias_variable[foreign_table_alias].name}.contains_key({join_cond['local_column']})"
                        )
        return "&&".join(join_conds)

    def build_filter_map_out(
        code_block: CodeBlock, program_context: ProgramContext
    ) -> str:
        is_min_loop = (
            program_context.semijoin_program.get_root().alias == code_block.alias
        )
        query_item = program_context.query_data[code_block.alias]
        target = []
        if is_min_loop:
            for selected_field in program_context.selected_fields:
                if selected_field.column == "title":
                    target.append("title.as_str()")
            if len(target) == 1:
                return f"{target[0]}"
            else:
                return "(" + ",".join(target) + ")"
        else:
            min_select_column = query_item["min_select"]
            parent = program_context.semijoin_program.find_parent(code_block.alias)
            assert parent is not None
            for parent_child_column in program_context.parent_child_physical_columns:
                if (
                    parent_child_column.child_field.alias == code_block.alias
                    and parent_child_column.parent_field.alias
                ):
                    if parent_child_column.child_field.type == Type.numeric:
                        target.append(f"*{parent_child_column.child_field.column}")
                    elif parent_child_column.child_field.type == Type.string:
                        target.append(
                            f"{parent_child_column.child_field.column}.as_str()"
                        )
            if min_select_column is not None:
                min_select_column_type = Type(
                    query_item["columns"][min_select_column]["type"]
                )
                if min_select_column_type == Type.numeric:
                    target.append(f"*{min_select_column}")
                elif min_select_column_type == Type.string:
                    target.append(f"{min_select_column}.as_str()")
            return "(" + ",".join(target) + ")"

    def build_filter_map_main(
        code_block, program_context: ProgramContext, code_gen_context: CodeGenContext
    ) -> str:
        query_item = program_context.query_data[code_block.alias]
        filter_columns = build_filter_columns(query_item["filters"])
        filter_nullable_columns = [
            x for x in code_block.nullable_columns if x in filter_columns
        ]
        assert len(filter_nullable_columns) <= 1
        filter_conditions = process_filters(
            code_block.alias, query_item["filters"], code_gen_context, program_context
        )
        if code_block.alias in code_gen_context.alias_sj:
            join_conditions = form_join_conds(query_item, code_gen_context)
        else:
            join_conditions = None
        nullable_local_variable = None
        nullable_column_exists = len(filter_nullable_columns) == 1
        if nullable_column_exists:
            nullable_local_variable = filter_nullable_columns[0].strip("'")
        else:
            for zip_column in code_block.zip_columns:
                if query_item["columns"][zip_column]["nullable"]:
                    nullable_column_exists = True
                    nullable_local_variable = zip_column
                    break
        if nullable_column_exists:
            if filter_conditions[0] is not None:
                filter_conditions = (
                    filter_conditions[0].strip("'").removeprefix('(').removesuffix(')')
                )
            else:
                filter_conditions = None
            map_out = build_filter_map_out(code_block, program_context)
            if code_block.alias in code_gen_context.alias_sj:
                conditions = []
                if filter_conditions is not None:
                    conditions.append(filter_conditions)
                if join_conditions is not None:
                    conditions.append(join_conditions)
                return f"""{nullable_local_variable}
                    .filter(|&{nullable_local_variable}| {"&&".join(conditions)})
                    .map(|{nullable_local_variable}| {map_out})"""
            else:
                return f"""{nullable_local_variable}
                    .filter(|&{nullable_local_variable}| {filter_conditions})
                    .map(|{nullable_local_variable}| {map_out})"""
        else:
            case1_template = Template("""
                {% set conditions = [] %}
                {% if filter_conditions is not none %}
                    {% set _ = conditions.append(filter_conditions) %}
                {% endif %}
                {% if join_conditions is not none %}
                    {% set _ = conditions.append(join_conditions) %}
                {% endif %}
                {% if conditions %}
                ({{ conditions | join(' && ') }})
                .then_some(*{{ join_column }})
                {% else %}
                Some(*{{ join_column }})
                {% endif %}
            """)
            data = dict()
            data["filter_conditions"] = (
                filter_conditions[0] if filter_conditions else None
            )
            data["join_conditions"] = join_conditions
            data["join_column"] = code_block.join_column
            return case1_template.render(data)

    data = dict()
    data["alias"] = code_block.alias
    data["zip_columns"] = format_zip_column(code_block.zip_columns, code_block.alias)
    data["filter_map_closure"] = build_filter_map(code_block.zip_columns)
    filter_conditions = process_filters(
        code_block.alias,
        program_context.query_data[code_block.alias]["filters"],
        code_gen_context,
        program_context,
    )
    data["filter_conditions"] = filter_conditions[0] if filter_conditions else None
    data["join_column"] = code_block.join_column
    if code_block.alias in code_gen_context.alias_sj:
        query_item = program_context.query_data[code_block.alias]
        data["join_conditions"] = form_join_conds(query_item, code_gen_context)
    else:
        data["join_conditions"] = None
    data["filter_map_main"] = build_filter_map_main(
        code_block, program_context, code_gen_context
    )

    if program_context.semijoin_program.get_root().alias == code_block.alias:
        # in the min_loop
        data["result_output"] = code_gen_context.template_data.data["result_output"]
        if len(program_context.selected_fields) == 1:
            # in the single_output
            code_block_template = Template("""
            let res: {{ result_output }} =
            {{ zip_columns }}
            .filter_map(|{{ filter_map_closure|replace("'","") }}| {
                {{ filter_map_main }}
            })
            .min();
            """)
            return code_block_template.render(data)
        else:
            code_block_template = Template("""
            let mut res: {{ result_output }} = None;
            for {{ filter_map_closure|replace("'","")}} in
            {{ zip_columns }}
            {
                {% set conditions = [] %}
                {% if some_conditions is not none %}
                    {% set _ = conditions.append(some_conditions) %}
                {% endif %}
                {% if filter_conditions is not none %}
                    {% set _ = conditions.append(filter_conditions) %}
                {% endif %}
                {% if join_conditions is not none %}
                    {% set _ = conditions.append(join_conditions) %}
                {% endif %}

                if {{ conditions | join(' && ') }} {
                    {{ res_match }}
                }
            }
            """)
            data["some_conditions"] = build_some_conditions(
                code_block.zip_columns, code_block.nullable_columns
            )
            data["res_match"] = build_res_match(program_context)
            return code_block_template.render(data)
    elif code_block.type == Type.numeric:
        template = Template("""
        let {{ alias }}_id =
        {{ zip_columns }}
        .find(|{{ filter_map_closure|replace("'","") }}| {{ filter_conditions|replace("'",'"') }})
        .map(|{{ filter_map_closure|replace("'","") }}| *{{ join_column }})
        .unwrap();
        """)
        code_gen_context.alias_variable[code_block.alias] = Variable(
            name=f"{code_block.alias}_id", type=code_block.type
        )
        return template.render(data)
    elif code_block.type == Type.set:
        template = Template("""
        let {{ alias }}_s : HashSet<i32> = 
        {{ zip_columns }}
        .filter_map(|{{ filter_map_closure|replace("'","") }}| {
            {{ filter_map_main }}
        })
        .collect();
        """)
        data["filter_map_main"] = build_filter_map_main(
            code_block, program_context, code_gen_context
        )
        code_gen_context.alias_variable[code_block.alias] = Variable(
            name=f"{code_block.alias}_s", type=code_block.type
        )
        return template.render(data)
    elif code_block.type == Type.map:
        template = Template("""
        let {{ alias }}_m: HashMap<i32, &str> =
        {{  zip_columns }}
        .filter_map(|{{ filter_map_closure|replace("'","")}}| {
            {{ filter_map_main }}
        })
        .collect();
        """)
        data["filter_map_main"] = build_filter_map_main(
            code_block, program_context, code_gen_context
        )
        code_gen_context.alias_variable[code_block.alias] = Variable(
            name=f"{code_block.alias}_m", type=code_block.type
        )
        return template.render(data)
    elif code_block.type == Type.map_vec:
        template = Template("""
        let {{ alias }}_m: HashMap<i32, Vec<&str>> =
        {{  zip_columns }}
        .filter_map(|{{ filter_map_closure|replace("'","")}}| {
            {{ filter_map_main }}
        })
        .fold(HashMap::default(), |mut acc, (k, v)| {
            acc.entry(k).or_default().push(v);
            acc
        });
        """)
        data["filter_map_main"] = build_filter_map_main(
            code_block, program_context, code_gen_context
        )
        code_gen_context.alias_variable[code_block.alias] = Variable(
            name=f"{code_block.alias}_m", type=code_block.type
        )
        return template.render(data)


def generate_main_block(semijoin_program: SemiJoinProgram,
                        sql_query_name,
                        output_file_path,
                        template_data: TemplateData):
    with open(output_file_path, "r") as f:
        query_data = json.load(f)

    meat_statements = []
    main_block = ""
    code_gen_context = CodeGenContext(alias_sj=dict(), alias_variable=dict(),
                                      template_data=template_data)
    orders, code_gen_context.alias_sj = semijoin_program.get_generation_order()
    program_context = ProgramContext(query_data, semijoin_program)
    print(f"orders: {orders}")
    print(f"alias_sj: {code_gen_context.alias_sj}")
    code_gen_context.template_data.data["result_output"] = format_result_output(program_context)
    code_gen_context.template_data.data["expected_result_set"] = get_expected_result_set(
        sql_query_name,
        program_context
    )
    code_gen_context.template_data.data["query_name"] = "q" + sql_query_name
    for idx, alias in enumerate(orders):
        item = query_data[alias]
        print(item["filters"])
        code_block = build_code_block(alias, item, program_context)
        meat_statements.append(
            generate_code_block(code_block, program_context, code_gen_context)
        )

    # todo: we can do an optimization pass (such as merge cct passes) on meat_statements
    main_block += "\n".join(list(code_gen_context.finders))
    main_block += "let start = Instant::now();"
    main_block += "\n".join(meat_statements)
    template_data.data["main_block"] = main_block


def optimization(sql_query_name, output_file_path) -> None:
    """
    Generate query implementation based on base.jinja
    """
    env = Environment(loader=FileSystemLoader("templates"))
    template_data = TemplateData(template=env.get_template("base.jinja"),
                                 data=dict())

    semijoin_program = decide_join_tree(output_file_path)
    generate_main_block(semijoin_program, sql_query_name, output_file_path, template_data)
    template_data.data["initialize_relation_block"] = _initialize_relation_block(output_file_path, [])


    query_implementation = template_data.template.render(template_data.data)
    output_dir = pathlib.Path(__file__).parent.parent / "src"
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
