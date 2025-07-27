import re
import json
import os

# Correctly get the list of files from the directory
files = [f for f in os.listdir('../src') if f.startswith('o') and f.endswith('.rs')]
files = [os.path.join('../src', f) for f in files]

results = {}
# This regex is a bit complex. Let's break it down:
# assert_eq!\(
#   res,
#   \s*                         // optional whitespace
#   (                           // start capture group 1
#     Some\(                    // literal "Some("
#       \(                        // literal "("
#         ".*?"                 // a double quoted string, non-greedy
#         (?:, ".*?")*          // 0 or more comma-space-quoted strings
#       \)                        // literal ")"
#     \)                        // literal ")"
#     |                         // OR
#     None                      // literal "None"
#   )                           // end capture group 1
#   \s*                         // optional whitespace
# \)
# The Some(...) part is to capture tuples like ("a", "b", "c")
# The None part is for queries that have no result.
regex = re.compile(r"assert_eq!\(res,\s*(Some\(\((.*?)\)\)|None)\s*\);", re.DOTALL)

for file_path in files:
    with open(file_path, 'r') as f:
        content = f.read()
        match = regex.search(content)
        if match:
            query_name = os.path.basename(file_path)[1:-3]
            result_str = match.group(1)
            if result_str == "None":
                results[query_name] = None
            else:
                # Extract the tuple content
                tuple_content = match.group(2)
                # Split by comma and strip quotes and whitespace
                if tuple_content:
                    extracted_results = [s.strip().strip('"') for s in tuple_content.split(',')]
                    results[query_name] = extracted_results
                else: # Handle empty Some(()) case
                    results[query_name] = []


with open('expected_results.json', 'w') as f:
    json.dump(results, f, indent=4, sort_keys=True)

print("Done.")
