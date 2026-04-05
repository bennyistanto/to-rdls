# Suggest Patterns

Feedback loop: analyze classification results from a data review session and generate YAML pattern suggestions for `configs/review_knowledge.yaml` to improve the automated classifier.

## Input
$ARGUMENTS - path to a reviewed data folder (with `_rdls_review/` subfolder), OR description of misclassified columns

## Instructions

### Step 1: Gather evidence

If a folder path is provided:
1. Read the review output from `{path}/_rdls_review/` if it exists
2. Run `inspect_folder_for_llm(path="$ARGUMENTS")` to get raw column data
3. Run `review_folder(path="$ARGUMENTS")` to see current classification + conflicts

If a description of misclassified columns is provided:
1. Parse the column names and expected vs actual classification
2. Proceed directly to Step 2

### Step 2: Identify classification gaps

For each file group, check:
1. **Unmatched columns**: columns that exist in the data but don't match any pattern in `column_detection`
2. **False positives**: patterns that matched but gave wrong HEVL component
3. **Conflict cases**: where filename signals and column signals disagree

Read the current column_detection config:
```
Read configs/review_knowledge.yaml - Section 7 (column_detection)
```

### Step 3: Generate pattern suggestions

For each gap found, create a YAML snippet:

```yaml
# NEW GROUP: {descriptive_name}
# Source: {which dataset revealed this gap}
# Columns that triggered this: {list}
group_name:
  component: L   # H, E, V, or L
  weight: high   # high (3), medium (2), or low (1)
  label: "Human-readable label"
  patterns:
    - "regex_pattern"   # matches: column_name_1, column_name_2
    - "another_pattern"  # matches: column_name_3
```

**Pattern writing rules**:
- Use `re.IGNORECASE` compatible regex (patterns are compiled with `re.I`)
- Prefer word boundaries or partial matches over exact strings
- Use `.*` for flexible word order: `damage.*status` matches `damage_status` and `damage_assessment_status`
- Group related columns that indicate the same HEVL component
- Avoid overly broad patterns that would cause false positives
- Test mentally: would this pattern match something it shouldn't?

### Step 4: Check for conflicts with existing patterns

For each suggested pattern:
1. Would it overlap with an existing group?
2. Would it cause false positives on known datasets (Tomorrow Cities, Sierra Leone, Vietnam, Liberia)?
3. Is the weight appropriate? (high = strong indicator, medium = supporting, low = weak)

### Step 5: Present the diff

Show the complete suggested additions as a YAML block that can be appended to `configs/review_knowledge.yaml` Section 7:

```yaml
# =============================================
# Suggested additions to column_detection
# Generated from: {dataset_name} review
# Date: {date}
# =============================================

  # --- New groups ---
  {group_yaml_here}
```

Also show any modifications to existing groups (e.g., adding patterns to an existing group):

```yaml
# MODIFY existing group: {group_name}
# Add these patterns:
  - "new_pattern_1"   # matches: {examples}
  - "new_pattern_2"   # matches: {examples}
```

### Output format

```markdown
# Pattern Suggestions: {source}

## Summary
- Unmatched columns found: N
- New groups suggested: N
- Existing groups modified: N
- False positives identified: N

## New Pattern Groups
{YAML blocks with comments}

## Modified Existing Groups
{YAML diff blocks}

## Conflict Check
- {pattern} vs {existing_group}: {OK or conflict description}

## Test Cases
For each new pattern, list 3-5 example column names it would match:
- `{pattern}` matches: `col1`, `col2`, `col3`

## Instructions to Apply
1. Open `configs/review_knowledge.yaml`
2. Navigate to Section 7 (`column_detection`)
3. Add new groups at the end of the section
4. For modified groups, add new patterns to the `patterns:` list
5. Run `tests/test_review_basic.py` to verify no regressions
```
