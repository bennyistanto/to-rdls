# RDLS Add Pattern

Add new extraction patterns to the signal dictionary for HEVL detection.

## Input
$ARGUMENTS - description of what to detect (e.g., "detect heat stress datasets", "add patterns for cyclone datasets")

## Instructions

1. Read the current signal dictionary: `to-rdls/configs/signal_dictionary.yaml`
2. Read the RDLS schema codelists: `to-rdls/configs/rdls_schema.yaml` to verify the target codelist value exists
3. Read the defaults: `to-rdls/configs/rdls_defaults.yaml` to understand constraint relationships

4. Determine where the new pattern belongs:
   - `hazard_type` section → if detecting a hazard category
   - `process_type` section → if detecting a specific process (must specify parent_hazard)
   - `exposure_category` section → if detecting exposure
   - `vulnerability_indicators` / `loss_indicators` → if detecting V or L signals

5. Write the regex pattern following existing conventions:
   - Use `\b` word boundaries
   - Use `(?:...)` for non-capturing groups
   - Use `.?` for optional separators (e.g., `storm.?surge`)
   - Assign appropriate confidence: high (0.9) for unambiguous terms, medium (0.7) for common but sometimes ambiguous, low (0.5) for weak signals

6. Check for false positive risks:
   - Would this pattern match non-risk contexts? (e.g., "flood of information")
   - If so, add to `exclusion_patterns` section
   - Or add to `TIER2_FALSE_POSITIVE_PATTERNS` in the relevant extract module

7. Add the pattern to signal_dictionary.yaml under the correct section
8. Explain the pattern and its confidence level
