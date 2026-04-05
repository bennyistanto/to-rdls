# RDLS Inspect Config

Inspect and explain an RDLS config file's structure and relationships.

## Input
$ARGUMENTS - config filename or topic (e.g., "signal_dictionary", "naming", "valid triplets", "format mapping")

## Instructions

1. Locate and read the relevant config file from `to-rdls/configs/`
2. Explain:
   - What the config controls
   - Which Python modules load it (see the module→config dependency table below)
   - The structure of each section
   - How values relate to RDLS codelists

3. Module → config dependencies:
   - `classify.py` → classification.yaml, signal_dictionary.yaml
   - `extract_hazard.py` → signal_dictionary.yaml, rdls_defaults.yaml
   - `extract_exposure.py` → signal_dictionary.yaml, rdls_defaults.yaml
   - `extract_vulnloss.py` → rdls_defaults.yaml
   - `translate.py` → format_mapping.yaml, license_mapping.yaml, spatial.yaml
   - `naming.py` → naming.yaml
   - `schema.py` → rdls_schema.yaml, rdls_schema_v0.3.json
   - `validate_qa.py` → rdls_schema.yaml, rdls_defaults.yaml
   - `spatial.py` → spatial.yaml
   - `sources/hdx.py` → sources/hdx.yaml
   - `integrate.py` → naming.yaml (via naming module)

4. If the user asks about a topic (e.g., "valid triplets"), find the relevant section across configs and explain the constraints.

5. Show example entries and explain the format.
