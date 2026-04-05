# RDLS Classify

Classify a dataset into RDLS risk data types (Hazard, Exposure, Vulnerability, Loss).

## Input
$ARGUMENTS - a dataset title and description, OR a path to a metadata JSON file

## Instructions

1. Analyze the text for HEVL signals using these indicators:

**Hazard signals**: flood, earthquake, cyclone, drought, landslide, tsunami, wildfire, storm surge, wind speed, return period, hazard map, risk map, inundation, seismic, volcanic

**Exposure signals**: population, buildings, infrastructure, roads, schools, hospitals, cropland, land use, land cover, census, assets, inventory, footprints, OpenStreetMap

**Vulnerability signals**: fragility, damage function, vulnerability curve, damage ratio, loss ratio, susceptibility, structural type, building type, damage state

**Loss signals**: economic loss, damage cost, casualties, fatalities, affected population, AAL (average annual loss), PML (probable maximum loss), impact assessment

2. Apply classification rules:
   - A dataset can have MULTIPLE risk_data_types
   - Hazard + Exposure together is common (e.g., "flood risk map with building exposure")
   - Check for false positives: "earthquake preparedness training" is NOT a hazard dataset

3. For each detected type, assess confidence (high/medium/low) and cite the specific text that triggered the classification

4. Output:
   - Primary `risk_data_type` (most confident)
   - Secondary types if applicable
   - Confidence level and evidence for each
   - Suggested `hazard_type` / `exposure_category` if detectable
