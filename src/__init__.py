"""
to-rdls: Modular RDLS metadata transformation toolkit.

Supports two pipeline generations:
  v1.0 (canonical): LLM-first single-phase pipeline for HDX -> RDLS v1.0 records.
  v0.3 (legacy):    Regex + LLM hybrid pipeline for HDX/GeoNode -> RDLS v0.3 records.

This is NOT a Python package. It's a set of scripts and configs
designed to be copied alongside your project.

Usage from notebooks or scripts:
    import sys
    sys.path.insert(0, "path/to/to-rdls")

    # v1.0 pipeline (canonical)
    from src.llm_classify import classify_v10, V10Config
    from src.translate import build_rdls_record_v10
    from src.extract import integrate_hevl_v10

    # v0.3 pipeline (legacy)
    from src.utils import sanitize_text, load_json, write_json
    from src.spatial import country_name_to_iso3, infer_spatial
    from src.schema import validate_record, load_codelists
    from src.classify import classify_dataset, Classification
    from src.translate_v03 import build_rdls_record, map_data_format
    from src.extract_hazard import HazardExtractor
    from src.extract_exposure import ExposureExtractor
    from src.extract_vulnloss import VulnerabilityExtractor, LossExtractor
    from src.integrate import integrate_record
    from src.validate_qa import validate_and_score, distribute_records
    from src.sources.hdx import HDXClient, extract_hdx_fields
    from src.inventory import inventory_folder, InventoryConfig
"""

# Convenience imports for common functions
from .utils import (
    sanitize_text,
    slugify,
    slugify_token,
    norm_str,
    load_json,
    write_json,
    append_jsonl,
    load_yaml,
    iter_json_files,
    as_list,
    split_semicolon_list,
)

from .spatial import (
    country_name_to_iso3,
    infer_spatial,
    load_spatial_config,
)

from .schema import (
    load_rdls_schema,
    load_codelists,
    validate_record,
)

from .classify import (
    classify_dataset,
    Classification,
    load_classification_config,
    load_exclusion_patterns,
)

from .translate_v03 import (
    build_rdls_record,
    map_data_format,
    map_license,
    load_format_config,
    load_license_config,
)

from .extract_hazard import HazardExtractor, HazardExtraction, build_hazard_block
from .extract_exposure import ExposureExtractor, ExposureExtraction, build_exposure_block
from .extract_vulnloss import (
    VulnerabilityExtractor,
    LossExtractor,
    VulnerabilityExtraction,
    LossExtraction,
    build_vulnerability_block,
    build_loss_block,
)
from .naming import (
    build_rdls_id,
    build_rdls_id_with_collision,
    encode_component_types,
    encode_countries,
    encode_items,
    is_valid_iso3,
    load_naming_config,
    parse_rdls_id,
    resolve_shortname,
    slugify_title,
)
from .integrate import integrate_record, merge_hevl_into_record, append_provenance
from .validate_qa import (
    compute_composite_confidence,
    validate_and_score,
    distribute_records,
    create_validation_report,
)
from .inventory import (
    InventoryConfig,
    inventory_folder,
    scan_target,
    render_and_write,
)
from .review import (
    review_folder,
    ReviewResult,
    FileGroup,
    FileInspection,
    GapAnalysis,
)
from .zipaccess import (
    open_zip_member,
    parse_zip_spec,
    resolve_and_open,
)
