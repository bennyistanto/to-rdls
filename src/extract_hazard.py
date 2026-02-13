"""
Hazard block extractor for RDLS metadata.

Extracts hazard types, process types, analysis types, return periods,
intensity measures, and calculation methods from metadata text fields
using pattern matching against the Signal Dictionary.

Uses a 2-tier cascade:
  Tier 1 (title, name, tags, resources): can INTRODUCE hazard types
  Tier 2 (notes, methodology): can only CORROBORATE Tier 1 findings,
    or serve as fallback if Tier 1 found nothing at all.

Source-independent — works on any metadata with standard text fields.
"""

import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .utils import load_yaml, normalize_text


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExtractionMatch:
    """A single pattern match result."""
    value: str              # RDLS codelist value
    confidence: float       # 0.0–1.0
    source_field: str       # Field that matched (title, tags, notes, etc.)
    matched_text: str       # The text that matched
    pattern: str            # Pattern that matched


@dataclass
class HazardExtraction:
    """Complete hazard extraction result for one dataset."""
    hazard_types: List[ExtractionMatch]
    process_types: List[ExtractionMatch]
    analysis_type: Optional[ExtractionMatch] = None
    return_periods: List[int] = field(default_factory=list)
    intensity_measures: List[str] = field(default_factory=list)
    overall_confidence: float = 0.0
    calculation_method: Optional[str] = None
    description: Optional[str] = None

    @property
    def has_hazard(self) -> bool:
        return len(self.hazard_types) > 0


# ---------------------------------------------------------------------------
# Hazard Extractor
# ---------------------------------------------------------------------------

class HazardExtractor:
    """Extracts RDLS Hazard block from metadata using pattern matching.

    Uses a 2-tier cascade for hazard detection to prevent false positives:
      Tier 1 (title, name, tags, resources): authoritative, can introduce types
      Tier 2 (notes, methodology): can only corroborate or fallback with FP filter
    """

    CONFIDENCE_MAP = {"high": 0.9, "medium": 0.7, "low": 0.5}
    TIER1_FIELDS = {"title", "name", "tags", "resources"}
    TIER2_FIELDS = {"notes", "methodology"}

    # False-positive context patterns for Tier 2 fallback
    TIER2_FALSE_POSITIVE_PATTERNS = [
        re.compile(r"earthquake\s+risk\s+reduction", re.IGNORECASE),
        re.compile(r"flood\s+(?:risk|resilience)\s+(?:management|reduction|program)", re.IGNORECASE),
        re.compile(r"same\s+dataset\s+as\s+(?:river\s+)?(?:flood|earthquake|drought|cyclone)", re.IGNORECASE),
    ]

    # Return period patterns
    RP_PATTERNS = [
        re.compile(r"return\s+period\s+(?:of\s+)?(\d+)\s*(?:year|yr)?", re.IGNORECASE),
        re.compile(r"(\d+)[\s-]*year\s+return\s+period", re.IGNORECASE),
        re.compile(r"\brp[\s-]*(\d+)\b", re.IGNORECASE),
        re.compile(r"1[\s-]*in[\s-]*(\d+)[\s-]*year", re.IGNORECASE),
        re.compile(r"recurrence\s+interval\s+(?:of\s+)?(\d+)", re.IGNORECASE),
        # Lists: "50, 100, 250, 500 and 1000 years return period"
        re.compile(r"((?:\d+[\s,]+(?:and\s+)?)+\d+)\s*(?:year|yr)s?\s*return\s+period", re.IGNORECASE),
    ]

    # Intensity measure text patterns
    IM_TEXT_PATTERNS = {
        "PGA:g": [r"\bpga\b", r"\bpeak\s+ground\s+acceleration\b"],
        "PGV:m/s": [r"\bpgv\b", r"\bpeak\s+ground\s+velocity\b"],
        "MMI:-": [r"\bmmi\b", r"\bmodified\s+mercalli\b"],
        "wd:m": [r"\bwater\s+depth\b", r"\binundation\s+depth\b", r"\bflood\s+depth\b"],
        "Rh_tsi:m": [
            r"\brun[\s-]?up\b.*\btsunami\b|\btsunami\b.*\brun[\s-]?up\b",
            r"\btsunami\s+height\b", r"\btsunami\s+wave\s+height\b",
        ],
        "sws_10m:m/s": [
            r"\bwind\s+speed\b", r"\bsustained\s+wind\b",
            r"\bcyclone\s+wind\b", r"\bwind\s+gust\b",
        ],
        "SPI:-": [r"\bspi\b", r"\bstandard(?:ized)?\s+precipitation\s+index\b"],
        "SPEI:-": [r"\bspei\b"],
        "FWI:-": [r"\bfire\s+weather\s+index\b", r"\bfwi\b"],
        "AirTemp:C": [r"\bheat\s+wave\b", r"\bcold\s+wave\b", r"\bthermal\s+stress\b"],
    }

    # Calculation method patterns
    SIMULATED_PATTERNS = [
        r"\bmodel(?:ed|ling|led)?\b", r"\bsimulat", r"\bscenario\b",
        r"\bprobabilistic\b", r"\bstochastic\b",
        r"\bgar[\s_-]?(?:15|2015)\b",
        r"\bhazard\s+model\b", r"\bflood\s+model\b", r"\bglobal\s+model\b",
    ]
    OBSERVED_PATTERNS = [
        r"\bobserved\b", r"\bhistorical\b", r"\brecorded\b",
        r"\bsatellite\b.*\bassessment\b", r"\bfield\s+survey\b",
        r"\bpost[\s-](?:disaster|event|earthquake|flood|cyclone)\b",
        r"\bdamage\s+assessment\b", r"\bevent\s+(?:of|from|in)\s+\d{4}\b",
        r"\bimpact\s+assessment\b", r"\brapid\s+assessment\b",
    ]
    INFERRED_PATTERNS = [
        r"\binferred\b", r"\bderived\b", r"\bstatistical\b",
        r"\bsusceptibility\b",
        r"\bindex\b.*\bhazard\b",
        r"\bhazard\s+classification\b", r"\brisk\s+score\b",
    ]

    def __init__(self, signal_dict: Dict[str, Any],
                 defaults: Optional[Dict[str, Any]] = None):
        """Initialize with signal dictionary and optional defaults config.

        Args:
            signal_dict: Loaded signal_dictionary.yaml.
            defaults: Loaded rdls_defaults.yaml (for hazard_process_defaults etc).
        """
        self.signal_dict = signal_dict
        self.defaults = defaults or {}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compile regex patterns from signal dictionary."""
        self.hazard_patterns = {}
        self.process_patterns = {}
        self.analysis_patterns = {}

        for hazard_type, cfg in self.signal_dict.get("hazard_type", {}).items():
            compiled = []
            for p in cfg.get("patterns", []):
                try:
                    compiled.append(re.compile(p, re.IGNORECASE))
                except re.error:
                    pass
            self.hazard_patterns[hazard_type] = {
                "compiled": compiled,
                "confidence": self.CONFIDENCE_MAP.get(cfg.get("confidence", "medium"), 0.7),
            }

        for process_type, cfg in self.signal_dict.get("process_type", {}).items():
            compiled = []
            for p in cfg.get("patterns", []):
                try:
                    compiled.append(re.compile(p, re.IGNORECASE))
                except re.error:
                    pass
            self.process_patterns[process_type] = {
                "compiled": compiled,
                "confidence": self.CONFIDENCE_MAP.get(cfg.get("confidence", "medium"), 0.7),
                "parent_hazard": cfg.get("parent_hazard"),
            }

        for analysis_type, cfg in self.signal_dict.get("analysis_type", {}).items():
            compiled = []
            for p in cfg.get("patterns", []):
                try:
                    compiled.append(re.compile(p, re.IGNORECASE))
                except re.error:
                    pass
            self.analysis_patterns[analysis_type] = {
                "compiled": compiled,
                "confidence": self.CONFIDENCE_MAP.get(cfg.get("confidence", "medium"), 0.7),
            }

    def _extract_text_fields(self, record: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Extract and normalize text fields for pattern matching.

        Returns:
            Tuple of (text_fields, secondary_needs_corroboration).
            secondary_needs_corroboration maps secondary hazard type
            to the source compound tag that introduced it.
        """
        title = normalize_text(record.get("title", ""))
        name = normalize_text(record.get("name", ""))
        notes = normalize_text(record.get("notes", ""))
        methodology = normalize_text(record.get("methodology", ""))

        # Parse tags with compound tag awareness
        raw_tags = record.get("tags", [])
        compound_tags = self.defaults.get("compound_tags", {})
        parsed_tags = []
        secondary_needs_corroboration = {}

        if isinstance(raw_tags, list):
            for tag in raw_tags:
                tag_str = tag.get("name", "") if isinstance(tag, dict) else str(tag)
                tag_lower = tag_str.lower().strip()

                if tag_lower in compound_tags:
                    info = compound_tags[tag_lower]
                    rule = info.get("rule", "single")
                    if rule == "ignore":
                        continue
                    primary = info.get("primary")
                    secondary = info.get("secondary")
                    if primary:
                        parsed_tags.append(primary)
                    if secondary and rule == "corroborate_secondary":
                        secondary_needs_corroboration[secondary] = tag_lower
                else:
                    parsed_tags.append(tag_str)
            tag_str = " ".join(parsed_tags).lower()
        else:
            tag_str = str(raw_tags).lower()

        # Resources as single string
        resources = record.get("resources", [])
        res_parts = []
        if isinstance(resources, list):
            for r in resources:
                if isinstance(r, dict):
                    res_parts.append(r.get("name", ""))
                    res_parts.append(r.get("description", ""))
        res_str = normalize_text(" ".join(res_parts))

        text_fields = {
            "title": title,
            "name": name,
            "tags": tag_str,
            "resources": res_str,
            "notes": notes,
            "methodology": methodology,
        }
        return text_fields, secondary_needs_corroboration

    def _match_hazard_types(self, text_fields: Dict[str, str]) -> Tuple[List[ExtractionMatch], List[ExtractionMatch]]:
        """Match hazard types using 2-tier cascade.

        Returns (tier1_matches, tier2_matches).
        """
        tier1_matches = []
        tier2_matches = []

        for hazard_type, cfg in self.hazard_patterns.items():
            for pat in cfg["compiled"]:
                for field_name, text in text_fields.items():
                    if not text:
                        continue
                    m = pat.search(text)
                    if m:
                        match = ExtractionMatch(
                            value=hazard_type,
                            confidence=cfg["confidence"],
                            source_field=field_name,
                            matched_text=m.group(0),
                            pattern=pat.pattern,
                        )
                        if field_name in self.TIER1_FIELDS:
                            tier1_matches.append(match)
                        elif field_name in self.TIER2_FIELDS:
                            tier2_matches.append(match)

        return tier1_matches, tier2_matches

    def _has_false_positive_context(self, text_fields: Dict[str, str],
                                     match: ExtractionMatch) -> bool:
        """Check if a Tier 2 match is a false positive."""
        field_text = text_fields.get(match.source_field, "")
        for fp_pat in self.TIER2_FALSE_POSITIVE_PATTERNS:
            if fp_pat.search(field_text):
                return True
        return False

    def _match_process_types(self, text: str) -> List[ExtractionMatch]:
        """Match process types from combined text."""
        matches = []
        for process_type, cfg in self.process_patterns.items():
            for pat in cfg["compiled"]:
                m = pat.search(text)
                if m:
                    matches.append(ExtractionMatch(
                        value=process_type,
                        confidence=cfg["confidence"],
                        source_field="combined",
                        matched_text=m.group(0),
                        pattern=pat.pattern,
                    ))
                    break
        return matches

    def _match_analysis_type(self, text: str) -> Optional[ExtractionMatch]:
        """Match analysis type from combined text."""
        best = None
        for analysis_type, cfg in self.analysis_patterns.items():
            for pat in cfg["compiled"]:
                m = pat.search(text)
                if m:
                    match = ExtractionMatch(
                        value=analysis_type,
                        confidence=cfg["confidence"],
                        source_field="combined",
                        matched_text=m.group(0),
                        pattern=pat.pattern,
                    )
                    if best is None or match.confidence > best.confidence:
                        best = match
                    break
        return best

    def _extract_return_periods(self, text: str) -> List[int]:
        """Extract numeric return period values.

        Filters out values 2000-2099 to avoid treating calendar years
        as return periods.
        """
        values = set()
        for pat in self.RP_PATTERNS:
            for m in pat.finditer(text):
                for g in m.groups():
                    if g and g.isdigit():
                        val = int(g)
                        if 1 <= val <= 100000 and not (2000 <= val <= 2099):
                            values.add(val)
                    elif g and not g.isdigit():
                        # Handle comma-separated lists like "50, 100, 250"
                        for num_str in re.findall(r"\d+", g):
                            val = int(num_str)
                            if 1 <= val <= 100000 and not (2000 <= val <= 2099):
                                values.add(val)
        return sorted(values)

    def _extract_intensity_measures(self, text: str, hazard_types: List[str]) -> List[str]:
        """Extract intensity measures from text or use defaults."""
        measures = []
        for im, patterns in self.IM_TEXT_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    measures.append(im)
                    break

        # If none found, use defaults from config
        if not measures and hazard_types:
            defaults = self.defaults.get("default_intensity_measures", {})
            for ht in hazard_types:
                if ht in defaults:
                    measures.append(defaults[ht])

        return list(dict.fromkeys(measures))  # deduplicate preserving order

    def _infer_calculation_method(self, text: str,
                                   analysis_type: Optional[str]) -> str:
        """Infer data_calculation_type using pattern scoring.

        Counts matches across all three categories, uses analysis_type
        as a 0.5 tiebreaker, and picks the category with the highest score.
        Returns 'inferred' as the default when nothing matches.
        """
        scores = {"simulated": 0.0, "observed": 0.0, "inferred": 0.0}
        for pat in self.SIMULATED_PATTERNS:
            if re.search(pat, text, re.IGNORECASE):
                scores["simulated"] += 1
        for pat in self.OBSERVED_PATTERNS:
            if re.search(pat, text, re.IGNORECASE):
                scores["observed"] += 1
        for pat in self.INFERRED_PATTERNS:
            if re.search(pat, text, re.IGNORECASE):
                scores["inferred"] += 1

        # Analysis type as tiebreaker
        if analysis_type:
            at = analysis_type.lower()
            if at == "probabilistic":
                scores["simulated"] += 0.5
            elif at == "empirical":
                scores["observed"] += 0.5
            elif at == "deterministic":
                scores["inferred"] += 0.5

        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else "inferred"

    def extract(self, record: Dict[str, Any]) -> HazardExtraction:
        """Run full hazard extraction on a metadata record.

        Args:
            record: Metadata dict with title, name, tags, notes, etc.

        Returns:
            HazardExtraction result.
        """
        text_fields, secondary_corroboration = self._extract_text_fields(record)
        combined_text = " ".join(text_fields.values())

        # 2-tier cascade for hazard types
        tier1_matches, tier2_matches = self._match_hazard_types(text_fields)

        # Determine final hazard types
        tier1_types = set(m.value for m in tier1_matches)
        if tier1_types:
            # Tier 2 can only corroborate Tier 1
            final_matches = list(tier1_matches)
            for m in tier2_matches:
                if m.value in tier1_types:
                    final_matches.append(m)
        else:
            # Fallback to Tier 2 with false-positive filtering
            final_matches = [
                m for m in tier2_matches
                if not self._has_false_positive_context(text_fields, m)
            ]

        # Deduplicate by value
        seen_types = set()
        hazard_types = []
        for m in final_matches:
            if m.value not in seen_types:
                hazard_types.append(m)
                seen_types.add(m.value)

        # Compound tag corroboration: secondary hazards from compound tags
        # need independent text evidence in non-tag fields
        if secondary_corroboration:
            non_tag_text = " ".join([
                text_fields.get("title", ""),
                text_fields.get("name", ""),
                text_fields.get("notes", ""),
                text_fields.get("resources", ""),
                text_fields.get("methodology", ""),
            ]).lower()
            for sec_hazard, source_tag in secondary_corroboration.items():
                if sec_hazard in self.hazard_patterns:
                    has_evidence = False
                    for compiled_pat in self.hazard_patterns[sec_hazard]["compiled"]:
                        if compiled_pat.search(non_tag_text):
                            has_evidence = True
                            break
                    if not has_evidence:
                        # Remove unsubstantiated secondary from results
                        hazard_types = [ht for ht in hazard_types if ht.value != sec_hazard]

        # Process types
        process_types = self._match_process_types(combined_text)

        # Fill default process types for hazards without specific process
        hazard_type_values = [m.value for m in hazard_types]
        process_type_values = set(m.value for m in process_types)
        hp_defaults = self.defaults.get("hazard_process_defaults", {})
        for ht in hazard_type_values:
            default_pt = hp_defaults.get(ht)
            if default_pt and default_pt not in process_type_values:
                process_types.append(ExtractionMatch(
                    value=default_pt,
                    confidence=0.5,
                    source_field="default",
                    matched_text="",
                    pattern="hazard_process_default",
                ))

        # Analysis type
        analysis_type = self._match_analysis_type(combined_text)

        # Return periods
        return_periods = self._extract_return_periods(combined_text)

        # Intensity measures
        intensity_measures = self._extract_intensity_measures(
            combined_text, hazard_type_values
        )

        # Calculation method
        calc_method = self._infer_calculation_method(
            combined_text,
            analysis_type.value if analysis_type else None,
        )

        # Overall confidence
        if hazard_types:
            overall_conf = max(m.confidence for m in hazard_types)
        else:
            overall_conf = 0.0

        return HazardExtraction(
            hazard_types=hazard_types,
            process_types=process_types,
            analysis_type=analysis_type,
            return_periods=return_periods,
            intensity_measures=intensity_measures,
            overall_confidence=overall_conf,
            calculation_method=calc_method,
        )


# ---------------------------------------------------------------------------
# Build RDLS hazard block
# ---------------------------------------------------------------------------

def build_hazard_block(extraction: HazardExtraction) -> Optional[Dict[str, Any]]:
    """Convert HazardExtraction to RDLS hazard JSON block.

    Returns None if no hazard detected.
    """
    if not extraction.has_hazard:
        return None

    # Build hazards list
    hazards = []
    for ht in extraction.hazard_types:
        hazard = {"hazard_type": ht.value}
        # Find matching process types
        related_processes = [
            pt.value for pt in extraction.process_types
        ]
        if related_processes:
            hazard["process_type"] = related_processes[0]
        hazards.append(hazard)

    # Build event
    event = {"id": f"event_{uuid.uuid4().hex[:8]}"}
    if extraction.analysis_type:
        event["analysis_type"] = extraction.analysis_type.value
    if extraction.calculation_method:
        event["calculation_method"] = extraction.calculation_method
    if extraction.return_periods:
        event["occurrence"] = {
            "probabilistic": {
                "return_period": extraction.return_periods[0]
            }
        }

    # Build event set
    event_set = {
        "id": f"event_set_{uuid.uuid4().hex[:8]}",
        "hazards": hazards,
        "events": [event],
    }
    if extraction.analysis_type:
        event_set["analysis_type"] = extraction.analysis_type.value

    return {
        "event_sets": [event_set],
    }
