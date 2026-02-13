"""
Exposure block extractor for RDLS metadata.

Extracts exposure categories, metric dimensions, quantity kinds,
taxonomy hints, and currency from metadata text fields using
pattern matching against the Signal Dictionary.

Uses a 3-tier cascade:
  Tier 1 (title, name, tags): highest confidence, can introduce categories
  Tier 2 (resources): medium confidence, can introduce new categories
  Tier 3 (notes, methodology): lowest confidence, fallback only

Source-independent.
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
    value: str
    confidence: float
    source_field: str
    matched_text: str
    pattern: str


@dataclass
class MetricExtraction:
    """Extracted metric for an exposure category."""
    dimension: str
    quantity_kind: str
    confidence: float
    source_hint: str = ""


@dataclass
class ExposureExtraction:
    """Complete exposure extraction result for one dataset."""
    categories: List[ExtractionMatch]
    metrics: Dict[str, List[MetricExtraction]] = field(default_factory=dict)
    taxonomy_hint: Optional[str] = None
    currency: Optional[str] = None
    overall_confidence: float = 0.0

    @property
    def has_exposure(self) -> bool:
        return len(self.categories) > 0


# ---------------------------------------------------------------------------
# Pattern dictionaries for dimension and quantity_kind detection.
# Used to scan text for hints about what metrics a dataset contains.
# Multiple dimensions CAN match for a single category (e.g., an
# infrastructure dataset may have both structure+length and disruption+time).
# ---------------------------------------------------------------------------

DIMENSION_PATTERNS = {
    "structure": [
        r"\b(building|structure|footprint|floor.?area)\b",
        r"\b(construction|built|asset|facility)\b",
        r"\b(road|bridge|railway|rail.?line|highway)\b",
        r"\b(airport|port|harbor|terminal|pipeline)\b",
        r"\b(power.?line|electricity.?grid|water.?supply)\b",
        r"\b(hospital|school|health.?center|clinic)\b",
    ],
    "content": [
        r"\b(content|inventory|equipment|furnishing)\b",
        r"\b(stock|goods|material|supply)\b",
        r"\b(land.?cover|land.?use|vegetation)\b",
        r"\b(ecosystem|habitat|species)\b",
    ],
    "product": [
        r"\b(crop|harvest|yield|production)\b",
        r"\b(output|commodity|livestock|cattle)\b",
        r"\b(food.?production|agricultural.?output)\b",
    ],
    "disruption": [
        r"\b(disruption|downtime|outage|interruption)\b",
        r"\b(delay|loss.?of.?function|service.?disruption)\b",
        r"\b(business.?interruption|closur)\b",
    ],
    "population": [
        r"\b(population[\s._-]?(?:count|density|data|distribution|grid|layer|estimate))\b",
        r"\b((?:census|demographic)[\s._-]?(?:data|survey|layer))\b",
        r"\b(household[\s._-]?(?:survey|count|data|size))\b",
        r"\b((?:displaced|refugee|idp)[\s._-]?(?:population|count|data|number))\b",
        r"\b(population[\s._-]?(?:exposure|at[\s._-]?risk|affected|vulnerable))\b",
    ],
    "index": [
        r"\b(index|indicator|score|ranking)\b",
        r"\b(hdi|svi|inform.?risk|poverty.?index)\b",
        r"\b(vulnerability.?index|resilience.?index)\b",
        r"\b(development.?index|risk.?index)\b",
        r"\b(gdp|gni|economic.?indicator)\b",
    ],
}

QUANTITY_KIND_PATTERNS = {
    "count": [
        r"\b(count[\s._-]?(?:of|data|per))\b",
        r"\b(number[\s._-]?of[\s._-]?(?:building|structure|house|people|person|household))\b",
        r"\b(total[\s._-]?(?:count|number|population|building|structure))\b",
    ],
    "area": [
        r"\b(area|hectare|acre|sq\.?\s*(?:m|km|ft))\b",
        r"\b(square|coverage|extent|footprint)\b",
    ],
    "length": [
        r"\b(length|distance|km|kilometer|mile)\b",
        r"\b(route|corridor|line|network)\b",
    ],
    "monetary": [
        r"\b(value|cost|price|worth|\$|usd|eur)\b",
        r"\b(economic|financial|monetary|budget)\b",
        r"\b(replacement|rebuild|damage.?cost)\b",
        r"\b(gdp|gni|income|expenditure)\b",
    ],
    "time": [
        r"\b(duration|time|hours|days|weeks)\b",
        r"\b(downtime|turnaround|recovery.?time)\b",
    ],
}

CURRENCY_PATTERNS = [
    (r"\b(USD|US\s*\$|United\s*States\s*Dollar)\b", "USD"),
    (r"\b(EUR|Euro)\b", "EUR"),
    (r"\b(GBP|British\s*Pound)\b", "GBP"),
    (r"\b(JPY|Japanese\s*Yen)\b", "JPY"),
    (r"\b(CHF|Swiss\s*Franc)\b", "CHF"),
    (r"\b(AUD|Australian\s*Dollar)\b", "AUD"),
    (r"\b(CAD|Canadian\s*Dollar)\b", "CAD"),
    (r"\b(CNY|RMB|Chinese\s*Yuan)\b", "CNY"),
    (r"\b(INR|Indian\s*Rupee)\b", "INR"),
    (r"\b(BRL|Brazilian\s*Real)\b", "BRL"),
]

# Set of common 3-letter ISO 4217 currency codes for fallback detection
COMMON_CURRENCIES = {
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY", "INR", "BRL",
    "ZAR", "MXN", "SGD", "HKD", "NOK", "SEK", "DKK", "NZD", "THB", "IDR",
    "PHP", "MYR", "KRW", "TRY", "RUB", "PLN", "CZK", "HUF", "CLP", "COP",
    "PEN", "ARS", "EGP", "NGN", "KES", "GHS", "TZS", "UGX", "ETB", "BDT",
    "PKR", "LKR", "MMK", "VND", "KHR", "LAK", "NPR", "AFN", "IQD", "SYP",
}

TAXONOMY_PATTERNS = {
    "GED4ALL":  [r"\b(ged4all|gem.?taxonomy)\b"],
    "MOVER":    [r"\bmover\b"],
    "GLIDE":    [r"\bglide\b"],
    "EMDAT":    [r"\b(em[\-\s]?dat|emdat)\b"],
    "USGS_EHP": [r"\b(usgs.?ehp|usgs.?earthquake.?hazard)\b"],
    "OED":      [r"\boed\b"],
    "HAZUS":    [r"\b(hazus|fema.?taxonomy)\b"],
    "EMS-98":   [r"\b(ems[\-\s]?98|european.?macroseismic)\b"],
    "PAGER":    [r"\b(pager|usgs.?pager)\b"],
    "CDC-SVI":  [r"\b(cdc[\-\s]?svi|social.?vulnerability.?index)\b"],
    "INFORM":   [r"\binform\s+(?:risk|index|severity)\b"],
    "Custom":   [],  # Fallback — matches nothing; assigned when no other scheme matches
}


# ---------------------------------------------------------------------------
# Exposure Extractor
# ---------------------------------------------------------------------------

class ExposureExtractor:
    """Extracts RDLS Exposure block from metadata using 3-tier cascade."""

    CONFIDENCE_MAP = {"high": 0.9, "medium": 0.7, "low": 0.5}
    TIER1_FIELDS = {"title", "name", "tags"}
    TIER2_FIELDS = {"resources"}
    TIER3_FIELDS = {"notes", "methodology"}
    CORROBORATION_BOOST = 0.05

    def __init__(self, signal_dict: Dict[str, Any],
                 defaults: Optional[Dict[str, Any]] = None):
        self.signal_dict = signal_dict
        self.defaults = defaults or {}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Compile exposure category patterns from signal dictionary."""
        self.category_patterns = {}
        for category, cfg in self.signal_dict.get("exposure_category", {}).items():
            compiled = []
            for p in cfg.get("patterns", []):
                try:
                    compiled.append(re.compile(p, re.IGNORECASE))
                except re.error:
                    pass
            self.category_patterns[category] = {
                "compiled": compiled,
                "confidence": self.CONFIDENCE_MAP.get(cfg.get("confidence", "medium"), 0.7),
            }

    def _extract_text_fields(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Extract and normalize text fields."""
        title = normalize_text(record.get("title", ""))
        name = normalize_text(record.get("name", ""))
        notes = normalize_text(record.get("notes", ""))
        methodology = normalize_text(record.get("methodology", ""))

        tags = record.get("tags", [])
        if isinstance(tags, list):
            tag_str = " ".join(
                t.get("name", "") if isinstance(t, dict) else str(t) for t in tags
            ).lower()
        else:
            tag_str = str(tags).lower()

        resources = record.get("resources", [])
        res_parts = []
        if isinstance(resources, list):
            for r in resources:
                if isinstance(r, dict):
                    res_parts.append(r.get("name", ""))
                    res_parts.append(r.get("description", ""))
        res_str = normalize_text(" ".join(res_parts))

        return {
            "title": title, "name": name, "tags": tag_str,
            "resources": res_str, "notes": notes, "methodology": methodology,
        }

    def _scan_tier(self, text_fields: Dict[str, str],
                   tier_fields: set) -> Dict[str, ExtractionMatch]:
        """Scan specific tier fields for exposure categories."""
        found = {}
        for category, cfg in self.category_patterns.items():
            for pat in cfg["compiled"]:
                for field_name in tier_fields:
                    text = text_fields.get(field_name, "")
                    if not text:
                        continue
                    m = pat.search(text)
                    if m and category not in found:
                        found[category] = ExtractionMatch(
                            value=category,
                            confidence=cfg["confidence"],
                            source_field=field_name,
                            matched_text=m.group(0),
                            pattern=pat.pattern,
                        )
        return found

    def _infer_metrics(self, text: str, categories: List[str]) -> Dict[str, List[MetricExtraction]]:
        """Infer metric dimensions and quantity kinds for each category.

        Detects ALL matching dimensions from text (not just the first),
        then validates each (dimension, quantity_kind) pair against the
        valid_triplets config. Falls back to default if nothing detected.

        A single category can produce multiple metrics — e.g., an
        infrastructure dataset mentioning road length AND service disruption
        yields both (structure, length) and (disruption, time).
        """
        metrics = {}
        metric_defaults = self.defaults.get("exposure_metric_defaults", {})
        valid_triplets = self.defaults.get("exposure_valid_triplets", {})

        # Pre-detect all dimensions and quantity_kinds found in text
        detected_dims = set()
        for dim, patterns in DIMENSION_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    detected_dims.add(dim)
                    break  # one match per dimension is enough

        detected_qks = set()
        for qk, patterns in QUANTITY_KIND_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    detected_qks.add(qk)
                    break

        for cat in categories:
            cat_metrics = []
            cat_triplets = valid_triplets.get(cat, [])

            if cat_triplets and detected_dims:
                # Match detected dimensions against valid triplets for this category
                seen = set()
                for triplet in cat_triplets:
                    t_dim = triplet["dimension"]
                    t_qk = triplet["quantity_kind"]
                    if t_dim in detected_dims and (t_dim, t_qk) not in seen:
                        # If we also detected the quantity_kind, use it; otherwise
                        # trust the triplet's default quantity_kind for that dimension
                        if detected_qks and t_qk not in detected_qks:
                            # Detected a different quantity_kind — check if the
                            # detected one is also valid for this dim+category
                            alt_match = [
                                tr for tr in cat_triplets
                                if tr["dimension"] == t_dim and tr["quantity_kind"] in detected_qks
                            ]
                            if alt_match:
                                for alt in alt_match:
                                    key = (alt["dimension"], alt["quantity_kind"])
                                    if key not in seen:
                                        cat_metrics.append(MetricExtraction(
                                            dimension=alt["dimension"],
                                            quantity_kind=alt["quantity_kind"],
                                            confidence=0.8,
                                            source_hint="pattern+triplet",
                                        ))
                                        seen.add(key)
                                continue

                        cat_metrics.append(MetricExtraction(
                            dimension=t_dim,
                            quantity_kind=t_qk,
                            confidence=0.7,
                            source_hint="pattern+triplet",
                        ))
                        seen.add((t_dim, t_qk))

            # If nothing matched from triplets, fall back to default
            if not cat_metrics:
                default = metric_defaults.get(cat, {})
                dimension = default.get("dimension", "structure")
                quantity_kind = default.get("quantity_kind", "count")
                cat_metrics.append(MetricExtraction(
                    dimension=dimension,
                    quantity_kind=quantity_kind,
                    confidence=0.5,
                    source_hint="default",
                ))

            metrics[cat] = cat_metrics

        return metrics

    def _detect_taxonomy(self, text: str) -> Optional[str]:
        """Detect taxonomy/classification scheme from text."""
        for taxonomy, patterns in TAXONOMY_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return taxonomy
        return None

    def _detect_currency(self, text: str) -> Optional[str]:
        """Detect currency from text.

        First checks explicit patterns, then falls back to matching
        any uppercase 3-letter word against COMMON_CURRENCIES set.
        """
        for pattern, currency in CURRENCY_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return currency
        # Fallback: look for 3-letter ISO currency codes
        for m in re.finditer(r"\b([A-Z]{3})\b", text):
            code = m.group(1)
            if code in COMMON_CURRENCIES:
                return code
        return None

    def extract(self, record: Dict[str, Any]) -> ExposureExtraction:
        """Run full exposure extraction on a metadata record."""
        text_fields = self._extract_text_fields(record)
        combined_text = " ".join(text_fields.values())

        # 3-tier cascade
        tier1 = self._scan_tier(text_fields, self.TIER1_FIELDS)
        tier2 = self._scan_tier(text_fields, self.TIER2_FIELDS)
        tier3 = self._scan_tier(text_fields, self.TIER3_FIELDS)

        # Merge tiers with corroboration boost:
        # Tier 1 always authoritative; Tier 2 can add new or boost;
        # Tier 3 can only boost existing (or fallback if nothing found).
        final = dict(tier1)

        # Tier 2 — add new categories or corroborate existing
        for cat, match in tier2.items():
            if cat in final:
                # Corroborate: boost confidence
                existing = final[cat]
                final[cat] = ExtractionMatch(
                    value=cat,
                    confidence=min(1.0, max(existing.confidence, match.confidence) + self.CORROBORATION_BOOST),
                    source_field=existing.source_field,
                    matched_text=existing.matched_text,
                    pattern=existing.pattern,
                )
            else:
                # New category from resource tier — allow it
                final[cat] = match

        # Tier 3 — corroborate only (or fallback when nothing found)
        if final:
            for cat, match in tier3.items():
                if cat in final:
                    existing = final[cat]
                    final[cat] = ExtractionMatch(
                        value=cat,
                        confidence=min(1.0, existing.confidence + self.CORROBORATION_BOOST),
                        source_field=existing.source_field,
                        matched_text=existing.matched_text,
                        pattern=existing.pattern,
                    )
                # Tier 3 never adds new categories when we already have some
        else:
            # Fallback: use Tier 3 as last resort
            for cat, match in tier3.items():
                final[cat] = match

        categories = list(final.values())
        category_names = [m.value for m in categories]

        # Metrics
        metrics = self._infer_metrics(combined_text, category_names)

        # Taxonomy
        taxonomy = self._detect_taxonomy(combined_text)

        # Currency
        currency = self._detect_currency(combined_text)

        # Confidence
        overall = max((m.confidence for m in categories), default=0.0)

        return ExposureExtraction(
            categories=categories,
            metrics=metrics,
            taxonomy_hint=taxonomy,
            currency=currency,
            overall_confidence=overall,
        )


# ---------------------------------------------------------------------------
# Build RDLS exposure block
# ---------------------------------------------------------------------------

def build_exposure_block(extraction: ExposureExtraction) -> Optional[List[Dict[str, Any]]]:
    """Convert ExposureExtraction to RDLS exposure JSON block.

    Returns None if no exposure detected.
    """
    if not extraction.has_exposure:
        return None

    exposures = []
    for cat_match in extraction.categories:
        cat = cat_match.value
        exp = {
            "id": f"exp_{uuid.uuid4().hex[:8]}",
            "category": cat,
        }

        if extraction.taxonomy_hint:
            exp["taxonomy"] = extraction.taxonomy_hint

        # Add metrics
        cat_metrics = extraction.metrics.get(cat, [])
        if cat_metrics:
            exp["metrics"] = []
            for i, metric in enumerate(cat_metrics):
                exp["metrics"].append({
                    "id": f"metric_{uuid.uuid4().hex[:8]}",
                    "dimension": metric.dimension,
                    "quantity_kind": metric.quantity_kind,
                })

        exposures.append(exp)

    return exposures
