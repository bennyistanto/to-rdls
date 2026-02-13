"""
Vulnerability and Loss block extractors for RDLS metadata.

Vulnerability extraction: function types, approaches, relationships,
    intensity measures, impact metrics with constraint validation.
Loss extraction: 8 signal types with full defaults, impact modelling,
    approach, frequency, currency, temporal references.

Source-independent.
"""

import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .utils import normalize_text


# ---------------------------------------------------------------------------
# Data classes — Vulnerability
# ---------------------------------------------------------------------------

@dataclass
class FunctionExtraction:
    """Extracted vulnerability/fragility function."""
    function_type: str       # vulnerability, fragility, damage_to_loss, engineering_demand
    approach: str = "empirical"
    relationship: str = "math_parametric"
    hazard_primary: Optional[str] = None
    hazard_process_primary: Optional[str] = None
    intensity_measure: Optional[str] = None
    category: Optional[str] = None
    dimension: str = "structure"
    quantity_kind: str = "count"
    impact_type: str = "direct"
    impact_modelling: Optional[str] = None
    impact_metric: str = "damage_ratio"
    confidence: float = 0.5


@dataclass
class SocioEconomicExtraction:
    """Extracted socio-economic vulnerability indicator."""
    indicator_name: str
    indicator_code: str
    scheme: str = ""
    description: str = ""
    confidence: float = 0.5


@dataclass
class VulnerabilityExtraction:
    """Complete vulnerability extraction result."""
    functions: List[FunctionExtraction] = field(default_factory=list)
    socio_economic: List[SocioEconomicExtraction] = field(default_factory=list)
    overall_confidence: float = 0.0

    @property
    def has_vulnerability(self) -> bool:
        return len(self.functions) > 0 or len(self.socio_economic) > 0


# ---------------------------------------------------------------------------
# Data classes — Loss
# ---------------------------------------------------------------------------

@dataclass
class LossEntryExtraction:
    """Extracted loss entry."""
    loss_signal_type: str    # human_loss, displacement, affected_population, etc.
    hazard_type: Optional[str] = None
    hazard_process: Optional[str] = None
    asset_category: str = "buildings"
    asset_dimension: str = "structure"
    impact_type: str = "direct"
    impact_modelling: Optional[str] = None
    impact_metric: str = "economic_loss_value"
    quantity_kind: str = "monetary"
    loss_type: str = "ground_up"
    loss_approach: Optional[str] = None
    loss_frequency_type: Optional[str] = None
    reference_year: Optional[int] = None
    currency: Optional[str] = None
    exposure_to_hazard: Optional[str] = None
    is_insured: bool = False
    confidence: float = 0.5


@dataclass
class LossExtraction:
    """Complete loss extraction result."""
    losses: List[LossEntryExtraction] = field(default_factory=list)
    overall_confidence: float = 0.0

    @property
    def has_loss(self) -> bool:
        return len(self.losses) > 0


# ---------------------------------------------------------------------------
# Pattern dictionaries — Vulnerability
# ---------------------------------------------------------------------------

FUNCTION_TYPE_PATTERNS = {
    "vulnerability": [
        r"\b(vulnerability[\s._-]?curve|vulnerability[\s._-]?function)\b",
        r"\b(damage[\s._-]?curve|damage[\s._-]?function)\b",
        r"\b(mean[\s._-]?damage[\s._-]?ratio|mdr)\b",
        r"\b(damage[\s._-]?ratio[\s._-]?(?:vs|versus|function))\b",
        r"\b(depth[\s._-]?damage)\b",
    ],
    "fragility": [
        r"\b(fragility[\s._-]?curve|fragility[\s._-]?function)\b",
        r"\b(probability[\s._-]?of[\s._-]?damage|failure[\s._-]?probability)\b",
        r"\b(capacity[\s._-]?spectrum|pushover)\b",
        r"\b(damage[\s._-]?state[\s._-]?(?:ds|probability))\b",
        r"\b(lognormal[\s._-]?fragility)\b",
    ],
    "damage_to_loss": [
        r"\b(damage[\s._-]?to[\s._-]?loss|consequence[\s._-]?function)\b",
        r"\b(loss[\s._-]?function|loss[\s._-]?model)\b",
        r"\b(repair[\s._-]?cost[\s._-]?(?:function|ratio|curve))\b",
        r"\b(replacement[\s._-]?cost[\s._-]?(?:function|ratio))\b",
    ],
    "engineering_demand": [
        r"\b(engineering[\s._-]?demand)\b",
        r"\b(interstorey[\s._-]?drift|inter[\s._-]?storey[\s._-]?drift)\b",
        r"\b(floor[\s._-]?acceleration|peak[\s._-]?floor)\b",
        r"\b(spectral[\s._-]?displacement|demand[\s._-]?capacity[\s._-]?ratio)\b",
    ],
}

APPROACH_PATTERNS = {
    "analytical": [
        r"\b(analytical|numerical|finite[\s._-]?element|simulation[\s._-]?based)\b",
        r"\b(capacity[\s._-]?spectrum|pushover[\s._-]?analysis|nonlinear[\s._-]?analysis)\b",
        r"\b(time[\s._-]?history[\s._-]?analysis|dynamic[\s._-]?analysis)\b",
    ],
    "empirical": [
        r"\b(empirical|observed|survey[\s._-]?based|field[\s._-]?data)\b",
        r"\b(post[\s._-]?disaster|post[\s._-]?event|damage[\s._-]?survey)\b",
        r"\b(historical[\s._-]?data|real[\s._-]?event)\b",
    ],
    "hybrid": [
        r"\b(hybrid|combined|mixed[\s._-]?method)\b",
    ],
    "judgement": [
        r"\b(expert[\s._-]?judg[e]?ment|expert[\s._-]?opinion|elicitation)\b",
        r"\b(heuristic|rule[\s._-]?based)\b",
    ],
}

RELATIONSHIP_PATTERNS = {
    "math_parametric": [
        r"\b(parametric|lognormal|normal[\s._-]?distribution|cumulative[\s._-]?distribution)\b",
        r"\b(cdf|probability[\s._-]?distribution|log[\s._-]?normal)\b",
        r"\b(median[\s._-]?and[\s._-]?dispersion|mu[\s._-]?and[\s._-]?sigma)\b",
    ],
    "math_bespoke": [
        r"\b(bespoke|custom[\s._-]?function|non[\s._-]?standard)\b",
        r"\b(piecewise|polynomial|spline)\b",
    ],
    "discrete": [
        r"\b(discrete|tabular|lookup[\s._-]?table|step[\s._-]?function)\b",
        r"\b(depth[\s._-]?damage[\s._-]?table|damage[\s._-]?matrix)\b",
    ],
}

IMPACT_TYPE_PATTERNS = {
    "direct": [r"\b(direct[\s._-]?(?:loss|damage|impact))\b"],
    "indirect": [r"\b(indirect[\s._-]?(?:loss|damage)|business[\s._-]?interruption|downtime)\b"],
    "total": [r"\b(total[\s._-]?(?:loss|damage|impact)|combined[\s._-]?loss)\b"],
}

IMPACT_MODELLING_PATTERNS = {
    "simulated": [r"\b(simulat|model(?:led|ed)|scenario[\s._-]?based)\b"],
    "observed": [r"\b(observed|recorded|actual|measured|field[\s._-]?survey)\b"],
    "inferred": [r"\b(inferred|derived|estimated|statistical)\b"],
}


# ---------------------------------------------------------------------------
# Pattern dictionaries — Loss
# ---------------------------------------------------------------------------

LOSS_SIGNAL_PATTERNS = {
    "human_loss": [
        r"\b(casualt(?:y|ies)|fatalit(?:y|ies)|mortalit(?:y|ies)|death)\b",
        r"\b(killed|dead|perished|deceased)\b",
        r"\b(injur(?:y|ies|ed)|wounded|hospitalized)\b",
        r"\b(missing[\s._-]?persons?|unaccounted)\b",
    ],
    "displacement": [
        r"\b(displaced|displacement|evacuated|evacuation)\b",
        r"\b(homeless|shelter[\s._-]?(?:less|need))\b",
        r"\b(internally[\s._-]?displaced|idp)\b",
        r"\b(refugee[\s._-]?(?:flow|movement|crisis))\b",
    ],
    "affected_population": [
        r"\b(affected[\s._-]?(?:population|people|person|household|communit))\b",
        r"\b(people[\s._-]?(?:affected|impacted|in[\s._-]?need))\b",
        r"\b(population[\s._-]?(?:affected|exposed|at[\s._-]?risk))\b",
    ],
    "economic_loss": [
        r"\b(economic[\s._-]?loss|financial[\s._-]?loss|monetary[\s._-]?loss)\b",
        r"\b(damage[\s._-]?cost|repair[\s._-]?cost|replacement[\s._-]?cost)\b",
        r"\b(insured[\s._-]?loss|insurance[\s._-]?claim)\b",
        r"\b(aal|average[\s._-]?annual[\s._-]?loss)\b",
        r"\b(expected[\s._-]?loss|probable[\s._-]?maximum[\s._-]?loss|pml)\b",
    ],
    "structural_damage": [
        r"\b(building[\s._-]?(?:damage|destroyed|collapsed|affected))\b",
        r"\b(structural[\s._-]?damage|house[\s._-]?(?:damage|destroyed))\b",
        r"\b(infrastructure[\s._-]?(?:damage|destroyed|loss))\b",
        r"\b(damage[\s._-]?(?:state|ratio|assessment|survey))\b",
    ],
    "agricultural_loss": [
        r"\b(crop[\s._-]?(?:loss|damage|failure|destroyed))\b",
        r"\b(agricultural[\s._-]?(?:loss|damage|impact))\b",
        r"\b(livestock[\s._-]?(?:loss|death|mortality))\b",
        r"\b(harvest[\s._-]?(?:loss|failure|damage))\b",
    ],
    "catastrophe_model": [
        r"\b(cat[\s._-]?model|catastrophe[\s._-]?model)\b",
        r"\b(risk[\s._-]?model|loss[\s._-]?model)\b",
        r"\b(loss[\s._-]?exceedance|ep[\s._-]?curve)\b",
    ],
    "general_loss": [
        r"\b(disaster[\s._-]?(?:loss|damage|impact|incident))\b",
        r"\b(natural[\s._-]?disaster[\s._-]?(?:loss|damage|impact|incident))\b",
        r"\b(damage[\s._-]?and[\s._-]?loss(?:es)?)\b",
        r"\b(post[\s._-]?disaster[\s._-]?(?:need|assessment|damage))\b",
        r"\b(pdna|dala|rapid[\s._-]?damage[\s._-]?assessment)\b",
    ],
}

LOSS_EXCLUSION_PATTERNS = [
    re.compile(r"\b(data[\s._-]?loss|packet[\s._-]?loss|signal[\s._-]?loss)\b", re.IGNORECASE),
    re.compile(r"\b(weight[\s._-]?loss|hair[\s._-]?loss|blood[\s._-]?loss)\b", re.IGNORECASE),
    re.compile(r"\b(loss[\s._-]?of[\s._-]?(?:data|signal|connectivity|precision))\b", re.IGNORECASE),
    re.compile(r"\b(profit[\s._-]?and[\s._-]?loss|p&l)\b", re.IGNORECASE),
]

INSURED_LOSS_PATTERNS = [
    re.compile(r"\b(insured[\s._-]?loss|insurance[\s._-]?claim|insured[\s._-]?damage)\b", re.IGNORECASE),
    re.compile(r"\b(insurance[\s._-]?payout|claim[\s._-]?amount)\b", re.IGNORECASE),
]

LOSS_APPROACH_PATTERNS = {
    "analytical": [
        r"\b(analytical|simulation[\s._-]?based|modelled|modeled|cat[\s._-]?model)\b",
        r"\b(catastrophe[\s._-]?model|risk[\s._-]?model)\b",
    ],
    "empirical": [
        r"\b(empirical|observed|survey|historical|field[\s._-]?data)\b",
        r"\b(post[\s._-]?disaster|post[\s._-]?event|damage[\s._-]?survey)\b",
        r"\b(actual|recorded|reported|pdna|dala)\b",
    ],
    "hybrid": [
        r"\b(hybrid|combined|mixed[\s._-]?method)\b",
    ],
    "judgement": [
        r"\b(expert[\s._-]?judg[e]?ment|expert[\s._-]?opinion|estimated)\b",
        r"\b(rapid[\s._-]?assessment|preliminary[\s._-]?estimate)\b",
    ],
}

LOSS_FREQUENCY_PATTERNS = {
    "probabilistic": [
        r"\b(probabilistic|stochastic|return[\s._-]?period|aal)\b",
        r"\b(average[\s._-]?annual[\s._-]?loss|expected[\s._-]?loss)\b",
        r"\b(ep[\s._-]?curve|loss[\s._-]?exceedance|exceedance[\s._-]?probability)\b",
        r"\b(probable[\s._-]?maximum[\s._-]?loss|pml|annual[\s._-]?exceedance)\b",
    ],
    "deterministic": [
        r"\b(deterministic|scenario[\s._-]?based|single[\s._-]?event)\b",
        r"\b(worst[\s._-]?case|maximum[\s._-]?credible)\b",
    ],
    "empirical": [
        r"\b(empirical|historical|observed|actual[\s._-]?event)\b",
        r"\b(recorded|reported|real[\s._-]?event|past[\s._-]?event)\b",
        r"\b(disaster[\s._-]?incident|event[\s._-]?based)\b",
    ],
}

CURRENCY_PATTERNS = [
    (r"\b(usd|us[\s._-]?dollar|united[\s._-]?states[\s._-]?dollar)\b", "USD"),
    (r"\b(eur|euro)\b", "EUR"),
    (r"\b(gbp|british[\s._-]?pound|pound[\s._-]?sterling)\b", "GBP"),
    (r"\b(jpy|japanese[\s._-]?yen)\b", "JPY"),
    (r"\b(cny|chinese[\s._-]?yuan|rmb|renminbi)\b", "CNY"),
    (r"\b(inr|indian[\s._-]?rupee)\b", "INR"),
    (r"\b(aud|australian[\s._-]?dollar)\b", "AUD"),
    (r"\b(cad|canadian[\s._-]?dollar)\b", "CAD"),
    (r"\b(chf|swiss[\s._-]?franc)\b", "CHF"),
    (r"\b(bdt|bangladeshi[\s._-]?taka|taka)\b", "BDT"),
    (r"\b(pkr|pakistani[\s._-]?rupee)\b", "PKR"),
    (r"\b(php|philippine[\s._-]?peso)\b", "PHP"),
    (r"\b(idr|indonesian[\s._-]?rupiah|rupiah)\b", "IDR"),
    (r"\b(kes|kenyan[\s._-]?shilling)\b", "KES"),
    (r"\b(ngn|nigerian[\s._-]?naira|naira)\b", "NGN"),
    (r"\b(etb|ethiopian[\s._-]?birr|birr)\b", "ETB"),
    (r"\b(mmk|myanmar[\s._-]?kyat|kyat)\b", "MMK"),
    (r"\b(afn|afghani)\b", "AFN"),
    (r"\b(htg|haitian[\s._-]?gourde|gourde)\b", "HTG"),
    (r"\b(ssp|south[\s._-]?sudanese[\s._-]?pound)\b", "SSP"),
    (r"\b(yer|yemeni[\s._-]?rial)\b", "YER"),
    (r"\b(sdg|sudanese[\s._-]?pound)\b", "SDG"),
    (r"\b(syp|syrian[\s._-]?pound)\b", "SYP"),
    (r"\b(cdf|congolese[\s._-]?franc)\b", "CDF"),
    (r"\b(mzn|mozambican[\s._-]?metical|metical)\b", "MZN"),
]

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20[0-2]\d)\b")


# ---------------------------------------------------------------------------
# Vulnerability Extractor
# ---------------------------------------------------------------------------

class VulnerabilityExtractor:
    """Extracts RDLS Vulnerability block from metadata.

    Uses constraint tables from rdls_defaults.yaml:
      - function_type_constraints: valid impact_metric per function type
      - function_type_approach_defaults: typical approach/relationship per type
      - vuln_category_defaults: category-specific overrides
      - impact_metric_constraints: quantity_kind + allowed impact_types per metric
      - socioeconomic_indicators: full indicator list with patterns
    """

    def __init__(self, signal_dict: Dict[str, Any],
                 defaults: Optional[Dict[str, Any]] = None):
        self.signal_dict = signal_dict
        self.defaults = defaults or {}
        # Load socioeconomic indicators from config (with patterns)
        self._load_socio_patterns()

    def _load_socio_patterns(self) -> None:
        """Compile socioeconomic indicator patterns from config."""
        self.socio_indicators = []
        indicators = self.defaults.get("socioeconomic_indicators", [])
        for ind in indicators:
            compiled = []
            for p in ind.get("patterns", []):
                try:
                    compiled.append(re.compile(p, re.IGNORECASE))
                except re.error:
                    pass
            self.socio_indicators.append({
                "code": ind.get("code", ""),
                "name": ind.get("name", ""),
                "scheme": ind.get("scheme", "Custom"),
                "description": ind.get("description", ""),
                "compiled": compiled,
            })
        # Generic patterns that need corroboration
        self.generic_socio_patterns = []
        for p in self.defaults.get("generic_socioeconomic_patterns", []):
            try:
                self.generic_socio_patterns.append(re.compile(p, re.IGNORECASE))
            except re.error:
                pass
        # Insufficient indicators (need corroboration)
        self.insufficient_codes = set(
            self.defaults.get("single_indicator_insufficient", [])
        )

    def _detect_approach(self, text: str) -> str:
        """Detect function approach from text."""
        for approach, patterns in APPROACH_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return approach
        return "empirical"  # fallback

    def _detect_relationship(self, text: str) -> str:
        """Detect relationship type from text."""
        for rel_type, patterns in RELATIONSHIP_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return rel_type
        return "math_parametric"  # fallback

    def _detect_impact_type(self, text: str) -> str:
        """Detect impact type from text."""
        for it, patterns in IMPACT_TYPE_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return it
        return "direct"

    def _detect_impact_modelling(self, text: str) -> Optional[str]:
        """Detect impact modelling type (simulated/observed/inferred)."""
        for mod_type, patterns in IMPACT_MODELLING_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return mod_type
        return None

    def _validate_metric(self, func_type: str, impact_metric: str,
                         impact_type: str) -> Tuple[str, str, str]:
        """Validate and correct impact_metric, quantity_kind, impact_type
        using constraint tables.

        Returns (impact_metric, quantity_kind, impact_type) — corrected if needed.
        """
        ft_constraints = self.defaults.get("function_type_constraints", {})
        im_constraints = self.defaults.get("impact_metric_constraints", {})

        ft = ft_constraints.get(func_type, {})
        allowed_metrics = set(ft.get("allowed_metrics", []))

        # If detected metric is not allowed for this function type, use default
        if allowed_metrics and impact_metric not in allowed_metrics:
            impact_metric = ft.get("default_metric", impact_metric)

        # Get quantity_kind from impact_metric_constraints
        mc = im_constraints.get(impact_metric, {})
        quantity_kind = mc.get("quantity_kind", ft.get("default_qty", "ratio"))

        # Validate impact_type against metric constraints
        allowed_impact_types = mc.get("allowed_impact_types", [])
        if allowed_impact_types and impact_type not in allowed_impact_types:
            impact_type = allowed_impact_types[0]

        return impact_metric, quantity_kind, impact_type

    def extract(self, record: Dict[str, Any],
                hazard_types: Optional[List[str]] = None,
                exposure_categories: Optional[List[str]] = None) -> VulnerabilityExtraction:
        """Run vulnerability extraction.

        Args:
            record: Metadata record.
            hazard_types: Pre-detected hazard types (from HazardExtractor).
            exposure_categories: Pre-detected exposure categories.
        """
        text = normalize_text(
            f"{record.get('title', '')} {record.get('notes', '')} "
            f"{record.get('methodology', '')}"
        )

        functions = []
        socio_economic = []

        # Config tables
        ft_approach_defaults = self.defaults.get("function_type_approach_defaults", {})
        vuln_cat_defaults = self.defaults.get("vuln_category_defaults", {})

        # Detect function types
        for func_type, patterns in FUNCTION_TYPE_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    # Detect approach (text first, then function type default)
                    approach = self._detect_approach(text)
                    ft_defaults = ft_approach_defaults.get(func_type, {})
                    if approach == "empirical" and ft_defaults:
                        # Only override if no explicit text match (empirical is fallback)
                        has_explicit_approach = False
                        for app, app_pats in APPROACH_PATTERNS.items():
                            if app == "empirical":
                                continue
                            for ap in app_pats:
                                if re.search(ap, text, re.IGNORECASE):
                                    has_explicit_approach = True
                                    break
                            if has_explicit_approach:
                                break
                        if not has_explicit_approach:
                            approach = ft_defaults.get("typical_approach", approach)

                    # Detect relationship (text first, then function type default)
                    relationship = self._detect_relationship(text)
                    if relationship == "math_parametric" and ft_defaults:
                        has_explicit_rel = False
                        for rel, rel_pats in RELATIONSHIP_PATTERNS.items():
                            if rel == "math_parametric":
                                continue
                            for rp in rel_pats:
                                if re.search(rp, text, re.IGNORECASE):
                                    has_explicit_rel = True
                                    break
                            if has_explicit_rel:
                                break
                        if not has_explicit_rel:
                            relationship = ft_defaults.get("typical_relationship", relationship)

                    # Impact metric default from function type
                    metric_defaults = self.defaults.get("function_impact_metric_defaults", {})
                    md = metric_defaults.get(func_type, {})
                    if isinstance(md, dict):
                        impact_metric = md.get("default_metric", "damage_ratio")
                    else:
                        # Backward compat: old format was just string
                        impact_metric = str(md) if md else "damage_ratio"

                    # Category override: if exposure category known, use its overrides
                    category = None
                    if exposure_categories:
                        category = exposure_categories[0]
                        cat_override = vuln_cat_defaults.get(category, {})
                        if cat_override:
                            # Use category-specific metric if available
                            impact_metric = cat_override.get("metric_override", impact_metric)

                    # Impact type
                    impact_type = self._detect_impact_type(text)

                    # Impact modelling
                    impact_modelling = self._detect_impact_modelling(text)

                    # Validate metric against constraints
                    impact_metric, quantity_kind, impact_type = self._validate_metric(
                        func_type, impact_metric, impact_type
                    )

                    func = FunctionExtraction(
                        function_type=func_type,
                        approach=approach,
                        relationship=relationship,
                        hazard_primary=hazard_types[0] if hazard_types else None,
                        category=category,
                        impact_type=impact_type,
                        impact_modelling=impact_modelling,
                        impact_metric=impact_metric,
                        quantity_kind=quantity_kind,
                        confidence=0.7,
                    )
                    functions.append(func)
                    break  # one match per function type is enough

        # If no function type detected but exposure category known, try category defaults
        if not functions and exposure_categories:
            for cat in exposure_categories:
                cat_default = vuln_cat_defaults.get(cat, {})
                if cat_default:
                    func_type = cat_default.get("typical_function", "vulnerability")
                    impact_metric = cat_default.get("metric_override", "damage_ratio")
                    qty = cat_default.get("qty_override", "ratio")

                    # Check if text actually suggests vulnerability
                    vuln_signal = False
                    generic_vuln_pats = [
                        r"\bvulnerability\b", r"\bfragility\b",
                        r"\bdamage[\s._-]?(?:function|curve|ratio)\b",
                    ]
                    for gp in generic_vuln_pats:
                        if re.search(gp, text, re.IGNORECASE):
                            vuln_signal = True
                            break

                    if vuln_signal:
                        ft_defaults = ft_approach_defaults.get(func_type, {})
                        func = FunctionExtraction(
                            function_type=func_type,
                            approach=ft_defaults.get("typical_approach", "empirical"),
                            relationship=ft_defaults.get("typical_relationship", "math_parametric"),
                            hazard_primary=hazard_types[0] if hazard_types else None,
                            category=cat,
                            impact_type="direct",
                            impact_metric=impact_metric,
                            quantity_kind=qty,
                            confidence=0.5,
                        )
                        functions.append(func)

        # Detect socio-economic indicators (from config patterns)
        for ind in self.socio_indicators:
            for pat in ind["compiled"]:
                if pat.search(text):
                    socio_economic.append(SocioEconomicExtraction(
                        indicator_name=ind["name"],
                        indicator_code=ind["code"],
                        scheme=ind["scheme"],
                        description=ind["description"],
                        confidence=0.7,
                    ))
                    break  # one match per indicator

        # Check generic socio-economic patterns (need corroboration)
        if not socio_economic:
            for gp in self.generic_socio_patterns:
                if gp.search(text):
                    # Generic match — add a generic indicator at lower confidence
                    socio_economic.append(SocioEconomicExtraction(
                        indicator_name="Socio-economic vulnerability",
                        indicator_code="SOCIO_VULN",
                        scheme="Custom",
                        description="Generic socio-economic vulnerability detected",
                        confidence=0.4,
                    ))
                    break

        # Filter out insufficient indicators when they're the only signal
        if socio_economic and not functions:
            strong_indicators = [
                se for se in socio_economic
                if se.indicator_code not in self.insufficient_codes
            ]
            if not strong_indicators:
                # All indicators are insufficient on their own
                for se in socio_economic:
                    se.confidence = max(se.confidence - 0.2, 0.2)

        # Overall confidence
        overall = 0.0
        if functions:
            overall = max(f.confidence for f in functions)
        elif socio_economic:
            overall = max(s.confidence for s in socio_economic)

        return VulnerabilityExtraction(
            functions=functions,
            socio_economic=socio_economic,
            overall_confidence=overall,
        )


# ---------------------------------------------------------------------------
# Loss Extractor
# ---------------------------------------------------------------------------

class LossExtractor:
    """Extracts RDLS Loss block from metadata.

    Uses constraint tables from rdls_defaults.yaml:
      - loss_signal_defaults: complete defaults per signal type
      - impact_metric_constraints: quantity_kind + impact_type validation
      - loss_valid_asset_triplets: valid dimensions per category
      - loss_type_approach_rules: valid approaches per loss_type
    """

    def __init__(self, signal_dict: Dict[str, Any],
                 defaults: Optional[Dict[str, Any]] = None):
        self.signal_dict = signal_dict
        self.defaults = defaults or {}

    def _is_excluded(self, text: str) -> bool:
        """Check if text matches loss exclusion patterns (false positives)."""
        for pat in LOSS_EXCLUSION_PATTERNS:
            if pat.search(text):
                return True
        return False

    def _detect_currency(self, text: str) -> Optional[str]:
        """Detect currency from text."""
        for pattern, currency in CURRENCY_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return currency
        return None

    def _detect_insured(self, text: str) -> bool:
        """Detect if loss is insured."""
        for pat in INSURED_LOSS_PATTERNS:
            if pat.search(text):
                return True
        return False

    def _detect_loss_approach(self, text: str) -> Optional[str]:
        """Detect loss approach from text."""
        for approach, patterns in LOSS_APPROACH_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return approach
        return None

    def _detect_loss_frequency(self, text: str) -> Optional[str]:
        """Detect loss frequency type from text."""
        for freq, patterns in LOSS_FREQUENCY_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return freq
        return None

    def _detect_impact_type(self, text: str) -> str:
        """Detect impact type from text."""
        for it, patterns in IMPACT_TYPE_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return it
        return "direct"

    def _detect_impact_modelling(self, text: str) -> Optional[str]:
        """Detect impact modelling type."""
        for mod_type, patterns in IMPACT_MODELLING_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    return mod_type
        return None

    def _validate_loss_entry(self, entry: LossEntryExtraction) -> LossEntryExtraction:
        """Validate and correct a loss entry using constraint tables."""
        im_constraints = self.defaults.get("impact_metric_constraints", {})
        asset_triplets = self.defaults.get("loss_valid_asset_triplets", {})

        # Validate quantity_kind from impact_metric
        mc = im_constraints.get(entry.impact_metric, {})
        if mc:
            entry.quantity_kind = mc.get("quantity_kind", entry.quantity_kind)
            # Validate impact_type
            allowed_types = mc.get("allowed_impact_types", [])
            if allowed_types and entry.impact_type not in allowed_types:
                entry.impact_type = allowed_types[0]

        # Validate asset_dimension for category
        valid_dims = asset_triplets.get(entry.asset_category, [])
        if valid_dims and entry.asset_dimension not in valid_dims:
            entry.asset_dimension = valid_dims[0]

        return entry

    def extract(self, record: Dict[str, Any],
                hazard_types: Optional[List[str]] = None) -> LossExtraction:
        """Run loss extraction.

        Args:
            record: Metadata record.
            hazard_types: Pre-detected hazard types.
        """
        text = normalize_text(
            f"{record.get('title', '')} {record.get('notes', '')} "
            f"{record.get('methodology', '')}"
        )

        # Check exclusions first
        if self._is_excluded(text):
            return LossExtraction(losses=[], overall_confidence=0.0)

        losses = []
        signal_defaults = self.defaults.get("loss_signal_defaults", {})

        # Detect loss signals
        for signal_type, patterns in LOSS_SIGNAL_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.IGNORECASE):
                    # Get defaults for this signal type
                    defaults = signal_defaults.get(signal_type, {})

                    # Impact type: text detection overrides default
                    impact_type = self._detect_impact_type(text)
                    if impact_type == "direct" and defaults.get("impact_type"):
                        # Only use detected if it's not just the fallback
                        has_explicit = False
                        for it, it_pats in IMPACT_TYPE_PATTERNS.items():
                            if it == "direct":
                                continue
                            for itp in it_pats:
                                if re.search(itp, text, re.IGNORECASE):
                                    has_explicit = True
                                    break
                            if has_explicit:
                                break
                        if not has_explicit:
                            impact_type = defaults.get("impact_type", "direct")

                    # Impact modelling
                    impact_modelling = self._detect_impact_modelling(text)

                    # Loss approach
                    loss_approach = self._detect_loss_approach(text)

                    # Loss frequency
                    loss_frequency = self._detect_loss_frequency(text)

                    # Insured loss check
                    is_insured = self._detect_insured(text)
                    impact_metric = defaults.get("impact_metric", "asset_loss")
                    if is_insured and signal_type == "economic_loss":
                        impact_metric = "insured_loss_value"

                    # Reference year
                    ref_year = None
                    year_match = YEAR_PATTERN.search(text)
                    if year_match:
                        ref_year = int(year_match.group(1))

                    # Currency
                    currency = self._detect_currency(text)

                    entry = LossEntryExtraction(
                        loss_signal_type=signal_type,
                        hazard_type=hazard_types[0] if hazard_types else None,
                        asset_category=defaults.get("asset_category", "buildings"),
                        asset_dimension=defaults.get("asset_dimension", "structure"),
                        impact_type=impact_type,
                        impact_modelling=impact_modelling,
                        impact_metric=impact_metric,
                        quantity_kind=defaults.get("quantity_kind", "count"),
                        loss_type=defaults.get("loss_type", "ground_up"),
                        loss_approach=loss_approach,
                        loss_frequency_type=loss_frequency,
                        reference_year=ref_year,
                        currency=currency,
                        is_insured=is_insured,
                        confidence=0.7,
                    )

                    # Set hazard process from defaults
                    if entry.hazard_type:
                        hp_defaults = self.defaults.get("hazard_process_defaults", {})
                        entry.hazard_process = hp_defaults.get(entry.hazard_type)

                    # Validate against constraints
                    entry = self._validate_loss_entry(entry)

                    losses.append(entry)
                    break  # one match per signal type

        overall = max((l.confidence for l in losses), default=0.0)

        return LossExtraction(
            losses=losses,
            overall_confidence=overall,
        )


# ---------------------------------------------------------------------------
# Build RDLS blocks
# ---------------------------------------------------------------------------

def build_vulnerability_block(extraction: VulnerabilityExtraction) -> Optional[Dict[str, Any]]:
    """Convert VulnerabilityExtraction to RDLS vulnerability JSON block."""
    if not extraction.has_vulnerability:
        return None

    block = {}

    if extraction.functions:
        functions_dict = {}
        for func in extraction.functions:
            ft = func.function_type
            if ft not in functions_dict:
                functions_dict[ft] = []
            entry = {
                "id": f"func_{uuid.uuid4().hex[:8]}",
                "approach": func.approach,
                "relationship": func.relationship,
                "impact": {
                    "type": func.impact_type,
                    "metric": func.impact_metric,
                    "quantity_kind": func.quantity_kind,
                },
            }
            if func.hazard_primary:
                entry["hazard_type"] = func.hazard_primary
            if func.impact_modelling:
                entry["impact_modelling"] = func.impact_modelling
            if func.category:
                entry["category"] = func.category
            if func.intensity_measure:
                entry["intensity_measure"] = func.intensity_measure
            functions_dict[ft].append(entry)
        block["functions"] = functions_dict

    if extraction.socio_economic:
        block["socio_economic"] = [
            {
                "id": f"se_{uuid.uuid4().hex[:8]}",
                "indicator_name": se.indicator_name,
                "indicator_code": se.indicator_code,
                "scheme": se.scheme,
                "description": se.description,
            }
            for se in extraction.socio_economic
        ]

    return block


def build_loss_block(extraction: LossExtraction) -> Optional[Dict[str, Any]]:
    """Convert LossExtraction to RDLS loss JSON block."""
    if not extraction.has_loss:
        return None

    losses = []
    for entry in extraction.losses:
        loss = {
            "id": f"loss_{uuid.uuid4().hex[:8]}",
            "asset_category": entry.asset_category,
            "asset_dimension": entry.asset_dimension,
            "impact_and_losses": {
                "type": entry.impact_type,
                "metric": entry.impact_metric,
                "quantity_kind": entry.quantity_kind,
                "loss_type": entry.loss_type,
            },
        }
        if entry.hazard_type:
            loss["hazard_type"] = entry.hazard_type
        if entry.hazard_process:
            loss["hazard_process"] = entry.hazard_process
        if entry.reference_year:
            loss["reference_year"] = entry.reference_year
        if entry.currency:
            loss["impact_and_losses"]["currency"] = entry.currency
        if entry.impact_modelling:
            loss["impact_and_losses"]["impact_modelling"] = entry.impact_modelling
        if entry.loss_approach:
            loss["impact_and_losses"]["loss_approach"] = entry.loss_approach
        if entry.loss_frequency_type:
            loss["impact_and_losses"]["loss_frequency_type"] = entry.loss_frequency_type
        if entry.is_insured:
            loss["impact_and_losses"]["loss_type"] = "insured"

        losses.append(loss)

    return {"losses": losses}
