# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/biolink.py (DEC-21).
# Changes from upstream:
#   - import paths only
# See README.md.
from enum import Enum

from .context import RunContext

try:
    from bmt import Toolkit
except ImportError:  # pragma: no cover - local unit env may not install worker deps.
    Toolkit = None # ty: ignore[invalid-assignment]
from translator_tom import Node


_BMT_TOOLKIT = None
_BMT_WARNING_EMITTED = False

_VALID_ASPECT_QUALIFIERS: frozenset | None = None
_ASPECT_QUALIFIER_WARNING_EMITTED = False

ASPECT_QUALIFIER_ENUM = "GeneOrGeneProductOrChemicalEntityAspectEnum"
ASPECT_QUALIFIER_ROOT = "activity_or_abundance"

FALLBACK_VALID_ASPECT_QUALIFIERS: frozenset[str] = frozenset({
    "activity_or_abundance",
    "abundance",
    "activity",
    "expression",
    "synthesis"
})

FALLBACK_CATEGORY_DEPTH: dict[str, int] = {
    "biolink:ChemicalEntity": 1,
    "biolink:ChemicalMixture": 2,
    "biolink:EnvironmentalFoodContaminant": 2,
    "biolink:FoodAdditive": 2,
    "biolink:MolecularEntity": 2,
    "biolink:ComplexMolecularMixture": 3,
    "biolink:Food": 3,
    "biolink:MolecularMixture": 3,
    "biolink:NucleicAcidEntity": 3,
    "biolink:ProcessedMaterial": 3,
    "biolink:SmallMolecule": 3,
    "biolink:Drug": 4,
}

# We could pull in "bmt" package, but that seems like overkill
# when we only need a few biolink classes.

class Agent_Type(Enum):
    MANUAL_AGENT        = "manual_agent"
    AUTOMATED_AGENT     = "automated_agent"
    COMPUTATIONAL_MODEL = "computational_model"
    TEXT_MINING_AGENT   = "text_mining_agent"

class Knowledge_Level(Enum):
    KNOWLEDGE_ASSERTION     = "knowledge_assertion"
    LOGICAL_ENTAILMENT      = "logical_entailment"
    PREDICTION              = "prediction"
    STATISTICAL_ASSOCIATION = "statistical_association"
    TEXT_CO_OCCURRENCE      = "text_co_occurrence"
    OBSERVATION             = "observation"
    NOT_PROVIDED            = "not_provided"


# TODO: Should we just expect that the user has installed the bmt library?
def get_bmt_toolkit(ctx: RunContext):
    """Return a cached Biolink Toolkit instance when the dependency is available."""
    global _BMT_TOOLKIT, _BMT_WARNING_EMITTED
    if Toolkit is None:
        if not _BMT_WARNING_EMITTED:
            ctx.reporter.warning("BMT is unavailable; using fallback specificity scores.")
            _BMT_WARNING_EMITTED = True
        return None
    if _BMT_TOOLKIT is None:
        try:
            _BMT_TOOLKIT = Toolkit()
        except Exception as exc:
            if not _BMT_WARNING_EMITTED:
                ctx.reporter.warning(
                    f"Failed to initialize BMT; using fallback specificity scores: {exc}"
                )
                _BMT_WARNING_EMITTED = True
            return None
    return _BMT_TOOLKIT


def get_valid_aspect_qualifiers() -> frozenset[str]:
    """Return the set of valid object_aspect_qualifier values for xCRG queries.

    Uses bmt to retrieve all descendants of activity_or_abundance in the
    GeneOrGeneProductOrChemicalEntityAspectEnum, falling back to a hardcoded
    set if bmt is unavailable.
    """
    global _VALID_ASPECT_QUALIFIERS, _ASPECT_QUALIFIER_WARNING_EMITTED
    if _VALID_ASPECT_QUALIFIERS is not None:
        return _VALID_ASPECT_QUALIFIERS
    # TODO: _module_logger = logging.getLogger(__name__)
    if Toolkit is not None:
        try:
            toolkit = _BMT_TOOLKIT or Toolkit()
            descendants = toolkit.get_permissible_value_descendants(
                ASPECT_QUALIFIER_ROOT, ASPECT_QUALIFIER_ENUM
            )
            _VALID_ASPECT_QUALIFIERS = frozenset(descendants)
            return _VALID_ASPECT_QUALIFIERS
        except Exception:
            if not _ASPECT_QUALIFIER_WARNING_EMITTED:
                # TODO: _module_logger.warning(
                # TODO:     f"Could not load valid aspect qualifiers from bmt; "
                # TODO:     f"using fallback set: {exc}"
                # TODO: )
                _ASPECT_QUALIFIER_WARNING_EMITTED = True
    _VALID_ASPECT_QUALIFIERS = FALLBACK_VALID_ASPECT_QUALIFIERS
    return _VALID_ASPECT_QUALIFIERS


def get_category_specificity(ctx: RunContext, category: str) -> int:
    """Return a Biolink specificity heuristic based on non-mixin ancestor count."""
    bmt_toolkit = get_bmt_toolkit(ctx)
    if bmt_toolkit:
        try:
            if not bmt_toolkit.get_element(category):
                return FALLBACK_CATEGORY_DEPTH.get(category, 0)
            ancestors = (
                    bmt_toolkit.get_ancestors(
                        category,
                        reflexive=False,
                        formatted=True,
                        mixin=False,
                    )
                    or []
            )
            return max(len(ancestors), FALLBACK_CATEGORY_DEPTH.get(category, 0))
        except Exception as exc:
            ctx.reporter.warning(
                f"Could not calculate BMT specificity for {category}: {exc}"
            )
    return FALLBACK_CATEGORY_DEPTH.get(category, 0)


def is_chemical_category(ctx: RunContext, category: str) -> bool:
    """Return True when a category is ChemicalEntity or a chemical descendant."""
    if category == "biolink:ChemicalEntity" or category in FALLBACK_CATEGORY_DEPTH:
        return True
    bmt_toolkit = get_bmt_toolkit(ctx)
    if not bmt_toolkit:
        return False
    try:
        ancestors = (
                bmt_toolkit.get_ancestors(
                    category,
                    reflexive=False,
                    formatted=True,
                    mixin=False,
                )
                or []
        )
        return "biolink:ChemicalEntity" in ancestors
    except Exception as exc:
        ctx.reporter.warning(f"Could not inspect category ancestry for {category}: {exc}")
        return False


def get_node_category_specificity(ctx: RunContext, node: Node | None) -> int:
    """Return the most specific chemical category score attached to a KG node."""
    if node is None:
        return 0
    chemical_categories = [
        category
        for category in node.categories
        if is_chemical_category(ctx, category)
    ]
    if not chemical_categories:
        return 0

    bmt_toolkit = get_bmt_toolkit(ctx)
    if bmt_toolkit and hasattr(bmt_toolkit, "get_most_specific_category"):
        try:
            most_specific = bmt_toolkit.get_most_specific_category(
                chemical_categories,
                formatted=True,
            )
            if is_chemical_category(ctx, most_specific):
                return get_category_specificity(ctx, most_specific)
        except Exception as exc:
            ctx.reporter.warning(f"Could not select most specific category with BMT: {exc}")

    return max(
        FALLBACK_CATEGORY_DEPTH.get(category, 0)
        for category in chemical_categories
    )
