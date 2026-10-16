"""Read-only tabular preparation shared by CLI application and staged previews."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moneybin.extractors.tabular.column_mapper import MappingResult

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.services.import_service import ReviewedTabularPlan

TabularPreviewPreparation = tuple[
    ReviewedTabularPlan, dict[str, list[str]], list[dict[str, str]], int
]


def read_tabular_source(path: Path, *, no_size_limit: bool = False) -> bytes:
    """Enforce the size gate before materializing one exact source snapshot."""
    from moneybin.extractors.tabular.format_detector import detect_format

    format_info = detect_format(path, no_size_limit=no_size_limit)
    with path.open("rb") as handle:
        source_bytes = handle.read(format_info.file_size + 1)
    if len(source_bytes) != format_info.file_size:
        raise UserError(
            "The file changed while MoneyBin was preparing its preview. Retry.",
            code=error_codes.IMPORT_PREVIEW_CHANGED,
        )
    return source_bytes


def detect_tabular_mapping(
    df: Any,
    *,
    file_type: str,
    date_format: str | None = None,
    overrides: dict[str, str] | None = None,
    omitted_fields: frozenset[str] = frozenset(),
    structural_red_flag: bool = False,
) -> MappingResult:
    """Use one normalization/detection path before any account/sign metadata."""
    from moneybin.config import get_settings
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.readers import (
        normalize_excel_date_columns_for_detection,
    )

    bands = get_settings().import_.confidence
    detection_df = normalize_excel_date_columns_for_detection(
        df, file_type=file_type, date_format=date_format
    )
    return map_columns(
        detection_df,
        overrides=overrides,
        omitted_fields=omitted_fields,
        t_high=bands.t_high,
        t_med=bands.t_med,
        structural_red_flag=structural_red_flag,
        declared_date_format=date_format,
    )


def prepare_tabular_preview(
    path: Path,
    *,
    source_bytes: bytes | None = None,
    mapping: dict[str, str] | None = None,
    omit_fields: list[str] | None = None,
) -> TabularPreviewPreparation:
    """Prepare the exact staged interpretation without database writes."""
    from moneybin.config import get_settings
    from moneybin.extractors.tabular.column_mapper import (
        collect_field_samples,
    )
    from moneybin.extractors.tabular.field_aliases import FIELD_ALIASES
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import (
        normalize_excel_date_columns_after_mapping,
        read_file,
    )
    from moneybin.services.import_confirmation import (
        MappingValidationError,
        coerce_confidence_tier,
        coerce_number_format,
        coerce_sign_convention,
        tabular_required_fields,
        validate_omitted_fields,
        validate_partial_mapping,
    )

    omissions = validate_omitted_fields(frozenset(omit_fields or []), mapping)
    bands = get_settings().import_.confidence
    try:
        format_info = detect_format(path, source_bytes=source_bytes)
        read_result = read_file(path, format_info, source_bytes=source_bytes)
        # No saved/matched format and no date-format parameter exist on this
        # preview path. detection_df is a throwaway copy — never imported,
        # never shown as a sample — that only exists so map_columns /
        # detect_date_format can recognize a native-typed date column's
        # content; read_result.df stays untouched until the final render,
        # below.
        mapping_result = detect_tabular_mapping(
            read_result.df,
            file_type=format_info.file_type,
            overrides=mapping,
            omitted_fields=omissions,
            structural_red_flag=read_result.header_row_looks_like_data,
        )
    except ValueError as e:
        raise UserError(str(e), code=error_codes.IMPORT_PREVIEW_ERROR) from e

    field_mapping = mapping_result.field_mapping
    confidence = mapping_result.confidence
    sign_convention = mapping_result.sign_convention
    number_format = mapping_result.number_format
    if mapping:
        required_fields = tabular_required_fields(
            proposed_keys=set(field_mapping.keys()),
            override_keys=set(mapping.keys()),
        )
        try:
            field_mapping = validate_partial_mapping(
                proposed=field_mapping,
                override=mapping,
                available_columns=tuple(read_result.df.columns),
                required_fields=required_fields,
                valid_destinations=tuple(FIELD_ALIASES.keys()),
            )
        except MappingValidationError as e:
            raise UserError(
                str(e), code=error_codes.IMPORT_PREVIEW_MAPPING_INVALID
            ) from e

    # The ONLY render of read_result.df — exactly once, against the FINAL
    # mapping, whether a column got there via the caller's own `mapping`
    # override or map_columns's own alias detection. No --date-format
    # parameter exists on this preview path, so the detector's own
    # mapping_result.date_format is always the effective one. field_mapping
    # and mapping_result.date_format are both already final here, before
    # anything below reads the frame's date-column text.
    read_result.df = normalize_excel_date_columns_after_mapping(
        read_result.df,
        file_type=format_info.file_type,
        field_mapping=field_mapping,
        date_format=mapping_result.date_format,
    )
    # Every caller-visible sample comes from the just-rendered frame under
    # the FINAL field_mapping — never mapping_result.sample_values, which
    # map_columns computed from detection_df: detection_df renders every
    # string column for format detection, so a native column mapped to a
    # non-date field (a Memo mapped to memo) would otherwise show a
    # rendering the real import never reproduces for it.
    sample_values = collect_field_samples(read_result.df, field_mapping)

    if mapping:
        sign_convention = coerce_sign_convention(
            field_mapping=field_mapping, detected=sign_convention
        )
        number_format = coerce_number_format(
            field_mapping=field_mapping,
            sample_values=sample_values,
            detected=number_format,
        )
        # A caller-validated override resolves the ambiguity the detector
        # couldn't (Req 11), but only for the fields it actually names. Re-score
        # and re-band through the canonical path rather than asserting a tier:
        # a hand-kept list of "facts an override cannot answer" was reported
        # incomplete twice — first missing the structural red flag and the
        # unreadable date, then missing a *second* required field still weakly
        # matched, which promoted an untouched weak match to `high` and made it
        # eligible for agent self-accept. score_mapping and resolve_tier already
        # know all three: a red flag forces low, an unread date scores 0.75, and
        # a surviving flag scores 0.85.
        confidence = coerce_confidence_tier(
            field_mapping=field_mapping,
            detected_flagged=mapping_result.flagged_fields,
            override_keys=set(mapping.keys()),
            date_format=mapping_result.date_format,
            structural_red_flag=read_result.header_row_looks_like_data,
            t_high=bands.t_high,
            t_med=bands.t_med,
        )

    from moneybin.services.import_confirmation import disputed_row_fields

    plan = ReviewedTabularPlan.from_read(
        format_info,
        read_result,
        field_mapping=field_mapping,
        date_format=mapping_result.date_format,
        sign_convention=sign_convention or "negative_is_expense",
        number_format=number_format or "us",
        is_multi_account=mapping_result.is_multi_account,
        confidence=confidence,
        flagged_fields=list(mapping_result.flagged_fields),
    )
    return (
        plan,
        sample_values,
        disputed_row_fields(
            read_result.header_position_ambiguous_rows,
            read_result.header_position_ambiguous_header_cells,
            field_mapping,
        ),
        len(read_result.df),
    )
