#!/usr/bin/env python3
"""Build the matched-title robustness inputs used by the paper table workflow."""

from __future__ import annotations

import json
import re
from collections import Counter

import pandas as pd

from generate_paper_tables import (
    ADULT_DATA,
    DATA_DIR,
    ModelResult,
    add_demeaned_columns,
    fit_clustered_fe,
    normalize,
    preprocess_adults,
    restrict_unisex_salons,
)


RAW_ADULT_DATA = DATA_DIR / "snapshots" / "treatwell-all-2025-06-02.csv"
MODEL_SUMMARY_OUTPUT = DATA_DIR / "derived" / "matched_category_model_summary.json"

MIN_PRIMARY_LISTINGS = 500
MIN_PRIMARY_SALONS = 150
MIN_THIN_LISTINGS = 300
MIN_THIN_SALONS = 100

GENDER_WORDS = [
    "men",
    "mens",
    "man",
    "gent",
    "gents",
    "women",
    "woman",
    "ladies",
    "lady",
    "herren",
    "herr",
    "damen",
    "dame",
    "homme",
    "femme",
    "hombre",
    "mujer",
    "uomo",
    "donna",
    "homem",
    "mulher",
    "heren",
    "dames",
]

BUNDLE_MARKERS = {
    "bundle_beard": [
        "beard",
        "bart",
        "barba",
        "rasur",
        "shave",
        "shaving",
        "moustache",
        "mustache",
        "grooming",
    ],
    "bundle_color": [
        "color",
        "colour",
        "tint",
        "bleach",
        "highlight",
        "balayage",
        "meches",
        "strahnen",
        "blond",
        "ombr",
        "patine",
        "gloss",
    ],
    "bundle_treatment": [
        "keratin",
        "mask",
        "repair",
        "treatment",
        "pflege",
        "soin",
        "tratamiento",
        "botox",
        "olaplex",
        "plex",
    ],
    "bundle_styling": [
        "styling",
        "coiffage",
        "stylen",
        "style",
        "finish",
    ],
    "bundle_blowdry": [
        "blow dry",
        "blowdry",
        "fohnen",
        "föhnen",
        "brushing",
        "sechage",
        "séchage",
        "piega",
    ],
    "bundle_premium": [
        "premium",
        "deluxe",
        "luxury",
        "signature",
        "makeover",
        "forfait",
        "package",
        "paket",
    ],
    "bundle_student": [
        "student",
        "junior",
        "senior",
        "schuler",
        "schüler",
        "etudiant",
        "étudiant",
        "idoso",
    ],
    "bundle_restyle": [
        "restyle",
        "re style",
        "reshape",
        "new style",
        "style change",
        "cambio de look",
        "trasformazione",
    ],
    "bundle_consultation": [
        "consultation",
        "beratung",
        "diagnostic",
        "diagnosis",
        "consulenza",
    ],
}

MARKER_GROUPS = {
    "length_short": ["short", "kurz", "court", "corto", "curto", "kort"],
    "length_medium": ["medium", "mittel", "medio", "moyen", "middellang"],
    "length_long": ["long", "lang", "largo", "longue"],
    "state_wash": ["wash", "waschen", "shampoo", "shampoing", "lavado", "lavage", "lavaggio", "wassen"],
    "state_dry": ["dry", "trocken", "a sec", "a secco", "sin lavado", "zonder wassen"],
    "state_wet": ["wet", "nass", "humido", "umido"],
    "type_machine": ["machine", "maschinen", "maschinenschnitt", "maquina", "máquina", "macchina"],
    "type_clipper": ["clipper", "tondeuse", "macchinetta"],
}

GENERIC_HAIRCUT_WORDS = [
    "haircut",
    "haarschnitt",
    "schnitt",
    "cut",
    "corte",
    "taglio",
    "coupe",
    "knippen",
    "knip",
    "frisur",
]

KEY_COLS = [
    "id",
    "simpleCutName",
    "simpleCutSalePrice",
    "simpleCutDurationMin",
    "simpleCutDurationMax",
    "is_male",
    "is_female",
]

GENDER_PATTERN = re.compile(r"\b(?:" + "|".join(re.escape(word) for word in GENDER_WORDS) + r")\b")
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")
MULTISPACE_PATTERN = re.compile(r"\s+")
MATCH_KEY = "genderless_title"


def normalize_title(text: object) -> str:
    value = normalize(text)
    value = NON_ALNUM_PATTERN.sub(" ", value)
    value = MULTISPACE_PATTERN.sub(" ", value).strip()
    return value


def strip_gender_words(text: str) -> str:
    cleaned = GENDER_PATTERN.sub(" ", text)
    return MULTISPACE_PATTERN.sub(" ", cleaned).strip()


def stem_text(text: str) -> str:
    stem = strip_gender_words(text)
    stem = re.sub(r"\b(?:%s)\b" % "|".join(re.escape(word) for word in GENERIC_HAIRCUT_WORDS), " ", stem)
    stem = MULTISPACE_PATTERN.sub(" ", stem).strip()
    return stem


def contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def add_match_sequence(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["match_seq"] = out.groupby(KEY_COLS).cumcount()
    return out


def parse_raw_service_metadata() -> tuple[pd.DataFrame, dict[str, int]]:
    records: list[dict[str, object]] = []
    stats: Counter[str] = Counter()
    seen_keys: Counter[tuple[object, ...]] = Counter()

    for chunk in pd.read_csv(RAW_ADULT_DATA, low_memory=False, chunksize=2000):
        for row in chunk.itertuples(index=False):
            key_tuple = tuple(getattr(row, column) for column in KEY_COLS)
            match_seq = seen_keys[key_tuple]
            seen_keys[key_tuple] += 1
            metadata = {
                "id": row.id,
                "simpleCutName": row.simpleCutName,
                "simpleCutSalePrice": row.simpleCutSalePrice,
                "simpleCutDurationMin": row.simpleCutDurationMin,
                "simpleCutDurationMax": row.simpleCutDurationMax,
                "is_male": row.is_male,
                "is_female": row.is_female,
                "match_seq": match_seq,
                "primaryTreatmentCategoryId": None,
                "treatmentCategoryIds": None,
                "optionGroupName": None,
                "optionName": None,
                "optionDurationMinutes": None,
                "raw_match_found": False,
            }
            try:
                raw = json.loads(row.raw)
            except Exception:
                stats["json_parse_failed"] += 1
                records.append(metadata)
                continue

            menu = raw.get("data", {}).get("menuHighlights", []) or []
            if not menu:
                stats["missing_menu_highlights"] += 1

            matched = False
            for item in menu:
                data = item.get("data", {}) or {}
                if data.get("name") != row.simpleCutName:
                    continue
                matched = True
                metadata["primaryTreatmentCategoryId"] = data.get("primaryTreatmentCategoryId")
                category_ids = data.get("treatmentCategoryIds")
                metadata["treatmentCategoryIds"] = json.dumps(category_ids) if category_ids is not None else None
                option_groups = data.get("optionGroups") or []
                if option_groups:
                    option_group = option_groups[0]
                    metadata["optionGroupName"] = option_group.get("name")
                    options = option_group.get("options") or []
                    if options:
                        option = options[0]
                        metadata["optionName"] = option.get("name")
                        metadata["optionDurationMinutes"] = option.get("durationMinutes")
                metadata["raw_match_found"] = True
                break

            if matched:
                stats["raw_name_match_found"] += 1
            else:
                stats["raw_name_match_missing"] += 1

            records.append(metadata)

    return pd.DataFrame.from_records(records), dict(stats)


def enrich_adult_baseline(adults: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    raw_meta, parse_stats = parse_raw_service_metadata()
    adults_keyed = add_match_sequence(adults)
    enriched = adults_keyed.merge(
        raw_meta,
        on=KEY_COLS + ["match_seq"],
        how="left",
        validate="one_to_one",
    )
    enriched["name_norm"] = enriched["simpleCutName"].map(normalize_title)
    enriched["name_stem"] = enriched["name_norm"].map(stem_text)
    enriched["name_stem"] = enriched["name_stem"].replace("", "__empty_stem__")
    enriched[MATCH_KEY] = enriched["name_norm"].map(strip_gender_words)
    enriched[MATCH_KEY] = enriched[MATCH_KEY].replace("", pd.NA)
    enriched["informative_residual"] = enriched[MATCH_KEY].map(lambda value: stem_text(value) if isinstance(value, str) else pd.NA)
    enriched["informative_residual"] = enriched["informative_residual"].replace("", pd.NA)

    for column, terms in MARKER_GROUPS.items():
        normalized_terms = [normalize_title(term) for term in terms]
        enriched[column] = enriched["name_norm"].apply(lambda value: contains_any(value, normalized_terms))

    for column, terms in BUNDLE_MARKERS.items():
        normalized_terms = [normalize_title(term) for term in terms]
        enriched[column] = enriched["name_norm"].apply(lambda value: contains_any(value, normalized_terms))

    bundle_cols = list(BUNDLE_MARKERS)
    enriched["noncomparable_bundle_flag"] = enriched[bundle_cols].any(axis=1)

    parse_stats["baseline_rows"] = len(adults)
    parse_stats["enriched_rows"] = len(enriched)
    parse_stats["merge_missing_primaryTreatmentCategoryId"] = int(enriched["primaryTreatmentCategoryId"].isna().sum())
    parse_stats["merge_missing_raw_match"] = int((~enriched["raw_match_found"].fillna(False)).sum())
    parse_stats["duplicate_join_rows"] = int(enriched.duplicated(KEY_COLS + ["match_seq"]).sum())

    drop_cols = ["match_seq"]
    enriched = enriched.drop(columns=drop_cols)
    return enriched, parse_stats


def add_cell_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["male_count_in_cell"] = out.groupby(["id", MATCH_KEY])["female"].transform(lambda s: int((s == 0).sum()))
    out["female_count_in_cell"] = out.groupby(["id", MATCH_KEY])["female"].transform(lambda s: int((s == 1).sum()))
    out["cell_count"] = out["male_count_in_cell"] + out["female_count_in_cell"]
    out["is_exact_pair_cell"] = (out["male_count_in_cell"] == 1) & (out["female_count_in_cell"] == 1)
    return out


def matched_cells(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.groupby(["id", MATCH_KEY])["female"]
        .agg(listings="size", female_count="sum")
        .reset_index()
    )
    counts["male_count"] = counts["listings"] - counts["female_count"]
    return counts[counts[MATCH_KEY].notna() & (counts["female_count"] > 0) & (counts["male_count"] > 0)].copy()


def apply_sample_a(df: pd.DataFrame) -> pd.DataFrame:
    cells = matched_cells(df)
    sample = df.merge(cells[["id", MATCH_KEY]], on=["id", MATCH_KEY], how="inner")
    return add_cell_counts(sample)


def apply_sample_b(sample_a: pd.DataFrame) -> pd.DataFrame:
    sample = sample_a[sample_a["is_exact_pair_cell"]].copy()
    return add_cell_counts(sample)


def marker_group_signature(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    grouped = df.groupby(["id", "name_stem", "female"])[columns].agg("max").reset_index()
    male = grouped[grouped["female"] == 0].drop(columns=["female"])
    female = grouped[grouped["female"] == 1].drop(columns=["female"])
    merged = male.merge(female, on=["id", "name_stem"], suffixes=("_male", "_female"))
    return merged


def markers_compatible(row: pd.Series, column_pairs: list[tuple[str, str]]) -> bool:
    for male_col, female_col in column_pairs:
        male_value = bool(row[male_col])
        female_value = bool(row[female_col])
        if male_value != female_value and (male_value or female_value):
            return False
    return True


def apply_sample_c(sample_b: pd.DataFrame) -> pd.DataFrame:
    sample = sample_b[sample_b["informative_residual"].notna()].copy()
    return add_cell_counts(sample)


def primary_sample(sample_c: pd.DataFrame) -> pd.DataFrame:
    sample = sample_c.copy()
    return add_cell_counts(sample)


def fit_matched_models(df: pd.DataFrame) -> tuple[ModelResult, ModelResult]:
    work = df.copy()
    work["fem_dur"] = work["female"] * work["duration"]
    work = add_demeaned_columns(work, ["price", "female", "duration", "fem_dur"])
    m1 = fit_clustered_fe(
        "price_dm ~ female_dm + duration_dm - 1",
        work,
        label="Matched Category FE (No Interaction)",
        coef_map={"Duration (min)": "duration_dm", "Female": "female_dm"},
    )
    m2 = fit_clustered_fe(
        "price_dm ~ female_dm + duration_dm + fem_dur_dm - 1",
        work,
        label="Matched Category FE (w/ Interaction)",
        coef_map={
            "Duration (min)": "duration_dm",
            "Female": "female_dm",
            "Duration $\\times$ Female": "fem_dur_dm",
        },
    )
    return m1, m2


def serialize_models(models: list[ModelResult]) -> list[dict[str, object]]:
    rows = ["Duration (min)", "Female", "Duration $\\times$ Female"]
    payload: list[dict[str, object]] = []
    for model in models:
        payload.append(
            {
                "label": model.label,
                "nobs": model.nobs,
                "n_groups": model.n_groups,
                "r2": model.r2,
                "coef": {row: model.coef(row) for row in rows},
                "se": {row: model.se(row) for row in rows},
                "pvalue": {row: model.pvalue(row) for row in rows},
            }
        )
    return payload


def print_parse_summary(stats: dict[str, int]) -> None:
    print("Raw enrichment summary:")
    for key in sorted(stats):
        print(f"- {key}: {stats[key]}")
    print("- no service-description field was found in raw menuHighlights/optionGroups/options; matching is title-based.")


def main() -> None:
    derived_dir = DATA_DIR / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    adults = preprocess_adults()
    adults_unisex = restrict_unisex_salons(adults)
    enriched, parse_stats = enrich_adult_baseline(adults_unisex)
    print_parse_summary(parse_stats)

    sample_a = apply_sample_a(enriched)
    sample_b = apply_sample_b(sample_a)
    sample_c = apply_sample_c(sample_b)
    primary = primary_sample(sample_c)

    primary_listings = len(primary)
    primary_salons = primary["id"].nunique()
    thin_ok = primary_listings >= MIN_THIN_LISTINGS and primary_salons >= MIN_THIN_SALONS
    primary_ok = primary_listings >= MIN_PRIMARY_LISTINGS and primary_salons >= MIN_PRIMARY_SALONS

    if primary_ok or thin_ok:
        models = list(fit_matched_models(primary))
        MODEL_SUMMARY_OUTPUT.write_text(
            json.dumps({"available": True, "models": serialize_models(models)}, indent=2) + "\n",
            encoding="utf-8",
        )
        if not primary_ok:
            print("\nWARNING: primary sample clears the thin threshold only; interpret the matched-category regression cautiously.")
    else:
        MODEL_SUMMARY_OUTPUT.write_text(
            json.dumps(
                {
                    "available": False,
                    "listings": primary_listings,
                    "salons": primary_salons,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print("\nMatched-category primary sample is too small for a stable regression.")
        print(f"- listings: {primary_listings}")
        print(f"- salons: {primary_salons}")
        print("- no model summary was generated.")


if __name__ == "__main__":
    main()
