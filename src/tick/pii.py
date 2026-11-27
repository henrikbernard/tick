"""Heuristic PII & sensitive data detection combining column names and value patterns."""

from __future__ import annotations

import re
from collections.abc import Sequence

from tick.models import CanonicalType, GovernanceFinding, PiiFinding

# Compiled value regexes
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
SSN_RE = re.compile(r"^(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}$")
PHONE_RE = re.compile(r"^(?:\+?\d{1,3}[\s.\-]?)?(?:\(\d{3}\)|\d{3})[\s.\-]?\d{3}[\s.\-]?\d{4}$")
IPV4_RE = re.compile(
    r"^(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)$"
)
IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$")
SECRET_RE = re.compile(
    r"^(?:sk_(?:live|test)_[A-Za-z0-9]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|eyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+|AKIA[0-9A-Z]{16})$"
)
CARD_CANDIDATE_RE = re.compile(r"^(?:\d[\s\-]?){13,19}$")

# Column-name patterns: category -> list of regexes
NAME_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "email": [re.compile(r"(?:^|_)(?:e?mail|email_addr(?:ess)?)(?:$|_)", re.I)],
    "phone": [re.compile(r"(?:^|_)(?:phone|mobile|cell_?(?:phone|num(?:ber)?)|tel|telephone|fax)(?:$|_|_num|_number)", re.I)],
    "ssn": [re.compile(r"(?:^|_)(?:ssn|social_security|sin_num|national_id|tax_id)(?:$|_)", re.I)],
    "credit_card": [
        re.compile(r"(?:^|_)(?:card_num(?:ber)?|cc_num(?:ber)?|credit_card|pan|debit_card)(?:$|_)", re.I)
    ],
    "ip_address": [re.compile(r"(?:^|_)(?:ip|ip_addr(?:ess)?|client_ip|remote_ip|ipv4|ipv6)(?:$|_)", re.I)],
    "iban": [re.compile(r"(?:^|_)(?:iban|bank_account|routing_num(?:ber)?|account_num(?:ber)?)(?:$|_)", re.I)],
    "secret_token": [
        re.compile(r"(?:^|_)(?:password|passwd|pwd|secret|api_key|access_token|refresh_token|private_key|auth_token)(?:$|_)", re.I)
    ],
    "date_of_birth": [re.compile(r"(?:^|_)(?:dob|birth_?date|date_of_birth|birthday)(?:$|_)", re.I)],
    "street_address": [
        re.compile(r"(?:^|_)(?:street|address|addr|street_address|billing_address|shipping_address|home_address)(?:$|_)", re.I)
    ],
    "person_name": [
        re.compile(r"(?:^|_)(?:first_name|last_name|full_name|given_name|family_name|surname|middle_name)(?:$|_)", re.I)
    ],
    "postal_code": [re.compile(r"(?:^|_)(?:zip|zip_code|postal_code|postcode)(?:$|_)", re.I)],
}

HIGH_RISK_CATEGORIES = {"ssn", "credit_card", "secret_token", "iban"}


def _luhn_valid(digits: str) -> bool:
    if not digits.isdigit() or not (13 <= len(digits) <= 19):
        return False
    total = 0
    reverse = digits[::-1]
    for idx, ch in enumerate(reverse):
        n = int(ch)
        if idx % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def _is_credit_card(value: str) -> bool:
    if not CARD_CANDIDATE_RE.match(value):
        return False
    digits = re.sub(r"[\s\-]", "", value)
    return _luhn_valid(digits)


def _is_iban(value: str) -> bool:
    """Validate IBAN format plus ISO 13616 MOD-97 checksum so serials/work-orders aren't misflagged."""
    compact = re.sub(r"\s+", "", value).upper()
    if not (15 <= len(compact) <= 34) or not IBAN_RE.match(compact):
        return False
    rearranged = compact[4:] + compact[:4]
    numeric = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in rearranged)
    try:
        return int(numeric) % 97 == 1
    except ValueError:
        return False


def _match_value_category(category: str, value: str) -> bool:
    v = value.strip()
    if not v or len(v) > 256:
        return False
    if category == "email":
        return bool(EMAIL_RE.match(v))
    if category == "ssn":
        return bool(SSN_RE.match(v))
    if category == "phone":
        return bool(PHONE_RE.match(v)) and any(ch in v for ch in ("-", "(", ")", "+", " "))
    if category == "credit_card":
        return _is_credit_card(v)
    if category == "ip_address":
        return bool(IPV4_RE.match(v))
    if category == "iban":
        return _is_iban(v)
    if category == "secret_token":
        return bool(SECRET_RE.match(v))
    return False


def detect_pii(
    column_name: str,
    canonical_type: CanonicalType,
    sample_values: Sequence[str],
) -> list[PiiFinding]:
    """Detect potential PII categories on a column using name + sampled value heuristics."""
    # Boolean flags (e.g. email_opt_in, is_phone_verified, has_dob) cannot contain actual PII payloads
    if canonical_type == "boolean":
        return []

    findings: list[PiiFinding] = []
    non_empty = [s.strip() for s in sample_values if s is not None and str(s).strip() != ""]
    sample_subset = non_empty[:200]

    # Check every category
    value_check_categories = {"email", "ssn", "phone", "credit_card", "ip_address", "iban", "secret_token"}
    norm_col = re.sub(r"[\s\-]+", "_", column_name.strip())

    for category, patterns in NAME_PATTERNS.items():
        # Temporal metadata columns (e.g. password_updated_at, email_sent_at) can only carry DOB PII
        if canonical_type in {"date", "timestamp", "time"} and category != "date_of_birth":
            continue
        name_hit = any(p.search(norm_col) for p in patterns)
        match_rate: float | None = None
        evidence: list[str] = []

        if name_hit:
            evidence.append(f"Column name '{column_name}' matches {category} pattern")

        # Run value pattern matching on string/other columns (or when name already matched)
        if sample_subset and category in value_check_categories and (
            canonical_type in {"string", "other"} or name_hit
        ):
            matches = sum(1 for val in sample_subset if _match_value_category(category, val))
            rate = matches / len(sample_subset)
            if rate > 0:
                match_rate = round(rate, 4)
                if rate >= 0.25:
                    evidence.append(f"{matches}/{len(sample_subset)} sampled values ({rate:.0%}) match {category} pattern")

        # Scoring
        score = 0.0
        if name_hit and match_rate is not None and match_rate >= 0.4:
            score = min(0.99, 0.75 + 0.24 * match_rate)
        elif match_rate is not None and match_rate >= 0.5:
            # Strong pure value match even if column has generic name like 'contact' or 'col_4'
            score = min(0.95, 0.55 + 0.4 * match_rate)
        elif name_hit:
            # Name-only match (e.g., first_name, date_of_birth, password_hash, or profile_mode=none)
            if category in {"date_of_birth", "person_name", "street_address", "postal_code", "secret_token"}:
                score = 0.80 if canonical_type in {"string", "date", "timestamp", "other"} else 0.55
            elif not sample_subset:
                score = 0.65
            elif category in {"phone", "ssn", "credit_card"} and canonical_type in {"integer", "string"}:
                score = 0.65
            else:
                score = 0.50

        if score >= 0.50:
            confidence = "high" if score >= 0.80 else ("medium" if score >= 0.65 else "low")
            findings.append(
                PiiFinding(
                    category=category,
                    confidence=confidence,  # type: ignore[arg-type]
                    score=round(score, 3),
                    evidence=evidence,
                    sample_match_rate=match_rate,
                )
            )

    findings.sort(key=lambda f: f.score, reverse=True)
    return findings


def redact_value(value: str, findings: Sequence[PiiFinding]) -> str:
    """Mask sensitive sample values when --redact-pii is enabled."""
    if not findings:
        return value
    top_cat = findings[0].category
    if top_cat == "email" and "@" in value:
        local, domain = value.split("@", 1)
        return f"{local[:1]}***@{domain}"
    if top_cat in {"ssn", "credit_card", "iban", "secret_token"}:
        return f"[REDACTED:{top_cat}]"
    if top_cat == "phone" and len(value) >= 4:
        return f"***-***-{value[-4:]}"
    return f"[REDACTED:{top_cat}]"


PLANT_ISOLATION_PATTERNS = [
    re.compile(r"^(?:werks|bukrs|lgort|plant_?id|plant_?code|site_?id|site_?code|facility_?id|facility_?code|factory_?id|factory_?code|mill_?code)$", re.I),
    re.compile(r"(?:^|_)(?:plant_id|plant_code|site_id|site_code|facility_id|factory_code)(?:$|_)", re.I),
]

COMMERCIAL_PATTERNS = [
    re.compile(
        r"(?:^|_)(?:unit_cost|std_cost|standard_cost|standard_price|moving_avg_price|bom_cost|labor_cost|overhead_cost|margin|gross_margin|supplier_price|contract_price|purchase_price|vendor_cost|wholesale_price|net_cost|net_price|cost_price|stprs|verpr|netpr)(?:$|_)",
        re.I,
    ),
]

IP_EXPORT_PATTERNS = [
    re.compile(
        r"(?:^|_)(?:cad_path|cad_file|cad_uri|cad_drawing(?:_path|_file)?|cad_model|drawing_path|drawing_file|drawing_no|drawing_num|drawing_number|blueprint|spec_doc|spec_uri|itar|itar_class|eccn|export_control|cage_code|defense_contract|formula_recipe|process_recipe|proprietary_spec)(?:$|_)",
        re.I,
    ),
]


def detect_governance(
    column_name: str,
    canonical_type: CanonicalType,
    semantic_name: str | None = None,
) -> list[GovernanceFinding]:
    """Detect manufacturing governance & Unity Catalog policy signals (plant row filters, cost masks, ITAR/IP)."""
    findings: list[GovernanceFinding] = []
    candidates = [re.sub(r"[\s\-]+", "_", column_name.strip())]
    if semantic_name:
        sem_norm = re.sub(r"[\s\-]+", "_", semantic_name.strip())
        if sem_norm and sem_norm not in candidates:
            candidates.append(sem_norm)

    if any(p.search(c) for c in candidates for p in PLANT_ISOLATION_PATTERNS):
        findings.append(
            GovernanceFinding(
                category="plant_isolation_key",
                confidence="high",
                uc_policy="row_filter",
                uc_tag_key="governance_scope",
                uc_tag_value="plant_isolation",
                evidence=[f"Column '{column_name}' matches plant/site/facility isolation key pattern (candidate UC row filter)."],
            )
        )

    if any(p.search(c) for c in candidates for p in COMMERCIAL_PATTERNS) and canonical_type in {"decimal", "float", "integer", "string", "other"}:
        findings.append(
            GovernanceFinding(
                category="commercial_sensitive",
                confidence="high" if canonical_type in {"decimal", "float"} else "medium",
                uc_policy="column_mask",
                uc_tag_key="sensitivity",
                uc_tag_value="commercial_cost",
                evidence=[f"Column '{column_name}' matches sensitive cost/margin/supplier pricing pattern (candidate UC column mask)."],
            )
        )

    if any(p.search(c) for c in candidates for p in IP_EXPORT_PATTERNS):
        findings.append(
            GovernanceFinding(
                category="ip_or_export_controlled",
                confidence="high",
                uc_policy="column_mask",
                uc_tag_key="compliance",
                uc_tag_value="itar_ear_ip",
                evidence=[f"Column '{column_name}' matches CAD/blueprint/ITAR/export-control/recipe IP pattern."],
            )
        )

    return findings

