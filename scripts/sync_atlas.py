#!/usr/bin/env python3
"""Sync the rac-us-co corpus into Atlas/Supabase."""

from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid5

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
WAVES_DIR = ROOT / "waves"
SOURCE_ROOT = ROOT / "sources"
ROOT_SEGMENTS = ("regulation", "statute")
OFFICIAL_REGULATION_ROOT = SOURCE_ROOT / "official" / "9-CCR-2503-6"
OFFICIAL_STATUTE_ROOT = SOURCE_ROOT / "official" / "statute" / "crs"
REGULATION_PDF_URL = (
    "https://www.coloradosos.gov/CCR/GenerateRulePdf.do?fileName=9%20CCR%202503-6&ruleVersionId=11535"
)
REGULATION_TITLES = {
    "9-CCR-2503-6": "9 CCR 2503-6 Colorado Works Program",
}
STATUTE_COLLECTION_TITLES = {
    "crs": "Colorado Revised Statutes",
}
STATUTE_SECTION_TITLES = {
    "26-2-703": "Definitions",
    "26-2-709": "Benefits",
}
ROOT_LABELS = {
    "regulation": "Regulations",
    "statute": "Statutes",
}
AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}


def deterministic_id(citation_path: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"atlas:{citation_path}"))


def natural_key(value: str) -> list[Any]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", value)]


def extract_embedded_source(rac_text: str) -> str:
    match = re.match(r'\s*"""(.*?)"""\s*', rac_text, re.DOTALL)
    return match.group(1).strip() if match else ""


def extract_effective_date(text: str) -> str | None:
    editorial = re.search(r"effective date of (\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    if editorial:
        return editorial.group(1)
    snapshot = re.search(r"retrieved on (\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    if snapshot:
        return snapshot.group(1)
    return None


def latest_snapshot_dir(root: Path) -> Path | None:
    candidates = sorted(path for path in root.iterdir() if path.is_dir())
    return candidates[-1] if candidates else None


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def element_text(elem: ET.Element | None) -> str | None:
    if elem is None:
        return None
    text = normalize_text(" ".join(elem.itertext()))
    return text or None


def _r2_client_from_config():
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "Missing local AKN payload and boto3 is not installed. "
            "Install boto3 or materialize source.akn.xml from R2 first."
        ) from exc

    config_path = Path(
        os.environ.get(
            "RULES_XML_R2_CREDENTIALS",
            Path.home() / ".config" / "rulesfoundation" / "r2-rules-xml-credentials.json",
        )
    )
    creds = json.loads(config_path.read_text())
    client = boto3.client(
        "s3",
        endpoint_url=creds["endpoint_url"],
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
    )
    return client, creds["bucket"]


def read_r2_backed_source(path: Path) -> str:
    if path.exists():
        return path.read_text()
    sidecar_path = path.with_name(path.name + ".r2.json")
    if not sidecar_path.exists():
        raise FileNotFoundError(path)
    sidecar = json.loads(sidecar_path.read_text())
    client, bucket = _r2_client_from_config()
    response = client.get_object(
        Bucket=sidecar.get("r2_bucket", bucket),
        Key=sidecar["r2_key"],
    )
    return response["Body"].read().decode()


def regulation_snapshot_paths() -> tuple[Path, Path] | None:
    snapshot = latest_snapshot_dir(OFFICIAL_REGULATION_ROOT)
    if snapshot is None:
        return None
    outline_path = snapshot / "outline.json"
    akn_path = snapshot / "source.akn.xml"
    if not outline_path.exists():
        return None
    if not akn_path.exists() and not akn_path.with_name(akn_path.name + ".r2.json").exists():
        return None
    return outline_path, akn_path


def build_official_regulation_rules() -> list[dict[str, Any]]:
    paths = regulation_snapshot_paths()
    if paths is None:
        return []
    outline_path, akn_path = paths
    outline = json.loads(outline_path.read_text())
    akn_root = ET.fromstring(read_r2_backed_source(akn_path))
    source_path = str(akn_path.relative_to(ROOT))

    nodes: dict[str, dict[str, Any]] = {}
    children_by_parent: dict[str | None, set[str]] = defaultdict(set)

    root_citation = "us-co/regulation"
    instrument_citation = "us-co/regulation/9-CCR-2503-6"
    nodes[root_citation] = {
        "id": deterministic_id(root_citation),
        "jurisdiction": "us-co",
        "doc_type": "regulation",
        "parent_id": None,
        "level": 0,
        "ordinal": None,
        "heading": ROOT_LABELS["regulation"],
        "body": None,
        "effective_date": None,
        "repeal_date": None,
        "source_url": REGULATION_PDF_URL,
        "source_path": None,
        "rac_path": None,
        "has_rac": False,
        "citation_path": root_citation,
        "line_count": 0,
    }
    nodes[instrument_citation] = {
        "id": deterministic_id(instrument_citation),
        "jurisdiction": "us-co",
        "doc_type": "regulation",
        "parent_id": deterministic_id(root_citation),
        "level": 1,
        "ordinal": None,
        "heading": REGULATION_TITLES["9-CCR-2503-6"],
        "body": None,
        "effective_date": None,
        "repeal_date": None,
        "source_url": REGULATION_PDF_URL,
        "source_path": source_path,
        "rac_path": None,
        "has_rac": False,
        "citation_path": instrument_citation,
        "line_count": 0,
    }
    children_by_parent[None].add(root_citation)
    children_by_parent[root_citation].add(instrument_citation)

    def akn_payload(code: str) -> tuple[str | None, str | None]:
        eid = f"sec_{code.replace('.', '_')}"
        elem = akn_root.find(f".//*[@eId='{eid}']")
        if elem is None:
            return None, None
        blocks: list[str] = []
        num = element_text(next((child for child in elem if local_name(child.tag) == "num"), None))
        heading = element_text(next((child for child in elem if local_name(child.tag) == "heading"), None))
        if num:
            blocks.append(num)
        if heading:
            blocks.append(heading)
        for child in elem:
            if local_name(child.tag) != "content":
                continue
            for paragraph in child.iter():
                if local_name(paragraph.tag) != "p":
                    continue
                text = element_text(paragraph)
                if text and not text.startswith("CODE OF COLORADO REGULATIONS"):
                    blocks.append(text)
        effective_date = None
        for child in elem:
            if local_name(child.tag) == "remark":
                text = element_text(child)
                if text:
                    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
                    if match:
                        effective_date = match.group(1)
                        break
        if not blocks:
            return None, effective_date
        return "\n\n".join(blocks), effective_date

    def add_outline_items(items: list[dict[str, Any]], parent_citation: str) -> None:
        for item in items:
            title = item.get("title", "")
            match = re.match(r"^(\d+(?:\.\d+)+)\s+(.+)$", title)
            if not match:
                continue
            code, heading = match.groups()
            citation = f"{instrument_citation}/{code}"
            body, effective_date = akn_payload(code)
            nodes[citation] = {
                "id": deterministic_id(citation),
                "jurisdiction": "us-co",
                "doc_type": "regulation",
                "parent_id": deterministic_id(parent_citation),
                "level": len(citation.split("/")) - 1,
                "ordinal": None,
                "heading": heading,
                "body": body,
                "effective_date": effective_date,
                "repeal_date": None,
                "source_url": REGULATION_PDF_URL,
                "source_path": source_path,
                "rac_path": None,
                "has_rac": False,
                "citation_path": citation,
                "line_count": len(body.splitlines()) if body else 0,
            }
            children_by_parent[parent_citation].add(citation)
            add_outline_items(item.get("children", []), citation)

    add_outline_items(outline, instrument_citation)

    for parent_citation, child_paths in children_by_parent.items():
        sorted_paths = sorted(child_paths, key=lambda path: natural_key(path.split("/")[-1]))
        for ordinal, child_path in enumerate(sorted_paths, start=1):
            nodes[child_path]["ordinal"] = ordinal

    return sorted(nodes.values(), key=lambda row: (row["level"], natural_key(row["citation_path"])))


def build_official_statute_rules() -> list[dict[str, Any]]:
    if not OFFICIAL_STATUTE_ROOT.exists():
        return []

    nodes: dict[str, dict[str, Any]] = {}
    children_by_parent: dict[str | None, set[str]] = defaultdict(set)

    root_citation = "us-co/statute"
    collection_citation = "us-co/statute/crs"
    nodes[root_citation] = {
        "id": deterministic_id(root_citation),
        "jurisdiction": "us-co",
        "doc_type": "statute",
        "parent_id": None,
        "level": 0,
        "ordinal": None,
        "heading": ROOT_LABELS["statute"],
        "body": None,
        "effective_date": None,
        "repeal_date": None,
        "source_url": None,
        "source_path": None,
        "rac_path": None,
        "has_rac": False,
        "citation_path": root_citation,
        "line_count": 0,
    }
    nodes[collection_citation] = {
        "id": deterministic_id(collection_citation),
        "jurisdiction": "us-co",
        "doc_type": "statute",
        "parent_id": deterministic_id(root_citation),
        "level": 1,
        "ordinal": None,
        "heading": STATUTE_COLLECTION_TITLES["crs"],
        "body": None,
        "effective_date": None,
        "repeal_date": None,
        "source_url": None,
        "source_path": None,
        "rac_path": None,
        "has_rac": False,
        "citation_path": collection_citation,
        "line_count": 0,
    }
    children_by_parent[None].add(root_citation)
    children_by_parent[root_citation].add(collection_citation)

    for section_dir in sorted(OFFICIAL_STATUTE_ROOT.iterdir(), key=lambda path: natural_key(path.name)):
        if not section_dir.is_dir():
            continue
        snapshot_dir = latest_snapshot_dir(section_dir)
        if snapshot_dir is None:
            continue
        source_html = snapshot_dir / "source.html"
        if not source_html.exists():
            continue
        soup = BeautifulSoup(source_html.read_text(), "html.parser")
        canonical = soup.find("link", rel="canonical")
        source_url = canonical["href"] if canonical and canonical.get("href") else f"https://colorado.public.law/statutes/crs_{section_dir.name}"
        h1 = soup.find("h1")
        heading = section_dir.name
        if h1:
            heading_text = normalize_text(h1.get_text(" ", strip=True))
            if " / " in heading_text:
                heading = heading_text.split(" / ", 1)[1]
            elif "–" in heading_text:
                heading = heading_text.split("–", 1)[1].strip()
            else:
                heading = re.sub(r"^C\.R\.S\. Section\s+\S+\s+", "", heading_text).strip() or heading
        if heading == section_dir.name:
            title_tag = soup.find("title")
            if title_tag:
                title_text = normalize_text(title_tag.get_text(" ", strip=True))
                if "–" in title_text:
                    heading = title_text.split("–", 1)[1].strip()
        blocks: list[str] = []
        content = soup.select_one(".statute-content")
        if content:
            for child in content.find_all(["section", "p"], recursive=False):
                text = normalize_text(child.get_text(" ", strip=True))
                if text:
                    blocks.append(text)
        source_note = soup.select_one(".source-note") or soup.select_one("footer")
        if source_note:
            text = normalize_text(source_note.get_text(" ", strip=True))
            if text:
                blocks.append(text)
        body = "\n\n".join(blocks) or None
        citation = f"{collection_citation}/{section_dir.name}"
        nodes[citation] = {
            "id": deterministic_id(citation),
            "jurisdiction": "us-co",
            "doc_type": "statute",
            "parent_id": deterministic_id(collection_citation),
            "level": 2,
            "ordinal": None,
            "heading": heading,
            "body": body,
            "effective_date": None,
            "repeal_date": None,
            "source_url": source_url,
            "source_path": str(source_html.relative_to(ROOT)),
            "rac_path": None,
            "has_rac": False,
            "citation_path": citation,
            "line_count": len(body.splitlines()) if body else 0,
        }
        children_by_parent[collection_citation].add(citation)

    for parent_citation, child_paths in children_by_parent.items():
        sorted_paths = sorted(child_paths, key=lambda path: natural_key(path.split("/")[-1]))
        for ordinal, child_path in enumerate(sorted_paths, start=1):
            nodes[child_path]["ordinal"] = ordinal

    return sorted(nodes.values(), key=lambda row: (row["level"], natural_key(row["citation_path"])))


def all_repo_rac_paths() -> list[str]:
    paths: list[str] = []
    for root_segment in ROOT_SEGMENTS:
        root_dir = ROOT / root_segment
        if not root_dir.exists():
            continue
        for path in root_dir.rglob("*.rac"):
            if path.name.endswith(".rac.test"):
                continue
            paths.append(str(path.relative_to(ROOT)))
    return sorted(paths, key=natural_key)


def build_boundaries(repo_rac_path: str) -> list[list[str]]:
    no_suffix = repo_rac_path.removesuffix(".rac")
    parts = no_suffix.split("/")
    return [parts[:i] for i in range(1, len(parts) + 1)]


def latest_wave_by_path() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for manifest_path in sorted(WAVES_DIR.glob("*/manifest.json")):
        data = json.loads(manifest_path.read_text())
        wave = data.get("wave", manifest_path.parent.name)
        for entry in data.get("encoded_files", []):
            path = entry.get("path")
            if path:
                mapping[path] = wave
    return mapping


def source_slice_for_path(repo_rac_path: str) -> str | None:
    slice_path = SOURCE_ROOT / "slices" / repo_rac_path.removesuffix(".rac")
    txt_path = slice_path.with_suffix(".txt")
    if txt_path.exists():
        return str(txt_path.relative_to(ROOT))
    return None


def infer_leaf_heading(repo_rac_path: str, body: str) -> str:
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    if repo_rac_path.startswith("regulation/"):
        heading_lines = [
            line
            for line in lines
            if re.match(r"^(?:\([A-Za-z0-9.]+\)|[A-Z]\.|[IVXLC]+\.)", line)
        ]
        if heading_lines:
            return heading_lines[-1]
        if len(lines) >= 3:
            return lines[2]
    if repo_rac_path.startswith("statute/"):
        quoted = re.search(r'"([^"]+)"', body)
        if quoted:
            return quoted.group(1)
        if len(lines) >= 3:
            return lines[2].strip('"')
        if len(lines) >= 2:
            return lines[1]
    return Path(repo_rac_path).stem


def boundary_heading(boundary: list[str], repo_rac_path: str | None, body: str | None) -> str:
    root = boundary[0]
    if len(boundary) == 1:
        return ROOT_LABELS[root]

    if root == "regulation":
        if len(boundary) == 2:
            return REGULATION_TITLES.get(boundary[1], boundary[1].replace("-CCR-", " CCR "))
        if body:
            lines = [line.strip() for line in body.splitlines() if line.strip()]
            index = min(len(boundary), len(lines)) - 1
            if 0 <= index < len(lines):
                return lines[index]
        return boundary[-1]

    if len(boundary) == 2:
        return STATUTE_COLLECTION_TITLES.get(boundary[1], boundary[1].upper())
    if len(boundary) == 3:
        return STATUTE_SECTION_TITLES.get(boundary[2], boundary[2])
    if body:
        return infer_leaf_heading(repo_rac_path or "/".join(boundary), body)
    return boundary[-1]


def source_url_for_path(repo_rac_path: str) -> str | None:
    parts = Path(repo_rac_path).parts
    if not parts:
        return None
    if parts[0] == "regulation":
        return REGULATION_PDF_URL
    if parts[0] == "statute" and len(parts) >= 3 and parts[1] == "crs":
        return f"https://colorado.public.law/statutes/crs_{parts[2]}"
    return None


def build_repo_rules() -> list[dict[str, Any]]:
    nodes: dict[str, dict[str, Any]] = {}
    children_by_parent: dict[str | None, set[str]] = defaultdict(set)

    for repo_rac_path in all_repo_rac_paths():
        rac_file = ROOT / repo_rac_path
        body = extract_embedded_source(rac_file.read_text())
        effective_date = extract_effective_date(body)
        slice_path = source_slice_for_path(repo_rac_path)
        boundaries = build_boundaries(repo_rac_path)
        parent_citation: str | None = None
        for index, boundary in enumerate(boundaries):
            citation_path = "us-co/" + "/".join(boundary)
            is_leaf = index == len(boundaries) - 1
            node = nodes.get(citation_path)
            if node is None:
                node = {
                    "id": deterministic_id(citation_path),
                    "jurisdiction": "us-co",
                    "doc_type": boundary[0],
                    "parent_id": deterministic_id(parent_citation) if parent_citation else None,
                    "level": len(boundary) - 1,
                    "ordinal": None,
                    "heading": boundary_heading(boundary, repo_rac_path if is_leaf else None, body if is_leaf else None),
                    "body": None,
                    "effective_date": None,
                    "repeal_date": None,
                    "source_url": source_url_for_path(repo_rac_path),
                    "source_path": None,
                    "rac_path": None,
                    "has_rac": False,
                    "citation_path": citation_path,
                    "line_count": 0,
                }
                nodes[citation_path] = node
            if is_leaf:
                node["heading"] = infer_leaf_heading(repo_rac_path, body)
                node["body"] = body or None
                node["effective_date"] = effective_date
                node["source_url"] = source_url_for_path(repo_rac_path)
                node["source_path"] = slice_path or None
                node["rac_path"] = repo_rac_path
                node["has_rac"] = True
                node["line_count"] = len(body.splitlines()) if body else 0
            children_by_parent[parent_citation].add(citation_path)
            parent_citation = citation_path

    for parent_citation, child_paths in children_by_parent.items():
        sorted_paths = sorted(child_paths, key=lambda path: natural_key(path.split("/")[-1]))
        for ordinal, child_path in enumerate(sorted_paths, start=1):
            nodes[child_path]["ordinal"] = ordinal

    return sorted(nodes.values(), key=lambda row: (row["level"], natural_key(row["citation_path"])))


def merge_rules(*rule_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    nodes: dict[str, dict[str, Any]] = {}
    for rule_set in rule_sets:
        for rule in rule_set:
            citation_path = rule["citation_path"]
            existing = nodes.get(citation_path)
            if existing is None:
                nodes[citation_path] = dict(rule)
                continue
            if rule.get("rac_path"):
                for key, value in rule.items():
                    if value is not None:
                        existing[key] = value
                existing["rac_path"] = rule["rac_path"]
            else:
                for key in ("heading", "body", "effective_date", "source_url", "source_path", "line_count"):
                    if existing.get(key) in (None, "", 0) and rule.get(key) is not None:
                        existing[key] = rule[key]
            existing["has_rac"] = existing.get("has_rac", False) or rule.get("has_rac", False)

    children_by_parent: dict[str | None, list[dict[str, Any]]] = defaultdict(list)
    for node in nodes.values():
        children_by_parent[node["parent_id"]].append(node)
    for siblings in children_by_parent.values():
        siblings.sort(key=lambda node: natural_key(node["citation_path"].split("/")[-1]))
        for ordinal, node in enumerate(siblings, start=1):
            node["ordinal"] = ordinal

    return sorted(nodes.values(), key=lambda row: (row["level"], natural_key(row["citation_path"])))


def build_encoding_runs() -> list[dict[str, Any]]:
    now = datetime.now(UTC).isoformat()
    wave_by_path = latest_wave_by_path()
    rows: list[dict[str, Any]] = []
    for repo_rac_path in all_repo_rac_paths():
        rac_path = ROOT / repo_rac_path
        rows.append(
            {
                "id": f"rac-us-co:{repo_rac_path}",
                "timestamp": now,
                "citation": "us-co/" + repo_rac_path.removesuffix(".rac"),
                "file_path": repo_rac_path,
                "complexity": {},
                "iterations": [],
                "total_duration_ms": None,
                "predicted_scores": None,
                "final_scores": None,
                "agent_type": "manual-repo",
                "agent_model": None,
                "rac_content": rac_path.read_text(),
                "session_id": None,
                "synced_at": now,
                "data_source": "manual_estimate",
                "has_issues": False,
                "note": f"Imported from rac-us-co {wave_by_path.get(repo_rac_path, 'manual-state')}",
                "autorac_version": None,
            }
        )
    return rows


def chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def post_json(url: str, headers: dict[str, str], rows: list[dict[str, Any]]) -> None:
    response = requests.post(url, headers=headers, json=rows, timeout=180)
    response.raise_for_status()


def sync_rules(rules: list[dict[str, Any]], service_key: str, supabase_url: str, batch_size: int) -> None:
    url = supabase_url.rstrip("/") + "/rest/v1/rules"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Accept-Profile": "arch",
        "Content-Profile": "arch",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for rule in rules:
        grouped[int(rule["level"])].append(rule)
    for level in sorted(grouped):
        for batch in chunked(grouped[level], batch_size):
            post_json(url, headers, batch)


def sync_encoding_runs(rows: list[dict[str, Any]], service_key: str, supabase_url: str, batch_size: int) -> None:
    url = supabase_url.rstrip("/") + "/rest/v1/encoding_runs"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    for batch in chunked(rows, batch_size):
        post_json(url, headers, batch)


def delete_managed_rules(service_key: str, supabase_url: str) -> None:
    url = supabase_url.rstrip("/") + "/rest/v1/rules?jurisdiction=eq.us-co"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Accept-Profile": "arch",
        "Content-Profile": "arch",
        "Prefer": "count=exact,return=minimal",
    }
    response = requests.delete(url, headers=headers, timeout=180)
    response.raise_for_status()


def delete_managed_encoding_runs(service_key: str, supabase_url: str) -> None:
    url = (
        supabase_url.rstrip("/")
        + "/rest/v1/encoding_runs?id=like."
        + quote("rac-us-co:%", safe="")
    )
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Prefer": "count=exact,return=minimal",
    }
    response = requests.delete(url, headers=headers, timeout=180)
    response.raise_for_status()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-rules", action="store_true")
    parser.add_argument("--skip-encodings", action="store_true")
    parser.add_argument("--append-only", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    supabase_url = os.environ.get("RAC_SUPABASE_URL")
    service_key = os.environ.get("RAC_SUPABASE_SECRET_KEY")
    if not supabase_url or not service_key:
        raise SystemExit("RAC_SUPABASE_URL and RAC_SUPABASE_SECRET_KEY are required")

    rules = merge_rules(
        build_official_regulation_rules(),
        build_official_statute_rules(),
        build_repo_rules(),
    )
    encodings = build_encoding_runs()
    print(f"Prepared {len(rules)} arch.rules rows")
    print(f"Prepared {len(encodings)} encoding_runs rows")

    if args.dry_run:
        sample = {
            "rule": rules[0],
            "encoding": {k: encodings[0][k] for k in ["id", "citation", "file_path", "note"]},
        }
        print(json.dumps(sample, indent=2))
        return 0

    if not args.append_only:
        if not args.skip_rules:
            delete_managed_rules(service_key, supabase_url)
        if not args.skip_encodings:
            delete_managed_encoding_runs(service_key, supabase_url)

    if not args.skip_rules:
        sync_rules(rules, service_key, supabase_url, args.batch_size)
        print("Synced arch.rules")
    if not args.skip_encodings:
        sync_encoding_runs(encodings, service_key, supabase_url, args.batch_size)
        print("Synced encoding_runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
