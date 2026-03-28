import os
from datetime import datetime

from sqlalchemy import desc, or_

from models import OpaBundleDbo


class BundlesController:
    SORT_KEY_MAP: dict[str, str] = {
        "platform": "platform",
        "bundle_filename": "bundle_filename",
        "policy_hash": "policy_hash",
        "e_tag": "e_tag",
        "record_updated_date": "record_updated_date",
    }

    @staticmethod
    def get_bundles_paginated(
        session,
        sort_col_name: str,
        page_number: int,
        page_size: int,
        search_term: str,
        scope: str = "platform",
    ) -> tuple[int, list[dict]]:
        if scope == "all":
            return BundlesController._get_generic_all_bundles(
                session=session,
                search_term=search_term,
                page_number=page_number,
                page_size=page_size,
            )

        query = session.query(OpaBundleDbo)
        if search_term:
            wildcard = f"%{search_term}%"
            query = query.filter(
                or_(
                    OpaBundleDbo.platform.ilike(wildcard),
                    OpaBundleDbo.bundle_filename.ilike(wildcard),
                    OpaBundleDbo.policy_hash.ilike(wildcard),
                    OpaBundleDbo.e_tag.ilike(wildcard),
                )
            )

        sort_attr = BundlesController.SORT_KEY_MAP.get(
            sort_col_name, "record_updated_date"
        )
        sort_column = getattr(OpaBundleDbo, sort_attr)
        query = query.order_by(desc(sort_column), desc(OpaBundleDbo.opa_bundle_id))

        total_count = query.count()
        bundles = query.offset(page_number * page_size).limit(page_size).all()

        return total_count, [BundlesController._to_row(bundle) for bundle in bundles]

    @staticmethod
    def _get_generic_all_bundles(
        session, search_term: str, page_number: int, page_size: int
    ) -> tuple[int, list[dict]]:
        bundles = (
            session.query(OpaBundleDbo)
            .order_by(
                desc(OpaBundleDbo.record_updated_date),
                desc(OpaBundleDbo.opa_bundle_id),
            )
            .all()
        )

        latest_by_platform: dict[str, OpaBundleDbo] = {}
        for bundle in bundles:
            if bundle.platform not in latest_by_platform:
                latest_by_platform[bundle.platform] = bundle

        rows = [BundlesController._to_row(b) for b in latest_by_platform.values()]
        rows.sort(
            key=lambda row: (
                row.get("record_updated_date") or datetime.min,
                row.get("platform") or "",
            ),
            reverse=True,
        )

        all_row = BundlesController._build_all_row(rows)
        if all_row:
            rows = [all_row] + rows

        if search_term:
            wildcard = search_term.lower()
            rows = [
                row
                for row in rows
                if wildcard in (row.get("platform") or "").lower()
                or wildcard in (row.get("bundle_filename") or "").lower()
                or wildcard in (row.get("policy_hash") or "").lower()
                or wildcard in (row.get("e_tag") or "").lower()
            ]

        total_count = len(rows)
        start = page_number * page_size
        end = start + page_size
        return total_count, rows[start:end]

    @staticmethod
    def _build_all_row(platform_rows: list[dict]) -> dict | None:
        if not platform_rows:
            return None

        latest_record = max(
            platform_rows,
            key=lambda row: row.get("record_updated_date") or datetime.min,
        )
        total_size = sum(
            [row.get("size_bytes", 0) for row in platform_rows if row.get("size_bytes")]
        )

        return {
            "platform": "all",
            "bundle_filename": f"{len(platform_rows)} latest platform bundles",
            "e_tag": latest_record.get("e_tag"),
            "policy_hash": "aggregate",
            "record_updated_date": latest_record.get("record_updated_date"),
            "size_bytes": total_size,
            "scope": "all",
            "bundle_url": None,
        }

    @staticmethod
    def _to_row(bundle: OpaBundleDbo) -> dict:
        bundle_path = os.path.join(bundle.bundle_directory, bundle.bundle_filename)
        size_bytes: int | None = (
            os.path.getsize(bundle_path) if os.path.isfile(bundle_path) else None
        )

        return {
            "platform": bundle.platform,
            "bundle_filename": bundle.bundle_filename,
            "e_tag": bundle.e_tag,
            "policy_hash": bundle.policy_hash,
            "record_updated_date": bundle.record_updated_date,
            "size_bytes": size_bytes,
            "scope": "platform",
            "bundle_url": f"/api/v1/opa/bundle/{bundle.platform}",
        }
