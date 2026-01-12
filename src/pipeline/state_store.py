"""
SQLite-based state machine for the enrichment pipeline.

Provides persistent storage for pipeline execution state, enabling:
- Resumable runs after failures
- Cost tracking per node and company
- Gate-based flow control (MX validation, ICP scoring)
- Thread-safe concurrent access
"""

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class StateStore:
    """
    SQLite-backed state store for enrichment pipeline execution.

    Tracks node-level execution state and company-level aggregations
    with support for resumable runs and gate-based flow control.

    Thread-safe: All database operations are protected by a threading lock.

    Usage:
        store = StateStore()  # Uses default .state/pipeline.db
        store.init_company(run_id, "acme-inc", {"name": "Acme Inc", "website": "acme.com"})
        store.update_node(run_id, "acme-inc", "mx_lookup", "completed", output={"valid": True})
        store.set_gate(run_id, "acme-inc", "mx_pass", True)
    """

    DEFAULT_DB_PATH = ".state/pipeline.db"

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the state store with SQLite backend.

        Args:
            db_path: Path to SQLite database file. Defaults to .state/pipeline.db
                     Parent directories are created if they don't exist.
        """
        self.db_path = Path(db_path or self.DEFAULT_DB_PATH)
        self._lock = threading.Lock()

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database schema
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """
        Create a new database connection.

        Uses row_factory for dict-like row access.
        """
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        """Create database tables and indexes if they don't exist."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Node state table - tracks individual node executions
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS node_state (
                        run_id TEXT NOT NULL,
                        company_key TEXT NOT NULL,
                        node_name TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        output_json TEXT,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        cost_usd REAL NOT NULL DEFAULT 0.0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, company_key, node_name)
                    )
                """)

                # Company state table - tracks overall company processing state
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS company_state (
                        run_id TEXT NOT NULL,
                        company_key TEXT NOT NULL,
                        company_name TEXT,
                        website TEXT,
                        overall_status TEXT NOT NULL DEFAULT 'pending',
                        mx_pass INTEGER,
                        icp_pass INTEGER,
                        webhook_pushed INTEGER NOT NULL DEFAULT 0,
                        total_cost_usd REAL NOT NULL DEFAULT 0.0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, company_key)
                    )
                """)

                # Indexes for fast queries on run_id and status
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_node_state_run_status
                    ON node_state(run_id, status)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_company_state_run_status
                    ON company_state(run_id, overall_status)
                """)

                conn.commit()
            finally:
                conn.close()

    def _now_iso(self) -> str:
        """Get current UTC timestamp in ISO format."""
        return datetime.now(timezone.utc).isoformat()

    def init_company(
        self,
        run_id: str,
        company_key: str,
        company_data: Dict[str, Any]
    ) -> None:
        """
        Initialize a company for processing in a run.

        Creates the company_state record. Safe to call multiple times;
        subsequent calls are no-ops if company already exists.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company (e.g., slug)
            company_data: Dictionary with company info (expects 'name', 'website')
        """
        now = self._now_iso()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO company_state
                    (run_id, company_key, company_name, website, overall_status,
                     webhook_pushed, total_cost_usd, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, 0.0, ?, ?)
                """, (
                    run_id,
                    company_key,
                    company_data.get('name') or company_data.get('company_name'),
                    company_data.get('website'),
                    now,
                    now
                ))
                conn.commit()
            finally:
                conn.close()

    def company_exists(self, run_id: str, company_key: str) -> bool:
        """
        Check if a company has been initialized for a run.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company

        Returns:
            True if company exists in company_state, False otherwise
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 1 FROM company_state
                    WHERE run_id = ? AND company_key = ?
                """, (run_id, company_key))
                return cursor.fetchone() is not None
            finally:
                conn.close()

    def get_node_state(
        self,
        run_id: str,
        company_key: str,
        node_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get the current state of a specific node for a company.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
            node_name: Name of the pipeline node

        Returns:
            Dictionary with node state if exists, None otherwise.
            Includes: status, output (parsed from JSON), attempts, cost_usd,
                     created_at, updated_at
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT status, output_json, attempts, cost_usd,
                           created_at, updated_at
                    FROM node_state
                    WHERE run_id = ? AND company_key = ? AND node_name = ?
                """, (run_id, company_key, node_name))

                row = cursor.fetchone()
                if row is None:
                    return None

                return {
                    'status': row['status'],
                    'output': json.loads(row['output_json']) if row['output_json'] else None,
                    'attempts': row['attempts'],
                    'cost_usd': row['cost_usd'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            finally:
                conn.close()

    def update_node(
        self,
        run_id: str,
        company_key: str,
        node_name: str,
        status: str,
        output: Optional[Any] = None,
        cost: float = 0.0
    ) -> None:
        """
        Update a node's state atomically.

        Creates the node_state record if it doesn't exist (upsert).
        Increments attempt counter on each update.
        Also updates total_cost_usd on company_state.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
            node_name: Name of the pipeline node
            status: New status (pending/running/completed/failed/skipped)
            output: Output data to store (will be JSON serialized)
            cost: Cost in USD for this node execution

        Raises:
            ValueError: If status is not a valid value
        """
        valid_statuses = {'pending', 'running', 'completed', 'failed', 'skipped'}
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

        now = self._now_iso()
        output_json = json.dumps(output) if output is not None else None

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Check if node exists to determine if this is insert or update
                cursor.execute("""
                    SELECT attempts, cost_usd FROM node_state
                    WHERE run_id = ? AND company_key = ? AND node_name = ?
                """, (run_id, company_key, node_name))

                existing = cursor.fetchone()

                if existing:
                    # Update existing node
                    new_attempts = existing['attempts'] + 1
                    cursor.execute("""
                        UPDATE node_state
                        SET status = ?, output_json = ?, attempts = ?,
                            cost_usd = cost_usd + ?, updated_at = ?
                        WHERE run_id = ? AND company_key = ? AND node_name = ?
                    """, (status, output_json, new_attempts, cost, now,
                          run_id, company_key, node_name))
                else:
                    # Insert new node
                    cursor.execute("""
                        INSERT INTO node_state
                        (run_id, company_key, node_name, status, output_json,
                         attempts, cost_usd, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """, (run_id, company_key, node_name, status, output_json,
                          cost, now, now))

                # Update company total cost
                if cost > 0:
                    cursor.execute("""
                        UPDATE company_state
                        SET total_cost_usd = total_cost_usd + ?, updated_at = ?
                        WHERE run_id = ? AND company_key = ?
                    """, (cost, now, run_id, company_key))

                conn.commit()
            finally:
                conn.close()

    def set_gate(
        self,
        run_id: str,
        company_key: str,
        gate_name: str,
        passed: bool
    ) -> None:
        """
        Set a gate value (mx_pass or icp_pass).

        Gates control flow through the pipeline. When a gate fails,
        subsequent nodes may be skipped.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
            gate_name: Gate identifier ('mx_pass' or 'icp_pass')
            passed: Whether the gate passed (True) or failed (False)

        Raises:
            ValueError: If gate_name is not 'mx_pass' or 'icp_pass'
        """
        valid_gates = {'mx_pass', 'icp_pass'}
        if gate_name not in valid_gates:
            raise ValueError(f"Invalid gate '{gate_name}'. Must be one of: {valid_gates}")

        now = self._now_iso()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    UPDATE company_state
                    SET {gate_name} = ?, updated_at = ?
                    WHERE run_id = ? AND company_key = ?
                """, (1 if passed else 0, now, run_id, company_key))
                conn.commit()
            finally:
                conn.close()

    def get_gate(
        self,
        run_id: str,
        company_key: str,
        gate_name: str
    ) -> Optional[bool]:
        """
        Get a gate value.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
            gate_name: Gate identifier ('mx_pass' or 'icp_pass')

        Returns:
            True if gate passed, False if failed, None if not yet evaluated

        Raises:
            ValueError: If gate_name is not 'mx_pass' or 'icp_pass'
        """
        valid_gates = {'mx_pass', 'icp_pass'}
        if gate_name not in valid_gates:
            raise ValueError(f"Invalid gate '{gate_name}'. Must be one of: {valid_gates}")

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT {gate_name} FROM company_state
                    WHERE run_id = ? AND company_key = ?
                """, (run_id, company_key))

                row = cursor.fetchone()
                if row is None:
                    return None

                value = row[gate_name]
                if value is None:
                    return None
                return bool(value)
            finally:
                conn.close()

    def get_resumable_companies(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Get companies that can be resumed (not completed or failed).

        Returns companies with status 'pending' or 'running' for the given run.

        Args:
            run_id: Unique identifier for the pipeline run

        Returns:
            List of dictionaries with company_key, company_name, website,
            overall_status, mx_pass, icp_pass, total_cost_usd
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT company_key, company_name, website, overall_status,
                           mx_pass, icp_pass, total_cost_usd
                    FROM company_state
                    WHERE run_id = ? AND overall_status NOT IN ('completed', 'failed')
                    ORDER BY created_at
                """, (run_id,))

                results = []
                for row in cursor.fetchall():
                    results.append({
                        'company_key': row['company_key'],
                        'company_name': row['company_name'],
                        'website': row['website'],
                        'overall_status': row['overall_status'],
                        'mx_pass': None if row['mx_pass'] is None else bool(row['mx_pass']),
                        'icp_pass': None if row['icp_pass'] is None else bool(row['icp_pass']),
                        'total_cost_usd': row['total_cost_usd']
                    })
                return results
            finally:
                conn.close()

    def set_company_status(
        self,
        run_id: str,
        company_key: str,
        status: str
    ) -> None:
        """
        Update the overall status of a company.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
            status: New status (pending/running/completed/failed)

        Raises:
            ValueError: If status is not a valid value
        """
        valid_statuses = {'pending', 'running', 'completed', 'failed'}
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

        now = self._now_iso()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE company_state
                    SET overall_status = ?, updated_at = ?
                    WHERE run_id = ? AND company_key = ?
                """, (status, now, run_id, company_key))
                conn.commit()
            finally:
                conn.close()

    def get_run_stats(self, run_id: str) -> Dict[str, Any]:
        """
        Get summary statistics for a pipeline run.

        Args:
            run_id: Unique identifier for the pipeline run

        Returns:
            Dictionary with:
            - total_companies: Total number of companies in run
            - status_counts: Dict mapping status -> count
            - mx_passed: Count of companies that passed MX validation
            - mx_failed: Count of companies that failed MX validation
            - icp_passed: Count of companies that passed ICP scoring
            - icp_failed: Count of companies that failed ICP scoring
            - total_cost_usd: Sum of all costs for the run
            - webhooks_pushed: Count of companies with webhook_pushed=1
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Get status counts
                cursor.execute("""
                    SELECT overall_status, COUNT(*) as count
                    FROM company_state
                    WHERE run_id = ?
                    GROUP BY overall_status
                """, (run_id,))

                status_counts = {}
                total_companies = 0
                for row in cursor.fetchall():
                    status_counts[row['overall_status']] = row['count']
                    total_companies += row['count']

                # Get gate stats and totals
                cursor.execute("""
                    SELECT
                        SUM(CASE WHEN mx_pass = 1 THEN 1 ELSE 0 END) as mx_passed,
                        SUM(CASE WHEN mx_pass = 0 THEN 1 ELSE 0 END) as mx_failed,
                        SUM(CASE WHEN icp_pass = 1 THEN 1 ELSE 0 END) as icp_passed,
                        SUM(CASE WHEN icp_pass = 0 THEN 1 ELSE 0 END) as icp_failed,
                        SUM(total_cost_usd) as total_cost,
                        SUM(CASE WHEN webhook_pushed = 1 THEN 1 ELSE 0 END) as webhooks_pushed
                    FROM company_state
                    WHERE run_id = ?
                """, (run_id,))

                row = cursor.fetchone()

                return {
                    'total_companies': total_companies,
                    'status_counts': status_counts,
                    'mx_passed': row['mx_passed'] or 0,
                    'mx_failed': row['mx_failed'] or 0,
                    'icp_passed': row['icp_passed'] or 0,
                    'icp_failed': row['icp_failed'] or 0,
                    'total_cost_usd': row['total_cost'] or 0.0,
                    'webhooks_pushed': row['webhooks_pushed'] or 0
                }
            finally:
                conn.close()

    def mark_webhook_pushed(self, run_id: str, company_key: str) -> None:
        """
        Mark a company as having its webhook pushed.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company
        """
        now = self._now_iso()

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE company_state
                    SET webhook_pushed = 1, updated_at = ?
                    WHERE run_id = ? AND company_key = ?
                """, (now, run_id, company_key))
                conn.commit()
            finally:
                conn.close()

    def get_all_node_states(
        self,
        run_id: str,
        company_key: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get all node states for a company.

        Useful for debugging and resumption logic.

        Args:
            run_id: Unique identifier for the pipeline run
            company_key: Unique identifier for the company

        Returns:
            Dictionary mapping node_name -> node state dict
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT node_name, status, output_json, attempts, cost_usd,
                           created_at, updated_at
                    FROM node_state
                    WHERE run_id = ? AND company_key = ?
                """, (run_id, company_key))

                results = {}
                for row in cursor.fetchall():
                    results[row['node_name']] = {
                        'status': row['status'],
                        'output': json.loads(row['output_json']) if row['output_json'] else None,
                        'attempts': row['attempts'],
                        'cost_usd': row['cost_usd'],
                        'created_at': row['created_at'],
                        'updated_at': row['updated_at']
                    }
                return results
            finally:
                conn.close()
