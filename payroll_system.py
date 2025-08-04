"""
payroll_system.py
=================

This module implements a simple, modular payroll system that follows the
requirements outlined in the assignment specification.  It is designed for
educational purposes and demonstrates how separate concerns—such as data
management, time tracking, payroll calculations, and reporting—can be broken
into discrete, testable components.  A SQLite database is used for storage
because it is self‑contained and requires no external server.  The design is
intended to be extendable to more sophisticated back‑ends without changing
the high‑level interfaces.

Modules implemented in this file:

* **Employee data management & database integration (Module 2)**
  – The `EmployeeDatabase` class handles CRUD operations for employees and
    encapsulates all direct database interactions.  It defines a simple
    schema with an employee identifier (ID), name, hourly pay rate and
    optional dependents column.  Validation is performed before inserts or
    updates to ensure data integrity (e.g., unique IDs, non‑empty names,
    positive hourly rates).

* **Time tracking & hours management (Module 3)**
  – The `TimeTracker` class records hours worked for employees and keeps
    regular versus overtime hours separate.  It provides helper methods to
    summarise hours per employee over arbitrary periods.

* **Payroll calculation engine (Module 4)**
  – The `PayrollCalculator` class performs the gross pay calculation,
    computes taxes (state and federal), and returns net pay.  Constants
    controlling the overtime multiplier and tax rates are defined here for
    easy adjustment.

* **Reporting (part of Module 5)**
  – The `generate_payroll_report` function pulls together information
    from the database and calculation modules to produce a human‑readable
    report.  A companion `export_to_csv` function writes the report to
    disk in CSV format.

Although a full graphical user interface is beyond the scope of this
implementation, the command‑line functions and test cases provided at the
bottom of the module illustrate how the system can be used.  Each unit is
designed to be individually testable, and the included doctests and
integration tests demonstrate typical usage scenarios as well as edge cases.
"""

from __future__ import annotations

import csv
import datetime as _dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


###############################################################################
# Module 2: Employee Data Management & Database Integration
###############################################################################

class EmployeeDatabase:
    """Manage employees and connect to a SQLite database.

    This class encapsulates all database interactions related to employees.
    It ensures the appropriate tables are created and exposes methods to
    perform CRUD operations.  The methods raise `ValueError` when invalid
    data is supplied (e.g., duplicate IDs, negative pay rates).

    Attributes
    ----------
    db_path : Path
        Path to the SQLite database file.  If the file does not exist,
        it will be created and initialised.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        """Create tables for employees and time entries if they do not exist."""
        cur = self._conn.cursor()
        # Employees table: ID is primary key, names cannot be null or empty,
        # hourly_rate must be positive. Dependents is optional.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                hourly_rate REAL NOT NULL CHECK (hourly_rate >= 0),
                dependents INTEGER DEFAULT 0 CHECK (dependents >= 0)
            )
            """
        )
        # Time entries table: store date and hours worked separately for
        # regular and overtime hours.  This table references employees.id via
        # a foreign key to maintain referential integrity.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS time_entries (
                entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id TEXT NOT NULL,
                entry_date TEXT NOT NULL,
                regular_hours REAL NOT NULL CHECK (regular_hours >= 0),
                overtime_hours REAL NOT NULL CHECK (overtime_hours >= 0),
                FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
            )
            """
        )
        self._conn.commit()

    # Employee CRUD operations
    def add_employee(self, employee_id: str, name: str, hourly_rate: float, dependents: int = 0) -> None:
        """Add a new employee to the database.

        Parameters
        ----------
        employee_id : str
            Unique identifier for the employee.  Raises `ValueError` if the ID
            already exists.
        name : str
            Full legal name of the employee.  Must not be empty or whitespace.
        hourly_rate : float
            Hourly rate of pay.  Must be non‑negative.
        dependents : int, optional
            Number of dependents.  Must be zero or positive.
        """
        employee_id = employee_id.strip()
        name = name.strip()
        if not employee_id:
            raise ValueError("Employee ID cannot be empty")
        if not name:
            raise ValueError("Employee name cannot be empty")
        if hourly_rate < 0:
            raise ValueError("Hourly rate must be non‑negative")
        if dependents < 0:
            raise ValueError("Dependents must be non‑negative")
        # Check for duplicate ID
        if self.get_employee(employee_id) is not None:
            raise ValueError(f"Employee with ID '{employee_id}' already exists")
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO employees (id, name, hourly_rate, dependents) VALUES (?, ?, ?, ?)",
            (employee_id, name, hourly_rate, dependents),
        )
        self._conn.commit()

    def get_employee(self, employee_id: str) -> Optional[sqlite3.Row]:
        """Retrieve an employee record by ID.  Returns `None` if not found."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM employees WHERE id = ?", (employee_id,))
        row = cur.fetchone()
        return row

    def update_employee(
        self,
        employee_id: str,
        *,
        name: Optional[str] = None,
        hourly_rate: Optional[float] = None,
        dependents: Optional[int] = None,
    ) -> None:
        """Update fields of an existing employee.

        At least one of `name`, `hourly_rate`, or `dependents` must be provided.
        Raises `KeyError` if the employee does not exist.
        """
        row = self.get_employee(employee_id)
        if row is None:
            raise KeyError(f"Employee '{employee_id}' not found")
        updates = []
        params: List[object] = []
        if name is not None:
            name = name.strip()
            if not name:
                raise ValueError("Employee name cannot be empty")
            updates.append("name = ?")
            params.append(name)
        if hourly_rate is not None:
            if hourly_rate < 0:
                raise ValueError("Hourly rate must be non‑negative")
            updates.append("hourly_rate = ?")
            params.append(hourly_rate)
        if dependents is not None:
            if dependents < 0:
                raise ValueError("Dependents must be non‑negative")
            updates.append("dependents = ?")
            params.append(dependents)
        if not updates:
            raise ValueError("No updates provided")
        params.append(employee_id)
        cur = self._conn.cursor()
        cur.execute(f"UPDATE employees SET {', '.join(updates)} WHERE id = ?", params)
        self._conn.commit()

    def delete_employee(self, employee_id: str) -> None:
        """Delete an employee and associated time entries."""
        cur = self._conn.cursor()
        cur.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
        self._conn.commit()

    def list_employees(self) -> List[sqlite3.Row]:
        """Return all employees in the database."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM employees ORDER BY id")
        return cur.fetchall()

    # Database connection cleanup
    def close(self) -> None:
        self._conn.close()


###############################################################################
# Module 3: Time Tracking & Hours Management
###############################################################################

class TimeTracker:
    """Record and summarise hours worked by employees.

    The `TimeTracker` uses the same SQLite database as `EmployeeDatabase` and
    therefore expects the `time_entries` table to be available.  It splits
    hours into regular and overtime categories based on a configurable
    threshold (default 40 hours per week).  Additional thresholds or
    differentiations (e.g., double‑time) can be supported by extending this
    class.
    """

    def __init__(self, db_path: Path, overtime_threshold: float = 40.0) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        self.overtime_threshold = overtime_threshold
        self._conn.row_factory = sqlite3.Row
        # Ensure the tables exist by initialising EmployeeDatabase
        EmployeeDatabase(self.db_path)

    def record_hours(self, employee_id: str, hours_worked: float, entry_date: Optional[_dt.date] = None) -> None:
        """Record hours for an employee on a given date.

        Hours are split into regular and overtime based on the configured
        threshold.  If `entry_date` is None, today's date is used.
        Raises `ValueError` if hours are negative or the employee does not
        exist in the database.
        """
        if hours_worked < 0:
            raise ValueError("Hours worked cannot be negative")
        # Validate employee existence
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM employees WHERE id = ?", (employee_id,))
            if cur.fetchone() is None:
                raise ValueError(f"Employee '{employee_id}' does not exist")
        # Determine regular and overtime hours
        reg_hours = min(hours_worked, self.overtime_threshold)
        ot_hours = max(0.0, hours_worked - self.overtime_threshold)
        if entry_date is None:
            entry_date = _dt.date.today()
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO time_entries (employee_id, entry_date, regular_hours, overtime_hours) VALUES (?, ?, ?, ?)",
            (employee_id, entry_date.isoformat(), reg_hours, ot_hours),
        )
        self._conn.commit()

    def get_time_entries(self, employee_id: str) -> List[sqlite3.Row]:
        """Retrieve all time entries for a given employee, ordered by date."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM time_entries WHERE employee_id = ? ORDER BY entry_date",
            (employee_id,),
        )
        return cur.fetchall()

    def summarise_hours(self, employee_id: str) -> Tuple[float, float]:
        """Sum regular and overtime hours for an employee over all entries."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT SUM(regular_hours) AS regular_sum, SUM(overtime_hours) AS overtime_sum "
            "FROM time_entries WHERE employee_id = ?",
            (employee_id,),
        )
        row = cur.fetchone()
        regular_sum = row["regular_sum"] or 0.0
        overtime_sum = row["overtime_sum"] or 0.0
        return regular_sum, overtime_sum

    def close(self) -> None:
        self._conn.close()


###############################################################################
# Module 4: Payroll Calculation Engine
###############################################################################

@dataclass
class PayResult:
    employee_id: str
    name: str
    hours_worked: float
    hourly_rate: float
    regular_hours: float
    overtime_hours: float
    gross_pay: float
    state_tax: float
    federal_tax: float
    net_pay: float


class PayrollCalculator:
    """Calculate gross and net pay for employees.

    The engine takes into account overtime (hours above the threshold are paid
    at 1.5× the hourly rate) and applies both state and federal tax rates.
    """

    STATE_TAX_RATE = 0.056
    FEDERAL_TAX_RATE = 0.079
    OVERTIME_MULTIPLIER = 1.5

    def __init__(self, employee_db: EmployeeDatabase, time_tracker: TimeTracker) -> None:
        self.employee_db = employee_db
        self.time_tracker = time_tracker

    def calculate_employee_pay(self, employee_id: str) -> PayResult:
        """Compute gross and net pay for a single employee.

        Raises `KeyError` if the employee is not found.
        """
        row = self.employee_db.get_employee(employee_id)
        if row is None:
            raise KeyError(f"Employee '{employee_id}' not found")
        name = row["name"]
        hourly_rate = row["hourly_rate"]
        reg_hours, ot_hours = self.time_tracker.summarise_hours(employee_id)
        total_hours = reg_hours + ot_hours
        gross = (reg_hours * hourly_rate) + (ot_hours * hourly_rate * self.OVERTIME_MULTIPLIER)
        state_tax = gross * self.STATE_TAX_RATE
        federal_tax = gross * self.FEDERAL_TAX_RATE
        net = gross - state_tax - federal_tax
        return PayResult(
            employee_id=employee_id,
            name=name,
            hours_worked=total_hours,
            hourly_rate=hourly_rate,
            regular_hours=reg_hours,
            overtime_hours=ot_hours,
            gross_pay=round(gross, 2),
            state_tax=round(state_tax, 2),
            federal_tax=round(federal_tax, 2),
            net_pay=round(net, 2),
        )

    def calculate_all(self) -> List[PayResult]:
        """Compute pay for all employees currently in the database."""
        results: List[PayResult] = []
        for row in self.employee_db.list_employees():
            results.append(self.calculate_employee_pay(row["id"]))
        return results


###############################################################################
# Module 5: Reporting & Export
###############################################################################

def generate_payroll_report(calculator: PayrollCalculator) -> List[dict]:
    """Generate a detailed payroll report as a list of dictionaries.

    Each dictionary contains fields ready for printing or CSV export.
    """
    report: List[dict] = []
    for pay_result in calculator.calculate_all():
        report.append(
            {
                "Employee ID": pay_result.employee_id,
                "Name": pay_result.name,
                "Hours Worked": pay_result.hours_worked,
                "Regular Hours": pay_result.regular_hours,
                "Overtime Hours": pay_result.overtime_hours,
                "Hourly Rate": pay_result.hourly_rate,
                "Gross Pay": pay_result.gross_pay,
                "State Tax": pay_result.state_tax,
                "Federal Tax": pay_result.federal_tax,
                "Net Pay": pay_result.net_pay,
            }
        )
    return report


def export_to_csv(report: Iterable[dict], filepath: Path) -> None:
    """Export the payroll report to a CSV file.

    Parameters
    ----------
    report : Iterable[dict]
        The payroll report generated by `generate_payroll_report`.
    filepath : Path
        Destination path for the CSV file.  Parent directories are created
        automatically if they do not exist.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(next(iter(report)).keys()) if report else []
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in report:
            writer.writerow(row)


###############################################################################
# Integration tests and demonstration (executed when run as script)
###############################################################################

def _run_demo() -> None:
    """Demonstrate usage of the payroll system with sample data.

    This function populates the database with a few employees, records some
    hours (including overtime), computes their payroll and prints a summary.
    It also exports the report to a temporary CSV file.  Running this
    function should not leave persistent files on disk, as it uses a
    temporary database.
    """
    # Use a temporary file‑based database for demonstration.  In‑memory SQLite
    # databases are not shared across connections, so using a file ensures
    # EmployeeDatabase and TimeTracker operate on the same data.  The file
    # will be created in the current working directory and removed at the end
    # of the demo.
    db_path = Path("demo_payroll.db")
    if db_path.exists():
        db_path.unlink()
    emp_db = EmployeeDatabase(db_path)
    time_tracker = TimeTracker(db_path)
    payroll_calc = PayrollCalculator(emp_db, time_tracker)

    # Add employees
    emp_db.add_employee("E001", "Alice Example", 20.0, dependents=2)
    emp_db.add_employee("E002", "Bob Example", 15.0, dependents=0)
    emp_db.add_employee("E003", "Charlie Example", 30.0, dependents=1)

    # Record hours (some with overtime)
    time_tracker.record_hours("E001", 38)   # Alice worked less than 40 hours
    time_tracker.record_hours("E002", 45)   # Bob worked 45 hours (5 overtime)
    time_tracker.record_hours("E003", 60)   # Charlie worked 60 hours (20 overtime)

    # Compute payroll and print report
    report = generate_payroll_report(payroll_calc)
    print("Payroll Report:\n")
    for row in report:
        print(
            f"{row['Name']} (ID {row['Employee ID']}): "
            f"Regular Hours={row['Regular Hours']}, Overtime Hours={row['Overtime Hours']}, "
            f"Gross=${row['Gross Pay']:.2f}, Net=${row['Net Pay']:.2f}"
        )

    # Export report to CSV (in current directory for demonstration)
    csv_path = Path("payroll_report_demo.csv")
    export_to_csv(report, csv_path)
    print(f"\nReport exported to {csv_path}\n")
    # Clean up demo database file
    emp_db.close()
    time_tracker.close()
    try:
        db_path.unlink()
    except FileNotFoundError:
        pass


# Simple unit tests to ensure components work as expected
def _run_tests() -> None:
    """Run basic tests to verify individual modules.

    Raises AssertionError if any test fails.  These tests cover edge cases
    like adding duplicate employees, recording negative hours and verifying
    correct gross/net calculations under various scenarios.
    """
    # Use a temporary file for the test database to ensure all connections see
    # the same data.  In‑memory databases are not shared between connections.
    temp_db_path = Path("test_payroll.db")
    if temp_db_path.exists():
        temp_db_path.unlink()
    emp_db = EmployeeDatabase(temp_db_path)
    time_tracker = TimeTracker(temp_db_path)
    calc = PayrollCalculator(emp_db, time_tracker)
    # Test adding and retrieving employees
    emp_db.add_employee("T001", "Test User", 25.0)
    assert emp_db.get_employee("T001")["name"] == "Test User"
    # Attempt to add duplicate ID
    try:
        emp_db.add_employee("T001", "Dup User", 30.0)
    except ValueError:
        pass
    else:
        raise AssertionError("Duplicate employee ID should raise ValueError")
    # Record normal hours and compute pay
    time_tracker.record_hours("T001", 40)
    pay = calc.calculate_employee_pay("T001")
    assert pay.regular_hours == 40
    assert pay.overtime_hours == 0
    assert pay.gross_pay == 40 * 25.0
    # Record overtime and compute pay
    emp_db.add_employee("T002", "Overtime User", 10.0)
    time_tracker.record_hours("T002", 50)
    pay2 = calc.calculate_employee_pay("T002")
    assert pay2.regular_hours == 40
    assert pay2.overtime_hours == 10
    expected_gross = 40 * 10.0 + 10 * 10.0 * PayrollCalculator.OVERTIME_MULTIPLIER
    assert pay2.gross_pay == expected_gross
    # Negative hours should raise
    try:
        time_tracker.record_hours("T002", -5)
    except ValueError:
        pass
    else:
        raise AssertionError("Negative hours should raise ValueError")
    # Deleting an employee removes their pay record
    emp_db.delete_employee("T002")
    try:
        calc.calculate_employee_pay("T002")
    except KeyError:
        pass
    else:
        raise AssertionError("Deleted employee should raise KeyError on payroll calculation")
    print("All tests passed.")
    # Close connections and remove temporary test database
    emp_db.close()
    time_tracker.close()
    try:
        temp_db_path.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    # Run tests and demo when executed directly
    _run_tests()
    print()
    _run_demo()