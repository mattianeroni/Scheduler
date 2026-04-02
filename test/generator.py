"""
Generate random problem instances for the school scheduling problem.

Produces:
  - config.toml
  - resources.csv       (professors)
  - tasks.csv           (classes & meetings)
  - groups.csv          (resource groups)
  - resource_assignments.csv
  - group_assignments.csv
"""
from __future__ import annotations

import argparse
import os
import random
import tomllib

import polars as pl


# ── Defaults ────────────────────────────────────────────────────────────────

TIMESLOT_DURATION = 1          # 1 hour per slot
MEETING_DURATION  = 1          # meetings can also be 1 slot (override with 0.5 if 30-min slots)
WEEK_HOURS        = 40         # 5 days × 8 hours
HOURS_PER_DAY     = 8

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]
SUBJECTS = [
    "Math", "Physics", "Chemistry", "Biology", "History",
    "Geography", "English", "Art", "Music", "PE",
    "CS", "Philosophy", "Economics", "Literature", "Spanish",
]


def _day_offset(day_idx: int) -> int:
    return day_idx * HOURS_PER_DAY


# ── Generator ───────────────────────────────────────────────────────────────

def generate_instance(
    output_dir: str,
    n_resources: int = 10,
    n_classes: int = 30,
    n_meetings: int = 5,
    n_groups: int = 3,
    group_size_range: tuple[int, int] = (2, 4),
    rigid_ratio: float = 0.3,
    forced_ratio: float = 0.1,
    avg_eligible_resources: int = 3,
    allow_infeasible: bool = False,
    seed: int | None = None,
):
    """
    Parameters
    ----------
    output_dir : str
        Directory where CSV / TOML files are written.
    n_resources : int
        Number of professors.
    n_classes : int
        Number of class tasks (1-hour slots).
    n_meetings : int
        Number of meeting tasks (1-hour slots, can be rigid or fluid).
    n_groups : int
        Number of resource groups.
    group_size_range : tuple[int, int]
        Min / max professors per group.
    rigid_ratio : float
        Fraction of tasks that are rigid (fixed start/end).
    forced_ratio : float
        Fraction of assignments that are forced.
    avg_eligible_resources : int
        Average number of resources eligible per task.
    allow_infeasible : bool
        If True, may produce instances where overlap avoidance is impossible.
    seed : int | None
        Random seed for reproducibility.
    """
    if seed is not None:
        random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)

    # ── Resources ───────────────────────────────────────────────────────
    resource_names = [f"Prof_{i}" for i in range(n_resources)]
    capacity_per_resource = WEEK_HOURS if not allow_infeasible else random.randint(
        WEEK_HOURS // 2, WEEK_HOURS
    )
    resources = pl.DataFrame({
        "name": resource_names,
        "capacity": [
            float(capacity_per_resource if not allow_infeasible
                  else random.randint(WEEK_HOURS // 2, WEEK_HOURS))
            for _ in resource_names
        ],
    })

    # ── Tasks ───────────────────────────────────────────────────────────
    tasks_rows: list[dict] = []

    for i in range(n_classes):
        subject = random.choice(SUBJECTS)
        section = chr(ord("A") + i % 26)
        name = f"{subject}_{section}_{i}"
        duration = TIMESLOT_DURATION
        is_rigid = random.random() < rigid_ratio

        if is_rigid:
            day_idx = random.randint(0, 4)
            slot = random.randint(0, HOURS_PER_DAY - duration)
            start = _day_offset(day_idx) + slot
            end = start + duration
        else:
            start = None
            end = None

        tasks_rows.append({
            "name": name,
            "duration": duration,
            "start": start,
            "end": end,
            "type": "rigid" if is_rigid else "fluid",
        })

    for i in range(n_meetings):
        name = f"Meeting_{i}"
        duration = MEETING_DURATION
        is_rigid = random.random() < rigid_ratio

        if is_rigid:
            day_idx = random.randint(0, 4)
            slot = random.randint(0, HOURS_PER_DAY - duration)
            start = _day_offset(day_idx) + slot
            end = start + duration
        else:
            start = None
            end = None

        tasks_rows.append({
            "name": name,
            "duration": duration,
            "start": start,
            "end": end,
            "type": "rigid" if is_rigid else "fluid",
        })

    tasks = pl.DataFrame(tasks_rows).cast({"start": pl.Int64, "end": pl.Int64})

    # ── Groups ──────────────────────────────────────────────────────────
    group_rows: list[dict] = []
    for g in range(n_groups):
        size = random.randint(*group_size_range)
        members = random.sample(resource_names, min(size, n_resources))
        for m in members:
            group_rows.append({"resource_name": m, "group_name": g})

    groups = pl.DataFrame(group_rows)

    # ── Resource assignments ────────────────────────────────────────────
    task_names = [r["name"] for r in tasks_rows]
    ra_rows: list[dict] = []

    for t_name in task_names:
        n_eligible = max(1, min(
            n_resources,
            random.randint(
                max(1, avg_eligible_resources - 1),
                avg_eligible_resources + 1,
            ),
        ))
        eligible = random.sample(resource_names, n_eligible)
        for r_name in eligible:
            is_forced = random.random() < forced_ratio
            ra_rows.append({
                "task_name": t_name,
                "resource_name": r_name,
                "type": "forced" if is_forced else "relaxed",
            })

    resource_assignments = pl.DataFrame(ra_rows)

    # ── Group assignments ───────────────────────────────────────────────
    ga_rows: list[dict] = []
    meeting_names = [r["name"] for r in tasks_rows if r["name"].startswith("Meeting")]
    group_ids = list(range(n_groups))

    for m_name in meeting_names:
        g_id = random.choice(group_ids)
        require_all = random.choice([True, False])
        ga_rows.append({
            "task_name": m_name,
            "group_name": g_id,
            "require_all_group": require_all,
        })

    group_assignments = pl.DataFrame(ga_rows) if ga_rows else pl.DataFrame(
        schema={"task_name": pl.Utf8, "group_name": pl.Int64, "require_all_group": pl.Boolean}
    )

    # ── Config ──────────────────────────────────────────────────────────
    config_str = f'timehorizon = {WEEK_HOURS}\n'

    # ── Write ───────────────────────────────────────────────────────────
    resources.write_csv(os.path.join(output_dir, "resources.csv"))
    tasks.write_csv(os.path.join(output_dir, "tasks.csv"))
    groups.write_csv(os.path.join(output_dir, "groups.csv"))
    resource_assignments.write_csv(os.path.join(output_dir, "resource_assignments.csv"))
    group_assignments.write_csv(os.path.join(output_dir, "group_assignments.csv"))

    with open(os.path.join(output_dir, "config.toml"), "w") as f:
        f.write(config_str)

    print(f"Instance written to {output_dir}/")
    print(f"  Resources:            {resources.height}")
    print(f"  Tasks:                {tasks.height} ({n_classes} classes + {n_meetings} meetings)")
    print(f"  Groups:               {groups.unique('group_name').height}")
    print(f"  Resource assignments: {resource_assignments.height}")
    print(f"  Group assignments:    {group_assignments.height}")


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate a school-scheduling problem instance.")
    parser.add_argument("--output-dir", default="instance", help="Output directory")
    parser.add_argument("--n-resources", type=int, default=10)
    parser.add_argument("--n-classes", type=int, default=30)
    parser.add_argument("--n-meetings", type=int, default=5)
    parser.add_argument("--n-groups", type=int, default=3)
    parser.add_argument("--rigid-ratio", type=float, default=0.3)
    parser.add_argument("--forced-ratio", type=float, default=0.1)
    parser.add_argument("--avg-eligible", type=int, default=3)
    parser.add_argument("--allow-infeasible", action="store_true",
                        help="Allow instances where overlap avoidance may be impossible")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    generate_instance(
        output_dir=args.output_dir,
        n_resources=args.n_resources,
        n_classes=args.n_classes,
        n_meetings=args.n_meetings,
        n_groups=args.n_groups,
        rigid_ratio=args.rigid_ratio,
        forced_ratio=args.forced_ratio,
        avg_eligible_resources=args.avg_eligible,
        allow_infeasible=args.allow_infeasible,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
