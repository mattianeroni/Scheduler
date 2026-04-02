from __future__ import annotations
"""
Generate random problem instances for the school scheduling problem.

Produces:
  - config.toml
  - resources.csv  
  - tasks.csv  
  - groups.csv
  - resource_assignments.csv
  - group_assignments.csv
"""
import math
import random
import pathlib
import logging
import polars as pl

from scheduler.utils import setup_logging

logger = logging.getLogger(__name__)

SUBJECTS = [
    "Math", "Physics", "Chemistry", "Biology", "History",
    "Geography", "English", "Art", "Music", "PE",
    "CS", "Philosophy", "Economics", "Literature", "Spanish",
]

def _get_lognorm_params(target_mean: float, target_std: float):
    phi = math.sqrt(target_std**2 + target_mean**2)
    mu = math.log(target_mean**2 / phi)
    sigma = math.sqrt(math.log(phi**2 / target_mean**2))
    return mu, sigma

def generate_instance(
    output_path: str,
    timehorizon: int = 160, # We are assuming 40 hours per week and min timeslot of 15 mins (0.25 hours)
    n_days: int = 5,
    n_resources: int = 10,
    n_classes: int = 30,
    n_meetings: int = 5,
    n_groups: int = 4,
    classes_durations: tuple[int, ...] = (4, 8),
    meetings_durations: tuple[int, ...] = (1, 2, 4),
    groups_size: int = 4,
    rigid_ratio: float = 0.3,
    forced_ratio: float = 0.1,
    require_all_group_ratio: float = 0.1,
    avg_eligible_resources: int = 3,
    avg_eligible_groups: int = 3,
    seed: int | None = None,
):
    output_path = pathlib.Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    setup_logging(output_path)

    if seed is not None:
        random.seed(seed)

    # Resources
    resource_names = [f"Prof_{i}" for i in range(n_resources)]
    resource_capacitites = [float(timehorizon // 2)] * n_resources
    resources = pl.DataFrame({"name": resource_names, "capacity": resource_capacitites})

    # Tasks
    tasks_rows: list[dict] = []
    subject_to_number = {i: 0 for i in SUBJECTS}
    for _ in range(n_classes):
        subject = random.choice(SUBJECTS)
        name = f"{subject}_{subject_to_number.get(subject)}"
        subject_to_number[subject] += 1
        duration = random.choice(classes_durations)
        is_rigid = random.random() < rigid_ratio
        if is_rigid:
            day = random.randint(0, n_days - 1)
            timeslots_per_day = timehorizon // n_days
            slot = random.randint(0, timeslots_per_day - duration)
            start = (day * timeslots_per_day) + slot
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

    # Meetings
    for i in range(n_meetings):
        name = f"Meeting_{i}"
        duration = random.choice(meetings_durations)
        is_rigid = random.random() < rigid_ratio
        if is_rigid:
            day = random.randint(0, n_days - 1)
            timeslots_per_day = timehorizon // n_days
            slot = random.randint(0, timeslots_per_day - duration)
            start = (day * timeslots_per_day) + slot
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

    # Groups
    if groups_size <= 0:
        groups = pl.DataFrame({"resource_name": [], "group_name": []})
    else:
        group_rows: list[dict] = []
        for i in range(n_groups):
            g_name = f"Group_{i}"
            members = random.sample(resource_names, min(groups_size, n_resources))
            for m in members:
                group_rows.append({"resource_name": m, "group_name": g_name})
        groups = pl.DataFrame(group_rows)

    # Resources to tasks assignment
    task_names = tasks["name"].unique().to_list()
    ra_rows: list[dict] = []
    mu, sigma = _get_lognorm_params(avg_eligible_resources, avg_eligible_resources * 0.25)
    for t_name in task_names:
        n_eligible = round(random.lognormvariate(mu, sigma))
        eligible_resources = random.sample(resource_names, min(n_resources, n_eligible))
        for r_name in eligible_resources:
            is_forced = random.random() < forced_ratio
            ra_rows.append({
                "task_name": t_name,
                "resource_name": r_name,
                "type": "forced" if is_forced else "relaxed",
            })
    resource_assignments = pl.DataFrame(ra_rows)

    # Groups to Meetings assignment
    if n_meetings <= 0 or n_groups <= 0:
        group_assignments = pl.DataFrame({"task_name": [], "group_name": [], "require_all_group": []})
    else:
        ga_rows: list[dict] = []
        meeting_names = tasks.filter(pl.col.name.str.starts_with("Meeting"))["name"].unique().to_list()
        group_names = groups["group_name"].unique().to_list()
        mu, sigma = _get_lognorm_params(avg_eligible_groups, avg_eligible_groups * 0.25)
        for m_name in meeting_names:
            n_eligible = round(random.lognormvariate(mu, sigma))
            eligible_groups = random.sample(group_names, min(n_groups, n_eligible))
            for g_name in eligible_groups:
                require_all = random.random() < require_all_group_ratio
                ga_rows.append({
                    "task_name": m_name,
                    "group_name": g_name,
                    "require_all_group": require_all,
                })
        group_assignments = pl.DataFrame(ga_rows)

    resources.write_csv(output_path / "resources.csv")
    tasks.write_csv(output_path / "tasks.csv")
    groups.write_csv(output_path / "groups.csv")
    resource_assignments.write_csv(output_path / "resource_assignments.csv")
    group_assignments.write_csv(output_path / "group_assignments.csv")

    logger.info(f"Instance written to {output_path.as_posix()}/")
    logger.info(f"  Resources: {resources.height}")
    logger.info(f"  Tasks: {tasks.height} ({n_classes} classes + {n_meetings} meetings)")
    logger.info(f"  Groups: {groups.unique('group_name').height}")
    logger.info(f"  Resource assignments: {resource_assignments.height}")
    logger.info(f"  Group assignments: {group_assignments.height}")


if __name__ == "__main__":
    generate_instance(
        output_path = "./test/test_instance",
        timehorizon = 160,
        n_days = 5,
        n_resources = 10,
        n_classes = 30,
        n_meetings = 5,
        n_groups = 4,
        classes_durations = (4, 8),
        meetings_durations = (1, 2, 4),
        groups_size = 4,
        rigid_ratio = 0.3,
        forced_ratio = 0.1,
        require_all_group_ratio = 0.1,
        avg_eligible_resources = 3,
        avg_eligible_groups = 3,
        seed = None,
    )
