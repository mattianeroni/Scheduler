from __future__ import annotations

import base64
import io
import pathlib

import matplotlib.pyplot as plt
import polars as pl

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem


def plot_solution(assignments_df: pl.DataFrame, tasks_df: pl.DataFrame, output_path: pathlib.Path | str) -> pathlib.Path:
    """Create an HTML schedule plot from solution dataframes.

    Resources are on the vertical axis, time is on the horizontal axis, and each
    assigned task is represented as a colored bar. Tasks assigned to multiple
    resources are drawn in every corresponding resource row.
    """
    output_path = pathlib.Path(output_path)

    if "assignment_solution" not in assignments_df.columns:
        raise ValueError("assignments_df must contain column 'assignment_solution'")
    if "task_name" not in assignments_df.columns or "resource_name" not in assignments_df.columns:
        raise ValueError("assignments_df must contain columns 'task_name' and 'resource_name'")
    for col in ("task_name", "start_solution", "end_solution"):
        if col not in tasks_df.columns:
            raise ValueError(f"tasks_df must contain column '{col}'")

    assigned = assignments_df.filter(pl.col("assignment_solution") == 1)
    if assigned.is_empty():
        raise ValueError("No selected assignments found in assignments_df")

    plot_df = assigned.join(
        tasks_df.select(["task_name", "start_solution", "end_solution"]),
        on="task_name",
        how="inner"
    )

    if plot_df.is_empty():
        raise ValueError("No matching tasks found between assignments_df and tasks_df")

    plot_df = plot_df.with_columns(
        pl.col("start_solution").round(0).cast(pl.Int64),
        pl.col("end_solution").round(0).cast(pl.Int64),
    )

    resources = plot_df.select("resource_name").unique().to_series().to_list()
    resources = [str(r) for r in resources]
    y_positions = {resource: idx for idx, resource in enumerate(resources)}

    task_names = plot_df.select("task_name").unique().to_series().to_list()
    task_names = [str(t) for t in task_names]
    cmap = plt.get_cmap("tab20")
    colors = {task: cmap(i % cmap.N) for i, task in enumerate(task_names)}

    fig, ax = plt.subplots(figsize=(12, max(4, len(resources) * 0.6)))

    for row in plot_df.iter_rows(named=True):
        task_name = str(row["task_name"])
        resource_name = str(row["resource_name"])
        start = int(row["start_solution"])
        end = int(row["end_solution"])
        width = max(1, end - start)
        y = y_positions[resource_name]

        ax.barh(
            y,
            width,
            left=start,
            height=0.8,
            color=colors[task_name],
            edgecolor="black",
            alpha=0.85,
        )
        ax.text(
            start + width / 2,
            y,
            task_name,
            va="center",
            ha="center",
            color="white",
            fontsize=8,
            clip_on=True,
        )

    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(resources)
    ax.set_xlabel("Time")
    ax.set_ylabel("Resource")
    ax.set_title("Schedule")
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.invert_yaxis()
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)

    img_base64 = base64.b64encode(buffer.read()).decode("ascii")
    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>Schedule Plot</title>
</head>
<body>
<h1>Schedule Plot</h1>
<p>Resources are displayed on the vertical axis. Tasks are shown as colored bars over time.</p>
<img src=\"data:image/png;base64,{img_base64}\" alt=\"Schedule plot\" style=\"max-width:100%;height:auto;\" />
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path
