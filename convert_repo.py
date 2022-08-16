import os

cleanup_files = [
    {"name": "README.md", "replace": "README_replace.md"},
    {"name": "week_1/project/week_1.py", "replace": "week_1/project/week_1_replace.py"},
    {"name": "week_1/project/week_1_challenge.py", "replace": "week_1/project/week_1_challenge_replace.py"},
    {"name": "week_2/dagster_ucr/project/week_2.py", "replace": "week_2/dagster_ucr/project/week_2_replace.py"},
    {"name": "week_2/dagster_ucr/resources.py", "replace": "week_2/dagster_ucr/resources_replace.py"},
    {"name": "week_3/project/week_3.py", "replace": "week_3/project/week_3_replace.py"},
    {"name": "week_4/project/week_4.py", "replace": "week_4/project/week_4_replace.py"},
    {"name": ".flake8"},
    {"name": "Taskfile.yml"},
    {"name": ".github/workflows/ci.yml"},
]

for replace_file in cleanup_files:
    os.remove(replace_file["name"])

    if replace_file.get("replace"):
        os.rename(replace_file["replace"], replace_file["name"])
