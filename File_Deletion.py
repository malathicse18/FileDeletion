import os
import time
import logging
import json
import threading
import argparse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Logging Configuration
logging.basicConfig(
    filename="file_deletion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Persistent Task Storage File
TASKS_FILE = "tasks.json"

# Initialize Scheduler
scheduler = BackgroundScheduler()

def log_to_mongodb(task_name, details, status, level="INFO"):
    """Dummy function to simulate MongoDB logging."""
    logging.info(f"[MongoDB Log] Task: {task_name} | Status: {status} | Details: {details}")

def load_tasks():
    """Load tasks from the JSON file."""
    try:
        with open(TASKS_FILE, "r") as file:
            tasks = json.load(file)
            return tasks if isinstance(tasks, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_tasks(tasks):
    """Save tasks to the JSON file."""
    with open(TASKS_FILE, "w") as file:
        json.dump(tasks, file, indent=4)

def file_deletion_task(task_name, directory, age_days, formats):
    """Deletes files in the specified directory older than `age_days` and matching `formats`."""
    try:
        cutoff_time = time.time() - (age_days * 86400)  # 86400 seconds in a day
        logging.info(f"Starting file deletion task '{task_name}' in directory: {directory}")
        
        deleted_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_extension = os.path.splitext(file)[1].lower()

                if file_extension in formats and os.path.getmtime(file_path) < cutoff_time:
                    os.remove(file_path)
                    deleted_files.append(file_path)
                    logging.info(f"Deleted file: {file_path}")

        if deleted_files:
            log_to_mongodb(task_name, {"deleted_files": deleted_files}, "Files deleted")
        else:
            logging.info(f"No files deleted in task '{task_name}'.")
            log_to_mongodb(task_name, {}, "No files deleted")
    except Exception as e:
        logging.error(f"File deletion task '{task_name}' failed: {e}")
        log_to_mongodb(task_name, {"directory": directory, "age_days": age_days, "formats": formats}, f"Error: {e}", level="ERROR")

def add_file_deletion_task(interval, unit, directory, age_days, formats):
    """Adds a new file deletion task to the scheduler."""
    tasks = load_tasks()  # Load existing tasks
    task_name = f"file_deletion_task_{len(tasks) + 1}"  # Generate a unique task name

    new_task_details = {
        "interval": interval,
        "unit": unit,
        "directory": directory,
        "age_days": age_days,
        "formats": formats
    }

    # Check for duplicates
    for existing_task_name, existing_task_details in tasks.items():
        if (
            existing_task_details["interval"] == interval
            and existing_task_details["unit"] == unit
            and existing_task_details["directory"] == directory
            and existing_task_details["age_days"] == age_days
            and set(existing_task_details["formats"]) == set(formats)
        ):
            print("⚠️ Task with the same interval and details already exists.")
            return

    # Add the new task to the tasks dictionary
    tasks[task_name] = new_task_details

    # Save the updated tasks dictionary to the JSON file
    save_tasks(tasks)

    # Schedule the task
    trigger = IntervalTrigger(**{unit: interval})
    scheduler.add_job(file_deletion_task, trigger, args=[task_name, directory, age_days, formats], id=task_name)

    logging.info(f"Added task to delete files in '{directory}' every {interval} {unit}.")
    log_to_mongodb("add_file_deletion_task", {"directory": directory, "interval": interval, "unit": unit, "age_days": age_days, "formats": formats}, "Task added")
    print(f"✅ Task '{task_name}' added successfully.")

def list_file_deletion_tasks():
    """Lists all scheduled file deletion tasks."""
    tasks = load_tasks()
    if not tasks:
        print("⚠️ No scheduled tasks found.")
        return
    
    print("\n📌 Scheduled File Deletion Tasks:")
    for task_name, details in tasks.items():
        print(f"🔹 {task_name} - Every {details['interval']} {details['unit']}")
        print(f"   Directory: {details['directory']}")
        print(f"   Age: {details['age_days']} days")
        print(f"   Formats: {', '.join(details['formats'])}")

def remove_file_deletion_task(task_name):
    """Removes a scheduled file deletion task."""
    tasks = load_tasks()
    
    # Check if the task exists in the tasks dictionary
    if task_name not in tasks:
        print(f"⚠️ Task '{task_name}' not found.")
        return

    # Remove the task from the tasks dictionary
    del tasks[task_name]
    save_tasks(tasks)

    # Remove the task from the scheduler
    if scheduler.get_job(task_name):  # Check if the job exists in the scheduler
        scheduler.remove_job(task_name)
        logging.info(f"Removed task '{task_name}'.")
        log_to_mongodb("remove_file_deletion_task", {"task_id": task_name}, "Task removed")
        print(f"✅ Task '{task_name}' removed successfully.")
    else:
        logging.warning(f"Task '{task_name}' not found in the scheduler.")
        log_to_mongodb("remove_file_deletion_task", {"task_id": task_name}, "Task not found in scheduler", level="WARNING")
        print(f"⚠️ Task '{task_name}' was not running but removed from saved tasks.")

def load_and_schedule_tasks():
    """Loads and schedules all tasks from the task file."""
    tasks = load_tasks()
    for task_name, details in tasks.items():
        trigger = IntervalTrigger(**{details["unit"]: details["interval"]})
        scheduler.add_job(file_deletion_task, trigger, args=[task_name, details["directory"], details["age_days"], details["formats"]], id=task_name)

def start_scheduler():
    """Runs the scheduler in a separate thread."""
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()

# CLI Argument Parsing
parser = argparse.ArgumentParser(description="File Deletion Scheduler")
parser.add_argument("--add", type=int, help="Add a new task with interval")
parser.add_argument("--unit", type=str, choices=["seconds", "minutes", "hours", "days"], help="Time unit for the interval")
parser.add_argument("--directory", type=str, help="Directory to organize")
parser.add_argument("--age-days", type=int, help="Delete files older than this number of days")
parser.add_argument("--formats", nargs="*", default=[], help="List of file formats to delete (e.g., .log .tmp)")
parser.add_argument("--list", action="store_true", help="List all scheduled tasks")
parser.add_argument("--remove", type=str, help="Remove a scheduled task by ID")

args = parser.parse_args()

# CLI Execution Logic
if args.add:
    if not all((args.unit, args.directory, args.age_days, args.formats)):
        print("⚠️ Please provide --unit, --directory, --age-days, and --formats.")
        exit(1)
    add_file_deletion_task(args.add, args.unit, args.directory, args.age_days, args.formats)
elif args.list:
    list_file_deletion_tasks()
elif args.remove:
    remove_file_deletion_task(args.remove)

# Load and schedule tasks before parsing commands
load_and_schedule_tasks()

# Start the scheduler thread only if no other command is given
if not (args.add or args.list or args.remove):
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()
