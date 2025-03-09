import os
import time
import logging

def file_deletion_task(task_name, directory, age_days, formats):
    """
    Deletes files in the specified directory older than `age_days` and matching `formats`.
    """
    try:
        # Calculate the cutoff time (current time - age_days)
        cutoff_time = time.time() - (age_days * 86400)  # 86400 seconds in a day

        # Log the start of the task
        logging.info(f"Starting file deletion task '{task_name}' in directory: {directory}")

        deleted_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_extension = os.path.splitext(file)[1].lower()

                # Check if the file matches the specified formats
                if file_extension in formats:
                    # Get the file's last modified time
                    file_mtime = os.path.getmtime(file_path)

                    # Delete the file if it's older than the cutoff time
                    if file_mtime < cutoff_time:
                        os.remove(file_path)
                        deleted_files.append(file_path)
                        logging.info(f"Deleted file: {file_path}")

        # Log the results
        if deleted_files:
            log_to_mongodb(task_name, {"deleted_files": deleted_files}, "Files deleted")
        else:
            logging.info(f"No files deleted in task '{task_name}'.")
            log_to_mongodb(task_name, {}, "No files deleted")

    except Exception as e:
        logging.error(f"File deletion task '{task_name}' failed: {e}")
        log_to_mongodb(task_name, {"directory": directory, "age_days": age_days, "formats": formats}, f"Error: {e}", level="ERROR")
def add_file_deletion_task(interval, unit, directory, age_days, formats):
    """
    Adds a new file deletion task to the scheduler.
    """
    tasks = load_tasks()
    task_name = f"file_deletion_task_{len(tasks) + 1}"

    new_task_details = {
        "interval": interval,
        "unit": unit,
        "directory": directory,
        "age_days": age_days,
        "formats": formats
    }

    # Check for duplicates
    for existing_task_details in tasks.values():
        if new_task_details == existing_task_details:
            print("⚠️ Task with the same interval and details already exists.")
            return

    tasks[task_name] = new_task_details
    save_tasks(tasks)

    trigger = IntervalTrigger(**{unit: interval})
    scheduler.add_job(file_deletion_task, trigger, args=[task_name, directory, age_days, formats], id=task_name)

    print(f"✅ File deletion task '{task_name}' added successfully.")
def list_file_deletion_tasks():
    """
    Lists all scheduled file deletion tasks.
    """
    tasks = load_tasks()
    if not tasks:
        print("⚠️ No scheduled file deletion tasks found.")
        return

    print("\n📌 Scheduled File Deletion Tasks:")
    for task_name, details in tasks.items():
        if "directory" in details:  # Filter file deletion tasks
            print(f"🔹 {task_name} - Every {details['interval']} {details['unit']}")
            print(f"   Directory: {details['directory']}")
            print(f"   Age: {details['age_days']} days")
            print(f"   Formats: {', '.join(details['formats'])}")
def remove_file_deletion_task(task_name):
    """
    Removes a scheduled file deletion task.
    """
    tasks = load_tasks()
    if task_name not in tasks:
        print(f"⚠️ Task '{task_name}' not found.")
        return

    del tasks[task_name]
    save_tasks(tasks)

    try:
        scheduler.remove_job(task_name)
        print(f"✅ Task '{task_name}' removed successfully.")
    except Exception:
        print(f"⚠️ Task '{task_name}' was not running but removed from saved tasks.")
# Update the CLI argument parser
parser.add_argument("--add-file-deletion", type=int, help="Add a new file deletion task with interval")
parser.add_argument("--directory", type=str, help="Directory to scan for files")
parser.add_argument("--age-days", type=int, help="Delete files older than this number of days")
parser.add_argument("--formats", nargs="*", default=[], help="List of file formats to delete (e.g., .log .tmp)")

# Update the CLI logic
if args.add_file_deletion:
    if not all((args.unit, args.directory, args.age_days, args.formats)):
        print("⚠️ Please provide --unit, --directory, --age-days, and --formats.")
        exit(1)
    add_file_deletion_task(args.add_file_deletion, args.unit, args.directory, args.age_days, args.formats)
def load_and_schedule_tasks():
    """
    Loads and schedules all tasks from the task file.
    """
    tasks = load_tasks()
    for task_name, details in tasks.items():
        if "directory" in details:  # File deletion task
            trigger = IntervalTrigger(**{details["unit"]: details["interval"]})
            scheduler.add_job(
                file_deletion_task,
                trigger,
                args=[task_name, details["directory"], details["age_days"], details["formats"]],
                id=task_name,
            )
def start_scheduler():
    """Runs the scheduler in a separate thread."""
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()

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
