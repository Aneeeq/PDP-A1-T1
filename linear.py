import csv
from datetime import datetime
from collections import defaultdict

# Record the script's start time
start_time = datetime.now()

# Determine the current year dynamically
current_year = datetime.now().year

# Load student data into a list of dictionaries
with open('data/students.csv', newline='', encoding='utf-8') as file:
    student_reader = csv.DictReader(file)
    student_records = list(student_reader)

# Load fee records and group them by Student ID
fee_records_by_student = defaultdict(list)
with open('data/student_fees.csv', newline='', encoding='utf-8') as file:
    fee_reader = csv.DictReader(file)
    for record in fee_reader:
        try:
            if record["Payment Status"].strip().lower() == "paid":
                # Parse "Month Day" into a full date by appending the current year
                payment_date = datetime.strptime(f"{record['Payment Date']} {current_year}", "%B %d %Y")
                student_id = record["Student ID"].strip()
                fee_records_by_student[student_id].append(payment_date)
        except ValueError as err:
            print(f"Invalid date format encountered: {record['Payment Date']}. Skipping. Error: {err}")

# Debugging: Print a small sample of the grouped fee data
print("Sample Fee Records by Student:", dict(list(fee_records_by_student.items())[:5]))

# Initialize a list to store the final processed output
data_summary = []

# Analyze each student record
for student in student_records:
    student_id = student["Student ID"].strip()
    payment_dates = fee_records_by_student.get(student_id, [])

    if payment_dates:
        # Calculate the most common day of the month for fee submission
        day_frequency = defaultdict(int)
        for payment_date in payment_dates:
            day_of_month = payment_date.day
            day_frequency[day_of_month] += 1

        # Determine the day with the highest submission frequency
        most_common_day = max(day_frequency, key=day_frequency.get)
        frequency = day_frequency[most_common_day]

        # Store the result
        data_summary.append({
            "Student ID": student_id,
            "Most Frequent Fee Submission Day": most_common_day,
            "Frequency": frequency
        })

# Debugging: Output a sample of the final summary data
print("Summary Data Sample:", data_summary[:5])

# Record the script's end time
end_time = datetime.now()

# Calculate the total execution time
execution_duration = end_time - start_time

# Display results
print("\nProcessed Results:")
for entry in data_summary:
    print(f"Student ID: {entry['Student ID']}, Most Frequent Day: {entry['Most Frequent Fee Submission Day']}, Frequency: {entry['Frequency']}")

# Display execution time details
print("\nExecution Time Details:")
print(f"Start Time: {start_time}")
print(f"End Time: {end_time}")
print(f"Total Duration: {execution_duration}")
