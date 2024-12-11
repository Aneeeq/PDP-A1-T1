import dask.dataframe as dd
from datetime import datetime

# Record the script's start time
start_time = datetime.now()

# Determine the current year dynamically
current_year = datetime.now().year

# Load students and fees data into Dask DataFrames
students_df = dd.read_csv('data/students.csv', encoding='utf-8')
fees_df = dd.read_csv('data/student_fees.csv', encoding='utf-8')

# Clean and preprocess "Payment Status"
fees_df['Payment Status'] = fees_df['Payment Status'].str.strip().str.lower()

# Append current year to "Payment Date" and convert to datetime
fees_df['Payment Date'] = fees_df['Payment Date'].apply(
    lambda x: f"{x} {current_year}", meta=('x', 'str')
)
fees_df['Payment Date'] = dd.to_datetime(fees_df['Payment Date'], format='%B %d %Y', errors='coerce')

# Filter to include only paid fees
paid_fees = fees_df[fees_df['Payment Status'] == 'paid']

# Extract day of the month for analysis
paid_fees['Day of Month'] = paid_fees['Payment Date'].dt.day

# Group by Student ID and Day of Month, then count occurrences
day_frequency = paid_fees.groupby(['Student ID', 'Day of Month']).size().reset_index()
day_frequency.columns = ['Student ID', 'Day of Month', 'Frequency']

# Trigger parallel computation
computed_frequency = day_frequency.compute()

# Determine the most frequent day for each student
most_frequent_days = computed_frequency.loc[
    computed_frequency.groupby('Student ID')['Frequency'].idxmax()
]

# Compute students DataFrame for merging
students_df = students_df.compute()

# Merge with the most frequent days data
output_data = students_df.merge(
    most_frequent_days[['Student ID', 'Day of Month', 'Frequency']],
    on='Student ID',
    how='left'
)

# Record the script's end time
end_time = datetime.now()

# Calculate the total execution time
execution_duration = end_time - start_time

# Display results
print("\nProcessed Results:")
print(output_data[['Student ID', 'Day of Month', 'Frequency']])

# Display execution time details
print("\nExecution Time Details:")
print(f"Start Time: {start_time}")
print(f"End Time: {end_time}")
print(f"Total Duration: {execution_duration}")
