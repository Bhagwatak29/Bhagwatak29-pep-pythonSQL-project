import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('C:/Users/hp/Bhagwatak29-pep-pythonSQL-project/resources/users.csv')
    load_and_clean_call_logs('C:/Users/hp/Bhagwatak29-pep-pythonSQL-project/resources/callLogs.csv')
    write_user_analytics('C:/Users/hp/Bhagwatak29-pep-pythonSQL-project/resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        next(reader) 

        valid_rows = []
        for row in reader:
            if len(row) != 2:
                continue  
            if any(not col.strip() for col in row):
                continue  
            if ',' in row[0] or ',' in row[1]:
                continue 
            valid_rows.append((row[0].strip(), row[1].strip()))

    cursor.executemany("INSERT INTO users (firstName, lastName) VALUES (?, ?)", valid_rows)
    conn.commit()



# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        next(reader) 

        valid_rows = []
        for row in reader:
            if len(row) != 5:
                continue  
            if any(not col.strip() for col in row):
                continue  
            try:
                start = int(row[1])
                end = int(row[2])
                user_id = int(row[4])
            except:
                continue  
            direction = row[3].strip().lower()
            if direction not in ('inbound', 'outbound'):
                continue  
            valid_rows.append((row[0].strip(), start, end, direction, user_id))

    cursor.executemany('''
        INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId)
        VALUES (?, ?, ?, ?, ?)
    ''', valid_rows)
    conn.commit()



# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    cursor.execute('''
        SELECT userId,
               AVG(endTime - startTime) AS avgDuration,
               COUNT(*) AS numCalls
        FROM callLogs
        GROUP BY userId
    ''')
    rows = cursor.fetchall()

    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])  
        for row in rows:
            writer.writerow([row[0], round(row[1], 1), row[2]])  


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute('''
        SELECT callId, phoneNumber, startTime, endTime, direction, userId
        FROM callLogs
        ORDER BY userId ASC, startTime ASC
    ''')
    rows = cursor.fetchall()

    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])  # CSV header
        writer.writerows(rows)




# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
