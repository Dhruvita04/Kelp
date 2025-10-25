# CSV to JSON Uploader
Quick start

1. Copy '.env.example' to '.env' and edit values.
2. Install dependencies:

3. npm install


4. Make sure Postgres is reachable and 'DATABASE_URL' is set.

5. Run one-time processing (reads 'CSV_FILE_PATH'):
npm run process

Or

7. Start the server and trigger via HTTP POST /process:
npm start
then POST to http://localhost:3000/process


Notes and assumptions
1. The CSV's first line must be headers.
2. Mandatory fields are 'name.firstName', 'name.lastName', and 'age'.
3. Any nested property is supported through dot notation in headers.
4. If mandatory fields are missing for a row it will be skipped and logged.

