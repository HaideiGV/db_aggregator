For running scripts you should install dependencies with next command:
`pip install -r requirements.txt`

DB create script run command - `python db_create.py`:
    Script creates 3 tables `raw_data`, `agg_data`, `last_processed_id`

DB fill script run command  - `python db_fill.py`:
    Fill raw_data tables with test data.

DB drop script run command  - `python db_drop.py`:
    Can drop tables if it needed.

DB aggregate script run command - `python db_aggregate.py`:
    Fill agg_data with aggregated data from raw_data.

DB validate script run command - `python db_validate.py`:
    Check if aggregated data is valid.