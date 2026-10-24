Opening the database in a new process is faster because schema comments are applied from a precomputed plan instead of re-parsing the DDL each time.
