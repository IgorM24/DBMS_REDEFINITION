# dbms_redefinition
Table online redefinition in PostgreSQL

### Function Description

---
Check if table reorganization can be performed
```
FUNCTION dbms_redefinition.can_redef_table(
	uname IN name,
	tname IN name
) RETURNS void

uname - table schema
tname - table name
```
---

Start table reorganization process
```
FUNCTION dbms_redefinition.start_redef_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	col_mapping IN text 	DEFAULT null,
	debug_mode 	IN boolean 	DEFAULT False
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - previously created interim table name
col_mapping	 - column mapping rules
debug_mode	 - output executed commands
```

---

Full initial synchronization (data copying) from source table to interim table
```
FUNCTION dbms_redefinition.sync_full_interim_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT True
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - previously created interim table name
debug_mode	 - output executed commands
```

---

Registration of objects that don't require transfer (e.g., created manually)
```
FUNCTION dbms_redefinition.register_dependent_object(
	uname 			IN name,
	orig_table 		IN name,
	int_table 		IN name,
	dep_type 		IN text,
	dep_orig_name 	IN name,
	dep_int_name 	IN name
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - interim table name
dep_type 	 - object type (allowed values: "INDEX", "CONSTRAINT", "TRIGGER")
dep_orig_name 	 - object name associated with the source table
dep_int_name 	 - object name associated with the interim table

```

---

Incremental synchronization (data copying) from source table to interim table
```
FUNCTION dbms_redefinition.sync_interim_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT False
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - interim table name
debug_mode	 - output executed commands
```

---

Creation/copying of additional objects (indexes, triggers, constraints, privileges) associated with the source table
```
FUNCTION dbms_redefinition.copy_table_dependents(
	uname 				IN name,
	orig_table 			IN name,
	int_table 			IN name,
	num_errors 			OUT numeric,
	copy_indexes 		IN boolean DEFAULT False,
	copy_triggers 		IN boolean DEFAULT False,
	copy_constraints 	IN boolean DEFAULT False,
	copy_privileges 	IN boolean DEFAULT False,
	ignore_errors 		IN boolean DEFAULT False,
	debug_mode 			IN boolean DEFAULT False
) RETURNS numeric

uname 		 - table schema
orig_table	 - table name
int_table	 - interim table name
num_errors 	 - number of errors recorded during operation
copy_indexes 	 - copy indexes
copy_triggers 	 - copy triggers
copy_constraints - copy constraints
copy_privileges	 - copy privileges
ignore_errors 	 - ignore errors
debug_mode	 - output executed commands

Return value - number of errors recorded during operation
```

---
Completion of the redefinition process (final synchronization and object renaming)
```
FUNCTION dbms_redefinition.finish_redef_table(
	uname 			IN name,
	orig_table 		IN name,
	int_table 		IN name,
	ignore_errors 	IN boolean DEFAULT False,
	debug_mode 		IN boolean DEFAULT False
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - interim table name
ignore_errors 	 - ignore errors
debug_mode	 - output executed commands
```

---
Cancellation ("rollback") of the redefinition process
```
FUNCTION dbms_redefinition.abort_redef_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT False
) RETURNS void

uname 		 - table schema
orig_table	 - table name
int_table	 - interim table name
debug_mode	 - output executed commands
```

---
Cleaning up the application service repository

```
FUNCTION dbms_redefinition.cleanup(
	uname 		IN name,
	orig_table 	IN name
) RETURNS void

uname 		 - table schema
orig_table	 - table name
```
