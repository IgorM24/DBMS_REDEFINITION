# dbms_redefinition
Table online redefinition in PostgreSQL

## Concept

### Key Features
1. Implemented in PL/pgSQL
2. No additional extensions required
3. Support for incremental data synchronization mode
4. Preserves dependent objects (indexes, triggers, privileges) and their names
5. Rollback capability - ability to abort the process without data loss
6. Procedure set and parameters are as close as possible to Oracle DBMS implementation

### How It Works
At the first stage, an interim table with the target structure is created. This table can differ from the original one - have a different column set, be located in a separate tablespace, or be partitioned.

At the second stage, data synchronization is performed - rows from the original table are copied to the interim table (INSERT AS SELECT). Incremental synchronization mode is supported - processing rows modified since the last synchronization run (for incremental mode support, a trigger and change log table are created for the original table).

The third stage - synchronization of additional objects and attributes - creates the same indexes, constraints, triggers, and privileges for the interim table as in the original.

The final stage - table renaming. At this stage, the interim table receives the name of the original table, and objects created at the previous stage (indexes, constraints, triggers) are renamed so that their names match those in the original table upon completion.
Executing the final stage requires a brief exclusive lock on the original and interim tables.

## Usage Example
***Original table to be reorganized***
```
CREATE TABLE scott.emp
       (EMPNO NUMERIC(4) NOT NULL PRIMARY KEY,
        ENAME VARCHAR(10),
        JOB VARCHAR(9),
        MGR NUMERIC(4),
        HIREDATE DATE,
        SAL NUMERIC(7, 2),
        COMM NUMERIC(7, 2),
        DEPTNO NUMERIC(2));
ALTER TABLE scott.emp ADD CONSTRAINT emp_deptno_fk FOREIGN KEY (deptno) REFERENCES scott.dept(deptno);
CREATE INDEX emp_ename_idx ON scott.emp(ename);
CREATE INDEX emp_deptno ON scott.emp(deptno);
CREATE INDEX emp_job ON scott.emp(job);
```

***Creating new table*** (changed column list, different types for some columns, difference in index definitions)
```
CREATE TABLE scott.emp2
       (EMPNO NUMERIC(4) NOT NULL,
        ENAME text, -- changed type
        JOB text, -- changed type
        MGR NUMERIC(4),
        HIREDATE timestamp, -- changed type
        SAL NUMERIC(7, 2),
        --COMM NUMERIC(7, 2), -- column removed
        DEPTNO NUMERIC(2),
        last_update_timestamp timestamp -- column added
	);
CREATE INDEX emp_ename_job_idx ON scott.emp2(ename,job); -- index on two fields instead of two single-field indexes
```

***Checking if table can be reorganized***
```
CALL dbms_redefinition.can_redef_table('scott','emp');
```

***Starting reorganization process, setting field mapping rules***

Unchanged fields can be simply listed, for changed fields transformation rules can be specified.

Note: in expressions "~" is replaced with space
```
CALL dbms_redefinition.start_redef_table('scott','emp','emp2','empno, ename::text ename, job, mgr, (cast(hiredate~as~text)||''~00:00:01'')::timestamp hiredate, sal, deptno, now() last_update_timestamp');
```

***Data synchronization***
```
CALL dbms_redefinition.sync_interim_table('scott','emp','emp2');
```

***Registering "exceptions"*** (objects that don't need to be transferred or have different names)
```
CALL dbms_redefinition.register_dependent_object(
	uname=>'scott',
	orig_table=>'emp',
	int_table=>'emp2',
	dep_type=>'INDEX', 
	dep_orig_name=>'emp_job',
	dep_int_name=>NULL
);
CALL dbms_redefinition.register_dependent_object(
	uname=>'scott',
	orig_table=>'emp',
	int_table=>'emp2',
	dep_type=>'INDEX', 
	dep_orig_name=>'emp_ename_idx',
	dep_int_name=>'emp_ename_job_idx'
);
```

***Creating additional objects and attributes for interim table***
```
DO
$$
DECLARE
	l_num_errors numeric;
BEGIN
	CALL dbms_redefinition.copy_table_dependents(
		uname=>'scott',
		orig_table=>'emp',
		int_table=>'emp2',
		copy_indexes=>True,
		copy_triggers=>True,
		copy_constraints=>True,
		copy_privileges=>True,
		ignore_errors=>True,
		debug_mode=>True,
		num_errors=>l_num_errors
	);
	RAISE NOTICE 'l_num_errors=%',l_num_errors;
END;
$$;
```

***Completing the process***
```
CALL dbms_redefinition.finish_redef_table(
		uname=>'scott',
		orig_table=>'emp',
		int_table=>'emp2',
		ignore_errors=>True,
		debug_mode=>True);
```

***Cleaning up application service repository***
```
CALL dbms_redefinition.cleanup('scott','emp');
```


## Installation
A small internal repository and function set are created in a separate schema named "dbms_redefinition".

To install, run the "sql/dbms_redefinition.sql" script under a user account with administrator privileges.

## Limitations
1. The table to be reorganized must have a primary key or a column of type "OID"

2. Additional database space is required for simultaneous storage of original and interim table data

3. The current version doesn't support transferring all object types that might be associated with the reorganized table.

   List of unsupported object types:
    - Table Rules
    - Triggers in "DISABLED" state
    - Triggers with "Deferable/Deferred" attribute
    - Indexes in invalid state
    - Constraints with "Deferable" attribute
    - Non-validated constraints

## Function Description
Function descriptions and their parameters - in file "doc/dbms_redefinition.md"

## Development Plans
  - Planned to add support for objects currently unsupported (listed in "Limitations" section).

  - Also planned to improve handling of situations that may arise when unable to obtain a lock on the redefined object at the final stage of the algorithm.
