-- Copyright 2025 IgorM24
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

----------------------------------------------------------------------
-- Schema for objects
CREATE SCHEMA IF NOT EXISTS dbms_redefinition;

-- (Re)Create objects

-- API (public functions)
DROP FUNCTION IF EXISTS dbms_redefinition.can_redef_table(name,name);
DROP FUNCTION IF EXISTS dbms_redefinition.start_redef_table(name,name,name,text,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.sync_full_interim_table(name,name,name,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.register_dependent_object(name,name,name,text,name,name);
DROP FUNCTION IF EXISTS dbms_redefinition.sync_interim_table(name,name,name,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.copy_table_dependents(name,name,name,boolean,boolean,boolean,boolean,boolean,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.finish_redef_table(name,name,name,boolean,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.abort_redef_table(name,name,name,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.cleanup(name,name);

-- Internal functions
DROP FUNCTION IF EXISTS dbms_redefinition.internal_syncinc_aux_create(name,name,name,boolean);
DROP FUNCTION IF EXISTS dbms_redefinition.internal_syncinc_aux_drop(name,name,name,boolean);

-- Repository tables
DROP TABLE IF EXISTS dbms_redefinition.redef_object_map;
DROP TABLE IF EXISTS dbms_redefinition.redef_status;

-- 

CREATE TABLE dbms_redefinition.redef_status(
   obj_owner 				name NOT NULL,
   obj_name 				name NOT NULL,
   last_operation 			varchar(30),
   last_operation_id 		numeric,
   last_operation_status 	varchar(30),
   last_operation_timestamp timestamp(0) without time zone,
   notes text
);
ALTER TABLE dbms_redefinition.redef_status ADD CONSTRAINT redef_status_pk PRIMARY KEY(obj_owner,obj_name);

COMMENT ON TABLE dbms_redefinition.redef_status IS 'Status of the redefinition process';
COMMENT ON COLUMN dbms_redefinition.redef_status.obj_owner IS 'Object owner';
COMMENT ON COLUMN dbms_redefinition.redef_status.obj_name IS 'Object name';
COMMENT ON COLUMN dbms_redefinition.redef_status.last_operation IS 'Name of the last completed stage';
COMMENT ON COLUMN dbms_redefinition.redef_status.last_operation_id IS 'Number of the last completed stage';
COMMENT ON COLUMN dbms_redefinition.redef_status.last_operation_status IS 'Status of the last completed stage';
COMMENT ON COLUMN dbms_redefinition.redef_status.last_operation_timestamp IS 'Timestamp of the last completed stage';
COMMENT ON COLUMN dbms_redefinition.redef_status.notes IS 'Notes';


CREATE TABLE dbms_redefinition.redef_object_map(
   obj_owner 		name NOT NULL,
   obj_name 		name NOT NULL,
   map_type 		varchar(30),
   orig_obj_name 	name,
   orig_col_type 	varchar(100),
   new_obj_name 	name,
   new_col_type 	varchar(100),
   notes 			text
);
ALTER TABLE dbms_redefinition.redef_object_map ADD CONSTRAINT redef_object_map_fk FOREIGN KEY(obj_owner,obj_name) REFERENCES dbms_redefinition.redef_status(obj_owner,obj_name) ON DELETE CASCADE;
ALTER TABLE dbms_redefinition.redef_object_map ADD CONSTRAINT redef_object_map_uk UNIQUE(obj_owner,obj_name,map_type,orig_obj_name,new_obj_name);
CREATE INDEX redef_object_map_idx1 ON dbms_redefinition.redef_object_map(obj_owner,obj_name);

COMMENT ON TABLE dbms_redefinition.redef_object_map IS 'Mapping rules';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.obj_owner IS 'Object owner';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.obj_name IS 'Object name';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.map_type IS 'Mapping type';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.orig_obj_name IS 'Original object name';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.orig_col_type IS 'Original column data type';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.new_obj_name IS 'New object name';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.new_col_type IS 'New column data type';
COMMENT ON COLUMN dbms_redefinition.redef_object_map.notes IS 'Notes';


----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.can_redef_table(
	uname IN name,
	tname IN name
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	l_last_oper_record 	dbms_redefinition.redef_status%ROWTYPE;
	l_status 			varchar(30);
	l_notes 			text;
	l_err_txt 			text;
	l_conname 			name;
	l_contype 			varchar(30);
	l_cnt 				numeric;
BEGIN
    -- operation_id=1
    -- Check if table exists
    SELECT count(*) 
	INTO l_cnt	
	FROM pg_catalog.pg_class r 
		JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
	WHERE n.nspname = uname AND r.relname = tname;
	IF l_cnt = 0 THEN
		RAISE NOTICE 'Table %.% not found', uname, tname;
		RETURN;
	ELSIF l_cnt > 1 THEN -- This shouldn't happen
		RAISE NOTICE 'Too many tables %.%', uname, tname;
		RETURN;
	END IF;
  
	-- Check if table have PK
	BEGIN 
		SELECT c.conname, c.contype::varchar
		INTO STRICT l_conname, l_contype
		FROM pg_catalog.pg_constraint c 
			JOIN pg_catalog.pg_class r ON r.oid = c.conrelid 
			JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace
		WHERE n.nspname = uname AND r.relname = tname AND c.contype='p';
		l_status:='OK';
		l_notes:='PK: '||l_conname;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			-- Check if table have OID
			BEGIN
				SELECT 'oid'
				INTO STRICT l_conname
				FROM pg_catalog.pg_class r
				LEFT JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
				WHERE 1=1
				  AND r.relkind = 'r'
				  AND r.relhasoids = true
				  AND n.nspname = uname 
				  AND r.relname = tname; 
				l_status:='OK';
				l_notes:='PK: '||l_conname;
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					l_status:='ERROR';
					l_notes:='ERROR: Table does not have a primary key or OID column';
				WHEN OTHERS THEN
					l_status:='ERROR';
					l_notes:='ERROR: Table does not have a primary key';
			END;
		WHEN TOO_MANY_ROWS THEN -- This shouldn't happen
			l_status:='ERROR';  
			l_notes:='ERROR: Table have multiple primary keys';
	END;
	
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=tname;
	END;
	IF l_last_oper_record.last_operation_id > 1 THEN
		RAISE NOTICE 'You are already in the process of reorganizing the table %.%. Execute "abort_redef_table" to cancel the previous reorganization session or "finish_redef_table" to complete it.', uname, tname;
		RETURN;
	END IF;
	
	-- Checking for unsupported options
	-- Table Rules
    SELECT count(*) 
	INTO l_cnt	
	FROM pg_catalog.pg_rules r 
	WHERE r.schemaname = uname AND r.tablename = tname;
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Table Rule !';
	END IF;
	
	-- Triggers in disabled state
	SELECT count(*)
	INTO l_cnt	
	FROM pg_catalog.pg_trigger t 
	JOIN pg_catalog.pg_class rel ON rel.oid = t.tgrelid
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
	WHERE nsp.nspname = uname 
	  AND rel.relname = tname 
	  AND rel.relkind = 'r' 
	  AND NOT t.tgisinternal
	  AND t.tgenabled IN ('D','R','A');
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Trigger status !';
	END IF;
	
	-- Deferable/Deffeded Triggers
	SELECT count(*)
	INTO l_cnt	
	FROM pg_catalog.pg_trigger t 
	JOIN pg_catalog.pg_class rel ON rel.oid = t.tgrelid
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
	WHERE nsp.nspname = uname 
	  AND rel.relname = tname 
	  AND rel.relkind = 'r' 
	  AND NOT t.tgisinternal
	  AND (t.tgdeferrable OR t.tginitdeferred);
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Deferable/Deffeded Trigger !';
	END IF;

	-- Invalid indexes
	SELECT count(*)
	INTO l_cnt	
	FROM pg_index i
	JOIN pg_class t ON t.oid = i.indrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	WHERE t.relname = tname
		  AND n.nspname = uname
		  AND NOT i.indisvalid;	  
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Invalid Index !';
	END IF;
	
	-- Deferable constraint
	SELECT count(*)
	INTO l_cnt	
	FROM pg_catalog.pg_constraint con 
	JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid 
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
	WHERE nsp.nspname = uname 	
	  AND rel.relname = tname
	  AND con.condeferrable;
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Deferable Constraint !';
	END IF;
	
	-- Not validated constraint
	SELECT count(*)
	INTO l_cnt	
	FROM pg_catalog.pg_constraint con 
	JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid 
	JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
	WHERE nsp.nspname = uname 	
	  AND rel.relname = tname
	  AND NOT con.convalidated;
	IF l_cnt > 0 THEN 
		RAISE NOTICE 'Warning: Unsuported option - Not validated Constraint !';
	END IF;
		
	-- Refresh operation status
	DELETE FROM dbms_redefinition.redef_status WHERE obj_owner=uname AND obj_name=tname;
	INSERT INTO dbms_redefinition.redef_status(obj_owner,obj_name,last_operation,last_operation_id,last_operation_status,last_operation_timestamp,notes)
	  VALUES(uname, tname, 'CHECK', 1, l_status, NOW(), l_notes);
	
	RAISE NOTICE 'Status: % (%)', l_status, l_notes;
END;
$$;
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.start_redef_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	col_mapping IN text 	DEFAULT null,
	debug_mode 	IN boolean 	DEFAULT False
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	l_last_oper_record 			dbms_redefinition.redef_status%ROWTYPE;
	l_redef_object_map_record 	dbms_redefinition.redef_object_map%ROWTYPE;
	rec_col_orig 				record;
	rec_col_map_rec 			record;
	l_col_mapping_array 		text[];
	l_old_name 					name;
	l_new_name 					name;
	l_mached 					boolean;
	l_status 					varchar(30);
	l_aux_call_status 			boolean;
	l_notes 					text;
	l_cnt 						numeric;
	l_sql_text01 				text;
	l_sql_text02 				text;
BEGIN
    -- operation_id=2
	-- Check parameters
    SELECT count(*) 
	INTO l_cnt	
	FROM pg_catalog.pg_class r 
		JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
	WHERE n.nspname = uname AND r.relname = orig_table;
	IF l_cnt = 0 THEN
		RAISE NOTICE 'Table %.% not found', uname, orig_table;
		RETURN;
	END IF;
    SELECT count(*) 
	INTO l_cnt	
	FROM pg_catalog.pg_class r 
		JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
	WHERE n.nspname = uname AND r.relname = int_table;
	IF l_cnt = 0 THEN
		RAISE NOTICE 'Table %.% not found', uname, int_table;
		RETURN;
	END IF;

    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id > 2 THEN
		RAISE NOTICE 'You are already in the process of reorganizing the table %.%. Execute "abort_redef_table" to cancel the previous reorganization session or "finish_redef_table" to complete it.', uname, orig_table;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id <> 2 AND l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;
	
	
	-- Delete old mapping
	DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=uname AND obj_name=orig_table;
	
	l_col_mapping_array:=string_to_array(trim(col_mapping), ',');
	
	l_status:= 'OK';
	l_notes	:= null;
	
	-- Check/fill columns mapping

	IF trim(col_mapping) IS NOT NULL THEN -- Manual column maping
		FOR i IN 1..CARDINALITY(l_col_mapping_array) LOOP
			l_old_name:=replace(trim(split_part(trim(l_col_mapping_array[i]),' ',1)),'~',' ');
			l_new_name:=replace(trim(split_part(trim(l_col_mapping_array[i]),' ',2)),'~',' ');
			IF l_new_name IS NULL OR l_new_name='' THEN 
				l_new_name:=l_old_name;
			END IF;	

			l_mached := False;
			l_redef_object_map_record.obj_owner		:= uname;
			l_redef_object_map_record.obj_name		:= orig_table;
			l_redef_object_map_record.map_type		:= 'COLUMN';
			l_redef_object_map_record.orig_obj_name	:= l_old_name;
			l_redef_object_map_record.orig_col_type	:= null;
			l_redef_object_map_record.new_obj_name	:= l_new_name;
			l_redef_object_map_record.new_col_type	:= null;
			l_redef_object_map_record.notes			:= null;		

			-- Check column in destination TABLE
			BEGIN
				SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) as col_type
				INTO STRICT l_redef_object_map_record.new_col_type
				FROM pg_catalog.pg_class r 
				  JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
				  JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
				WHERE n.nspname = uname AND r.relname = int_table AND a.attnum > 0 AND a.attname=replace(l_new_name,'"','');					
				-- Column mached by name
				l_mached := True;
			EXCEPTION 
				WHEN NO_DATA_FOUND THEN
					l_redef_object_map_record.notes:='Invalid mapping';
					l_status:='ERROR';
			END;
			
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=l_redef_object_map_record.obj_owner AND obj_name=l_redef_object_map_record.obj_name AND map_type=l_redef_object_map_record.map_type AND orig_obj_name=l_redef_object_map_record.orig_obj_name AND new_obj_name=l_redef_object_map_record.new_obj_name;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
			  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, l_redef_object_map_record.orig_obj_name, l_redef_object_map_record.orig_col_type, l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
		
		END LOOP;
		
	ELSE -- Automatic mapping	
	
		FOR rec_col_orig IN (
			SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as col_type
			FROM pg_catalog.pg_class r 
			  JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
			  JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
			WHERE n.nspname = uname AND r.relname = orig_table AND a.attnum > 0
			ORDER BY a.attnum
		) LOOP
		
			l_mached := False;
			l_redef_object_map_record.obj_owner		:= uname;
			l_redef_object_map_record.obj_name		:= orig_table;
			l_redef_object_map_record.map_type		:= 'COLUMN';
			l_redef_object_map_record.orig_obj_name	:= rec_col_orig.attname;
			l_redef_object_map_record.orig_col_type	:= rec_col_orig.col_type;
			l_redef_object_map_record.new_obj_name	:= null;
			l_redef_object_map_record.new_col_type	:= null;
			l_redef_object_map_record.notes			:= null;		
			
			BEGIN
				SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as col_type
				INTO STRICT l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type
				FROM pg_catalog.pg_class r 
				  JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
				  JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
				WHERE n.nspname = uname AND r.relname = int_table AND a.attnum > 0 AND a.attname=rec_col_orig.attname;	
				l_mached := True;
			EXCEPTION 
				WHEN NO_DATA_FOUND THEN -- Invalid mapping
					l_redef_object_map_record.notes:='Invalid mapping';
					l_status:='ERROR';
					l_notes:='Invalid mapping';
			END;	
			
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=l_redef_object_map_record.obj_owner AND obj_name=l_redef_object_map_record.obj_name AND map_type=l_redef_object_map_record.map_type AND orig_obj_name=l_redef_object_map_record.orig_obj_name AND new_obj_name=l_redef_object_map_record.new_obj_name;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
			  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, quote_ident(l_redef_object_map_record.orig_obj_name), l_redef_object_map_record.orig_col_type, quote_ident(l_redef_object_map_record.new_obj_name), l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
		
		END LOOP;
		
	END IF; -- IF trim(col_mapping) IS NOT NULL THEN
	
	RAISE NOTICE 'Column mapping:';
	FOR rec_col_map_rec IN(
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type='COLUMN'
	) LOOP
		RAISE NOTICE '  % --> % [%]', rec_col_map_rec.orig_obj_name, rec_col_map_rec.new_obj_name, rec_col_map_rec.notes;
	END LOOP;

	IF l_status='OK' THEN
	    
		-- Create (recreate) auxiliary objects for incremental refresh 
		PERFORM dbms_redefinition.internal_syncinc_aux_drop(uname,orig_table,int_table,debug_mode);
		SELECT dbms_redefinition.internal_syncinc_aux_create(uname,orig_table,int_table,debug_mode)
		INTO l_aux_call_status;
		IF l_aux_call_status THEN
			l_status:='OK';
			l_notes :='';
		ELSE
			l_status:='ERROR';
			l_notes :='Error during execution "internal_syncinc_aux_create"';
			-- Drop auxiliary objects
			PERFORM dbms_redefinition.internal_syncinc_aux_drop(uname,orig_table,int_table,debug_mode);
		END IF;
		
	END IF; -- IF l_status='OK'

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='BEGIN', 
	  last_operation_id=2, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;

	RAISE NOTICE 'Status: %', l_status;

END;
$$;	

----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.sync_full_interim_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT True
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	l_last_oper_record dbms_redefinition.redef_status%ROWTYPE;
	rec_col_map_rec record;
	l_status varchar(30);
	l_notes 		text;
	l_cnt 			numeric;
	l_ok_flag 		boolean;
	l_sql_text01 	text;
	l_sql_text02 	text;
	l_sql_text03 	text;

BEGIN

    -- operation_id=3
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id < 2 THEN
		RAISE NOTICE 'This procedure must be executed after "start_redef_table"';
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id <> 3 AND l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;

	IF l_last_oper_record.last_operation_id = 5 AND l_last_oper_record.last_operation_status = 'OK' THEN
		RAISE NOTICE 'Redifinition for table %.% already successfully completed ', uname, orig_table;
		RETURN;
	END IF;	
	IF l_last_oper_record.last_operation_id = 6 THEN
		RAISE NOTICE 'Redifinition for table %.% has already been cancelled', uname, orig_table;
		RETURN;
	END IF;
 	
	l_status:='OK';
	l_notes	 := null;
	l_ok_flag:= True;

	l_sql_text01:='INSERT INTO '||quote_ident(uname)||'.'||quote_ident(int_table)||'(';
	l_sql_text02:=' SELECT ';
	l_cnt:=0;
	FOR rec_col_map_rec IN(
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type='COLUMN'
	) LOOP
		l_cnt:=l_cnt+1;
		IF l_cnt>1 THEN
			l_sql_text01:=l_sql_text01||',';
			l_sql_text02:=l_sql_text02||',';
		END IF;
		IF rec_col_map_rec.orig_obj_name IS NULL OR rec_col_map_rec.new_obj_name IS NULL THEN
			l_ok_flag:=False;
		END IF;	
		l_sql_text01:=l_sql_text01||COALESCE(rec_col_map_rec.new_obj_name,'?');
		l_sql_text02:=l_sql_text02||COALESCE(rec_col_map_rec.orig_obj_name,'?');
	END LOOP;
	l_sql_text01:=l_sql_text01||')';
	l_sql_text02:=l_sql_text02||' FROM '||quote_ident(uname)||'.'||quote_ident(orig_table)||';';
	l_notes:=null;

	IF l_ok_flag THEN
		BEGIN
			l_sql_text03:='TRUNCATE TABLE '||quote_ident(uname)||'.'||quote_ident(int_table)||'; ';
			l_sql_text03:=l_sql_text03||l_sql_text01||l_sql_text02;
			RAISE NOTICE 'Note: full table synchronization start at %', now();
			IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text03; END IF;
			EXECUTE l_sql_text03;
			RAISE NOTICE 'Note: full table synchronization finish at %', now();
		EXCEPTION
			WHEN OTHERS THEN
				l_status:='ERROR';
				l_notes	 := 'Something seems to be wrong with this SQL: ['||l_sql_text03||']';
				RAISE NOTICE '[%]', l_notes;
		END;
	ELSE
		l_status:='ERROR';
		l_notes	 :=	'Something seems to be wrong with this SQL: ['||l_sql_text01||l_sql_text02||']';
		RAISE NOTICE '[%]', l_notes;
	END IF; -- IF l_ok_flag

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='SYNC', 
	  last_operation_id=3, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;

	RAISE NOTICE 'Status: %', l_status;

END;
$$;	

----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.register_dependent_object(
	uname 			IN name,
	orig_table 		IN name,
	int_table 		IN name,
	dep_type 		IN text, -- (INDEX or CONSTRAINT or TRIGGER)
	dep_orig_name 	IN name,
	dep_int_name 	IN name
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	l_last_oper_record 			dbms_redefinition.redef_status%ROWTYPE;
	l_redef_object_map_record 	dbms_redefinition.redef_object_map%ROWTYPE;
	l_status 					varchar(30);
BEGIN
	
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id < 2 THEN
		RAISE NOTICE 'First, execute the "start_redef_table" procedure for the table %.%', uname, orig_table;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;
	
	-- Check parameters
	IF dep_type NOT IN ('INDEX', 'CONSTRAINT',  'TRIGGER') THEN
		RAISE NOTICE 'Wrong value for paramenet "dep_type". Valid values: "INDEX","CONSTRAINT", "TRIGGER"';
		RETURN;
	END IF;

	l_redef_object_map_record.obj_owner		:= uname;
	l_redef_object_map_record.obj_name		:= orig_table;
	l_redef_object_map_record.map_type		:= dep_type;
	l_redef_object_map_record.orig_obj_name	:= trim(dep_orig_name);
	l_redef_object_map_record.orig_col_type	:= null;
	l_redef_object_map_record.new_obj_name	:= trim(dep_int_name);
	l_redef_object_map_record.new_col_type	:= null;
	l_redef_object_map_record.notes			:= 'SELF_REGISTERED';
	
	l_status:='OK';	
	
	BEGIN
		INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
		  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, l_redef_object_map_record.orig_obj_name, l_redef_object_map_record.orig_col_type, l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
	EXCEPTION
		WHEN unique_violation THEN
		RAISE NOTICE 'Such object is already registered';
		l_status:='ERROR';
	END;	
	
	RAISE NOTICE 'Status: %', l_status;

END;
$$;			

----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.sync_interim_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT False
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	c_aux_name_prefix 	CONSTANT name := 'xxaux';
	l_last_oper_record 	dbms_redefinition.redef_status%ROWTYPE;
	l_status 			varchar(30);
	l_notes 			text;
	l_cnt 				numeric;
	l_proc_name 		text;
	l_sql_text01 		text;

BEGIN
    -- operation_id=3
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id < 2 THEN
		RAISE NOTICE 'This procedure must be executed after "start_redef_table"';
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id <> 3 AND l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id = 5 AND l_last_oper_record.last_operation_status = 'OK' THEN
		RAISE NOTICE 'Redifinition for table %.% already successfully completed ', uname, orig_table;
		RETURN;
	END IF;	
	IF l_last_oper_record.last_operation_id = 6 THEN
		RAISE NOTICE 'Redifinition for table %.% has already been cancelled', uname, orig_table;
		RETURN;
	END IF;
	
	-- Check objects for incremental refresh
	SELECT count(*)
	INTO l_cnt 
	FROM dbms_redefinition.redef_object_map
	WHERE obj_owner=uname
	  AND obj_name=orig_table
	  AND map_type IN ('SYSTABLE','SYSTRIGGER','SYSTFUNCTION','SYSSFUNCTION');
	IF l_cnt <> 4 THEN
		RAISE NOTICE 'Auxiliary objects not registered, incremental sync inpossible';
		RETURN;
	END IF;
	
	SELECT new_obj_name
	INTO STRICT l_proc_name
	FROM dbms_redefinition.redef_object_map
	WHERE obj_owner=uname
	  AND obj_name=orig_table
	  AND map_type = 'SYSSFUNCTION';
	
	l_status:='OK';
	l_notes :=null;

	BEGIN
		l_sql_text01:='DO $SYNC$ BEGIN PERFORM '||quote_ident(uname)||'.'||quote_ident(l_proc_name)||'(); END; $SYNC$;';
		IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
		EXECUTE l_sql_text01;
	EXCEPTION
		WHEN OTHERS THEN
			l_notes:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
			l_status:='ERROR';
			RAISE NOTICE '[%]', l_notes;
	END;

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='SYNC', 
	  last_operation_id=3, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;
	
	RAISE NOTICE 'Status: %', l_status;

END;
$$;	

----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.copy_table_dependents(
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
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE

    c_new_name_prefix 			CONSTANT name := '__';
	l_last_oper_record 			dbms_redefinition.redef_status%ROWTYPE;
	l_redef_object_map_record 	dbms_redefinition.redef_object_map%ROWTYPE;
	l_rec_constraint 			record;
	l_rec_index 				record;
	l_rec_trigger 				record;
	l_rec_grants 				record;
	l_constraints_array 		text[] := '{}';
	l_array_element 			text;
	l_new_name 					name;
	l_status 					varchar(30);
	l_notes 					text;
	l_cnt 						numeric;
	l_errors_cnt 				numeric;
	l_sql_text01 				text;
	l_bool_flag 				boolean;

BEGIN
    -- operation_id=4
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id < 2 THEN
		RAISE NOTICE 'First, execute the "start_redef_table" procedure for the table %.%', uname, orig_table;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id <> 4 AND l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;
	
	l_errors_cnt:=0;
	l_notes:=null;
	
	IF copy_constraints AND (ignore_errors OR l_errors_cnt=0) THEN -- Copy CONSTRAINTS
	
		FOR l_rec_constraint IN
			SELECT nsp.nspname,
				   rel.relname,
				   con.conname, 
				   con.contype,
				   pg_get_constraintdef(con.oid) AS consql,
				   CASE
					WHEN con.contype = 'p' THEN 1 -- PK
					WHEN con.contype = 'u' THEN 2 -- UK
					WHEN con.contype = 'f' THEN 3 -- FK
					WHEN con.contype = 'c' THEN 4
					ELSE 5
				   END AS sort_field
			FROM pg_catalog.pg_constraint con 
			JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid 
			JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
			WHERE nsp.nspname = uname 
			  AND rel.relname = orig_table
			  AND NOT EXISTS (
			    SELECT '1' 
				FROM dbms_redefinition.redef_object_map rom
				WHERE rom.obj_owner=uname
				  AND rom.obj_name=orig_table
				  AND rom.map_type='CONSTRAINT'
				  AND rom.orig_obj_name=con.conname
				  AND rom.notes='SELF_REGISTERED'
				)
			ORDER BY sort_field
		LOOP
			l_new_name:= SUBSTR(c_new_name_prefix||l_rec_constraint.conname,1,63);
			l_sql_text01:='ALTER TABLE ' || quote_ident(l_rec_constraint.nspname) || '.' || quote_ident(int_table) || ' ADD CONSTRAINT ' || quote_ident(l_new_name) || ' ' || l_rec_constraint.consql || ';';
			l_notes:=null;
			
			BEGIN
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			EXCEPTION
				WHEN OTHERS THEN
					l_errors_cnt:=l_errors_cnt+1;
					l_notes:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
					RAISE NOTICE '%', l_notes;
			END;
			l_constraints_array:=l_constraints_array || l_rec_constraint.conname::text;
			
			l_redef_object_map_record.obj_owner		:= uname;
			l_redef_object_map_record.obj_name		:= orig_table;
			l_redef_object_map_record.map_type		:= 'CONSTRAINT';
			l_redef_object_map_record.orig_obj_name	:= l_rec_constraint.conname;
			l_redef_object_map_record.orig_col_type	:= null;
			l_redef_object_map_record.new_obj_name	:= l_new_name;
			l_redef_object_map_record.new_col_type	:= null;
			l_redef_object_map_record.notes			:= l_notes;		
		
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=l_redef_object_map_record.obj_owner AND obj_name=l_redef_object_map_record.obj_name AND map_type=l_redef_object_map_record.map_type AND orig_obj_name=l_redef_object_map_record.orig_obj_name AND new_obj_name=l_redef_object_map_record.new_obj_name;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
			  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, l_redef_object_map_record.orig_obj_name, l_redef_object_map_record.orig_col_type, l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
			
			IF (NOT ignore_errors AND l_errors_cnt>0) THEN
				EXIT;
			END IF;
			
		END LOOP;
		
	END IF; -- IF copy_constraints


	IF copy_indexes AND (ignore_errors OR l_errors_cnt=0) THEN -- Copy INDEXES

		FOR l_rec_index IN
			SELECT idx.indexname,
				   idx.indexdef
			FROM pg_catalog.pg_indexes idx 
			WHERE idx.schemaname = uname 
			  AND idx.tablename = orig_table
			  AND NOT EXISTS (
			    SELECT '1' 
				FROM dbms_redefinition.redef_object_map rom
				WHERE rom.obj_owner=uname
				  AND rom.obj_name=orig_table
				  AND rom.map_type='INDEX'
				  AND rom.orig_obj_name=idx.indexname
				  AND rom.notes='SELF_REGISTERED'
				)
		LOOP

			-- Skip indexes already created for PK/UK
			l_bool_flag = False;
			FOREACH l_array_element IN ARRAY l_constraints_array LOOP 
				IF l_array_element = l_rec_index.indexname THEN
					l_bool_flag = True;
					EXIT;
				END IF;
			END LOOP;   
			IF l_bool_flag THEN 
				CONTINUE; -- Skip this index
			END IF;
		
			l_new_name	 := SUBSTR(c_new_name_prefix||l_rec_index.indexname,1,63);
			l_sql_text01 := REPLACE(l_rec_index.indexdef, 'CREATE INDEX', 'CREATE INDEX IF NOT EXISTS');
            l_sql_text01 := REPLACE(l_sql_text01, 'CREATE UNIQUE INDEX', 'CREATE UNIQUE INDEX IF NOT EXISTS');
            l_sql_text01 := REPLACE(l_sql_text01, 'INDEX IF NOT EXISTS '||quote_ident(l_rec_index.indexname), 'INDEX IF NOT EXISTS '||quote_ident(l_new_name));
            l_sql_text01 := REPLACE(l_sql_text01, ' ON '||quote_ident(uname)||'.'||quote_ident(orig_table), ' ON '||quote_ident(uname)||'.'||quote_ident(int_table));
			l_notes		 := null;

			BEGIN
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			EXCEPTION
				WHEN OTHERS THEN
					l_errors_cnt:=l_errors_cnt+1;
					l_notes:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
					RAISE NOTICE '%', l_notes;
			END;
			
			l_redef_object_map_record.obj_owner		:= uname;
			l_redef_object_map_record.obj_name		:= orig_table;
			l_redef_object_map_record.map_type		:= 'INDEX';
			l_redef_object_map_record.orig_obj_name	:= l_rec_index.indexname;
			l_redef_object_map_record.orig_col_type	:= null;
			l_redef_object_map_record.new_obj_name	:= l_new_name;
			l_redef_object_map_record.new_col_type	:= null;
			l_redef_object_map_record.notes			:= l_notes;		
		
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=l_redef_object_map_record.obj_owner AND obj_name=l_redef_object_map_record.obj_name AND map_type=l_redef_object_map_record.map_type AND orig_obj_name=l_redef_object_map_record.orig_obj_name AND new_obj_name=l_redef_object_map_record.new_obj_name;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
			  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, l_redef_object_map_record.orig_obj_name, l_redef_object_map_record.orig_col_type, l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
			
			IF (NOT ignore_errors AND l_errors_cnt>0) THEN
				EXIT;
			END IF;
			
		END LOOP;
	
	END IF; -- IF copy_indexes 
	

	IF copy_triggers AND (ignore_errors OR l_errors_cnt=0) THEN -- Copy TRIGGERS
	
		FOR l_rec_trigger IN
			SELECT t.tgname,
				   t.tgenabled,
				   pg_get_triggerdef(t.oid) AS trgsql
			FROM pg_catalog.pg_trigger t 
			JOIN pg_catalog.pg_class rel ON rel.oid = t.tgrelid
			JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
			WHERE nsp.nspname = uname 
			  AND rel.relname = orig_table 
			  AND rel.relkind = 'r' 
			  AND NOT t.tgisinternal
			  AND t.tgenabled <>'D'
			  AND NOT EXISTS (
			    SELECT '1' 
				FROM dbms_redefinition.redef_object_map rom
				WHERE rom.obj_owner=uname
				  AND rom.obj_name=orig_table
				  AND rom.map_type='TRIGGER'
				  AND rom.orig_obj_name=t.tgname
				  AND rom.notes='SELF_REGISTERED'
				)
			  AND NOT EXISTS (
			    SELECT '1' 
				FROM dbms_redefinition.redef_object_map rom
				WHERE rom.obj_owner=uname
				  AND rom.obj_name=orig_table
				  AND rom.map_type='SYSTRIGGER'
				  AND rom.new_obj_name=t.tgname
				)
		LOOP
			-- Trigger sql format: " ON <schema>.<table_name>  "
			l_new_name	 := SUBSTR(c_new_name_prefix||l_rec_trigger.tgname,1,63);
			l_sql_text01 := REPLACE(l_rec_trigger.trgsql, 'CREATE TRIGGER '||quote_ident(l_rec_trigger.tgname), 'CREATE TRIGGER '||quote_ident(l_new_name));
			l_sql_text01 := REPLACE(l_sql_text01, ' ON '||quote_ident(uname)||'.'||quote_ident(orig_table), ' ON '||quote_ident(uname)||'.'||quote_ident(int_table));
			l_notes		 := null;
			
			BEGIN
				-- Create trigger
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
				-- Disable Trigger
				l_sql_text01 := 'ALTER TABLE '||quote_ident(uname)||'.'||quote_ident(int_table)||' DISABLE TRIGGER '||quote_ident(l_new_name);
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			EXCEPTION
				WHEN OTHERS THEN
					l_errors_cnt:=l_errors_cnt+1;
					l_notes		:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
					RAISE NOTICE '%', l_notes;
			END;

			l_redef_object_map_record.obj_owner		:= uname;
			l_redef_object_map_record.obj_name		:= orig_table;
			l_redef_object_map_record.map_type		:= 'TRIGGER';
			l_redef_object_map_record.orig_obj_name	:= l_rec_trigger.tgname;
			l_redef_object_map_record.orig_col_type	:= null;
			l_redef_object_map_record.new_obj_name	:= l_new_name;
			l_redef_object_map_record.new_col_type	:= null;
			l_redef_object_map_record.notes			:= l_notes;		
		
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=l_redef_object_map_record.obj_owner AND obj_name=l_redef_object_map_record.obj_name AND map_type=l_redef_object_map_record.map_type AND orig_obj_name=l_redef_object_map_record.orig_obj_name AND new_obj_name=l_redef_object_map_record.new_obj_name;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, orig_obj_name, orig_col_type, new_obj_name, new_col_type, notes)
			  VALUES(l_redef_object_map_record.obj_owner, l_redef_object_map_record.obj_name, l_redef_object_map_record.map_type, l_redef_object_map_record.orig_obj_name, l_redef_object_map_record.orig_col_type, l_redef_object_map_record.new_obj_name, l_redef_object_map_record.new_col_type, l_redef_object_map_record.notes);
			
			IF (NOT ignore_errors AND l_errors_cnt>0) THEN
				EXIT;
			END IF;
			
		END LOOP;
		
	END IF; -- IF copy_triggers
	

	IF copy_privileges AND (ignore_errors OR l_errors_cnt=0) THEN -- Copy PRIVILEGES
	
		FOR l_rec_grants IN
			SELECT g.*
			FROM information_schema.role_table_grants g,
			  pg_catalog.pg_class rel,
			  pg_catalog.pg_namespace nsp
			WHERE nsp.oid = rel.relnamespace
			  AND rel.relname = g.table_name 
			  AND nsp.nspname = g.table_schema
			  AND rel.relkind IN ('r','p')
			  AND pg_catalog.pg_get_userbyid(rel.relowner) <> g.grantee 
			  AND g.grantor <> g.grantee
			  AND nsp.nspname = uname AND rel.relname = orig_table
		LOOP
			l_sql_text01:='GRANT ' || l_rec_grants.privilege_type || ' ON TABLE ' || quote_ident(l_rec_grants.table_schema) || '.' || quote_ident(int_table) || ' TO ' || quote_ident(l_rec_grants.grantee);
			IF l_rec_grants.is_grantable = 'YES' THEN
				l_sql_text01:=l_sql_text01||' WITH GRANT OPTION';
			END IF;
			
			BEGIN
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			EXCEPTION
				WHEN OTHERS THEN
					l_errors_cnt:=l_errors_cnt+1;
					l_notes		:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
					RAISE NOTICE '%', l_notes;
			END;
			
			IF (NOT ignore_errors AND l_errors_cnt>0) THEN
				EXIT;
			END IF;
			
		END LOOP;
		
	END IF; -- IF copy_privileges

	
	IF (ignore_errors OR l_errors_cnt=0) THEN
		l_status:='OK';
	ELSE
		l_status:='ERROR';
	END IF;

	num_errors:=l_errors_cnt;

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='COPYDEPENDENTS', 
	  last_operation_id=4, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;

	RAISE NOTICE 'Status: %', l_status;

END;
$$;	

----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dbms_redefinition.finish_redef_table(
	uname 			IN name,
	orig_table 		IN name,
	int_table 		IN name,
	ignore_errors 	IN boolean DEFAULT False,
	debug_mode 		IN boolean DEFAULT False
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE

	c_object_temporary_name 	CONSTANT name := '__dbms_redefinition_temp';
	l_last_oper_record 			dbms_redefinition.redef_status%ROWTYPE;
	l_redef_object_map_element 	record;
	l_external_fk 				record;
	l_status 					varchar(30);
	l_notes 					text;
	l_cnt 						numeric;
	l_errors_cnt 				numeric;
	l_sql_text01 				text;
	l_sql_text02 				text;
	l_bool_flag 				boolean;

BEGIN
    -- operation_id=5
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'First, execute the "can_redef_table" procedure for the table %.%', uname, orig_table;
			RETURN;
	END;
	IF l_last_oper_record.last_operation_id < 2 THEN
		RAISE NOTICE 'First, execute the "start_redef_table" procedure for the table %.%', uname, orig_table;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id <> 5 AND l_last_oper_record.last_operation_status <> 'OK' THEN
		RAISE NOTICE 'The previous step ("%") was completed with an error, the action cannot be continued', l_last_oper_record.last_operation;
		RETURN;
	END IF;
	IF l_last_oper_record.last_operation_id = 5 AND l_last_oper_record.last_operation_status = 'OK' THEN
		RAISE NOTICE 'Redifinition for table %.% already successfully completed ', uname, orig_table;
		RETURN;
	END IF;	
	IF l_last_oper_record.last_operation_id = 6 THEN
		RAISE NOTICE 'Redifinition for table %.% has already been cancelled', uname, orig_table;
		RETURN;
	END IF;

	l_status	:= 'OK';
	l_errors_cnt:= 0;
	l_notes		:= null;

    BEGIN
		l_sql_text01:='LOCK TABLE '||quote_ident(uname)||'.'||quote_ident(orig_table)||' IN EXCLUSIVE MODE NOWAIT;';
		IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
		EXECUTE l_sql_text01;
		l_sql_text01:='LOCK TABLE '||quote_ident(uname)||'.'||quote_ident(int_table)||' IN EXCLUSIVE MODE NOWAIT;';
		IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
		EXECUTE l_sql_text01;
		
		-- Sync data
		PERFORM dbms_redefinition.sync_interim_table(uname,orig_table,int_table,debug_mode);
		
		-- Rename table objects 
		FOR l_redef_object_map_element IN (
			SELECT m.*,
				   CASE
					WHEN m.map_type = 'CONSTRAINT' THEN 1
					WHEN m.map_type = 'INDEX' THEN 2 
					WHEN m.map_type = 'TRIGGER' THEN 3 
					ELSE 10
				   END AS sort_field			
			FROM dbms_redefinition.redef_object_map m
			WHERE m.obj_owner=uname
			  AND m.obj_name=orig_table
			  AND m.orig_obj_name IS NOT NULL
			  AND m.new_obj_name IS NOT NULL
			  AND coalesce(m.notes,' ')<>'SELF_REGISTERED' 
			ORDER BY  sort_field 
		) LOOP
		
			BEGIN
				l_notes:=null;
			
				IF l_redef_object_map_element.map_type = 'CONSTRAINT' THEN
					l_sql_text01:='ALTER TABLE '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.obj_name)||' RENAME CONSTRAINT '||quote_ident(l_redef_object_map_element.orig_obj_name)||' TO '||quote_ident(c_object_temporary_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER TABLE '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(int_table)||' RENAME CONSTRAINT '||quote_ident(l_redef_object_map_element.new_obj_name)||' TO '||quote_ident(l_redef_object_map_element.orig_obj_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER TABLE '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.obj_name)||' RENAME CONSTRAINT '||quote_ident(c_object_temporary_name)||' TO '||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
					IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
					EXECUTE l_sql_text01;
				END IF;
				
				IF l_redef_object_map_element.map_type = 'INDEX' THEN
					l_sql_text01:='ALTER INDEX '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.orig_obj_name)||' RENAME TO '||quote_ident(c_object_temporary_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER INDEX '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.new_obj_name)||' RENAME TO '||quote_ident(l_redef_object_map_element.orig_obj_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER INDEX '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(c_object_temporary_name)||' RENAME TO '||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
					IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
					EXECUTE l_sql_text01;
				END IF;
				
				IF l_redef_object_map_element.map_type = 'TRIGGER' THEN
					-- Enable Trigger
					l_sql_text01 := 'ALTER TABLE '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(int_table)||' ENABLE TRIGGER '||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
					-- Rename Trigger
					l_sql_text01:=l_sql_text01||'ALTER TRIGGER '||quote_ident(l_redef_object_map_element.orig_obj_name)||' ON '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.obj_name)||' RENAME TO '||quote_ident(c_object_temporary_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER TRIGGER '||quote_ident(l_redef_object_map_element.new_obj_name)||' ON ' ||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(int_table)||' RENAME TO '||quote_ident(l_redef_object_map_element.orig_obj_name)||'; ';
					l_sql_text01:=l_sql_text01||'ALTER TRIGGER '||quote_ident(c_object_temporary_name)||' ON '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.obj_name)||' RENAME TO '||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
					IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
					EXECUTE l_sql_text01;
				END IF;
				
			EXCEPTION
				WHEN OTHERS THEN
					l_errors_cnt:= l_errors_cnt+1;
					l_notes		:= 'Something seems to be wrong with this SQL: ['||l_sql_text01||']';
					RAISE NOTICE '%', l_notes;	
			END;
			IF (NOT ignore_errors AND l_errors_cnt>0) THEN
				l_status := 'ERROR';
				EXIT;
			END IF;
			
		END LOOP;
		
		IF ignore_errors OR l_errors_cnt=0 THEN -- continue
		
			-- Rename table
			l_sql_text01:='ALTER TABLE '||quote_ident(uname)||'.'||quote_ident(orig_table)||' RENAME TO '||quote_ident(c_object_temporary_name)||'; ';
			l_sql_text01:=l_sql_text01||'ALTER TABLE '||quote_ident(uname)||'.'||quote_ident(int_table)||' RENAME TO '||quote_ident(orig_table)||'; ';
			l_sql_text01:=l_sql_text01||'ALTER TABLE '||quote_ident(uname)||'.'||quote_ident(c_object_temporary_name)||' RENAME TO '||quote_ident(int_table)||'; ';
			IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
			EXECUTE l_sql_text01;
			
			-- Recreate FK from external tables (from old to new table)
			FOR l_external_fk IN (
				SELECT nsp.nspname,
					   rel.relname,
					   con.conname, 
					   con.contype,
					   pg_get_constraintdef(con.oid) AS consql
				FROM pg_catalog.pg_constraint con 
				  JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
				  JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
				WHERE con.contype = 'f' 
				  AND (nsp.nspname, con.conname) IN (
						SELECT 
							fk_tco.constraint_schema,
							fk_tco.constraint_name
						FROM information_schema.referential_constraints rco
						JOIN information_schema.table_constraints fk_tco ON rco.constraint_name = fk_tco.constraint_name AND rco.constraint_schema = fk_tco.table_schema
						JOIN information_schema.table_constraints pk_tco ON rco.unique_constraint_name = pk_tco.constraint_name AND rco.unique_constraint_schema = pk_tco.table_schema
						WHERE pk_tco.table_schema = uname 
						  AND pk_tco.table_name = int_table 
						)
			) LOOP
				BEGIN
					l_sql_text01:= 'ALTER TABLE '||quote_ident(l_external_fk.nspname)||'.'||quote_ident(l_external_fk.relname)||' DROP CONSTRAINT IF EXISTS '||quote_ident(l_external_fk.conname)||'; ';
					l_sql_text02:= REPLACE(l_external_fk.consql, ' REFERENCES '||quote_ident(uname)||'.'||quote_ident(int_table), ' REFERENCES '||quote_ident(uname)||'.'||quote_ident(orig_table));
					l_sql_text01:= l_sql_text01||'ALTER TABLE '||quote_ident(l_external_fk.nspname)||'.'||quote_ident(l_external_fk.relname)||' ADD CONSTRAINT '||quote_ident(l_external_fk.conname)||' '||l_sql_text02||'; ';
					IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
					EXECUTE l_sql_text01;
				EXCEPTION
					WHEN OTHERS THEN
						l_errors_cnt:=l_errors_cnt+1;
						l_notes		:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
						RAISE NOTICE '%', l_notes;	
				
				END;
			END LOOP;
			
		END IF; -- IF ignore_errors OR l_errors_cnt=0

	EXCEPTION
		WHEN OTHERS THEN
			l_errors_cnt:=l_errors_cnt+1;
			l_notes		:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
			RAISE NOTICE '%', l_notes;	
	END;

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='FINISH', 
	  last_operation_id=5, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;

	RAISE NOTICE 'Status: %', l_status;

END;
$$;	
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.abort_redef_table(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT False
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE

	l_last_oper_record 			dbms_redefinition.redef_status%ROWTYPE;
	l_redef_object_map_element 	record;
	l_status 					varchar(30);
	l_notes 					text;
	l_cnt 						numeric;
	l_sql_text01 				text;
	l_bool_flag 				boolean;

BEGIN
    -- operation_id=6
    -- Check status of previous steps
	BEGIN
		SELECT *
		INTO STRICT l_last_oper_record
		FROM dbms_redefinition.redef_status	
		WHERE obj_owner=uname
		  AND obj_name=orig_table;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE NOTICE 'Redifinition for  table %.% is not currently being executed', uname, orig_table;
			RETURN;
	END;

	IF l_last_oper_record.last_operation_id = 5 AND l_last_oper_record.last_operation_status = 'OK' THEN
		RAISE NOTICE 'Redifinition for table %.% already successfully completed ', uname, orig_table;
		RETURN;
	END IF;

	l_notes:=null;
	l_status:='OK';
	
	-- Drop intermit table objects
	FOR l_redef_object_map_element IN (
		SELECT m.*,
			   CASE
				WHEN m.map_type = 'CONSTRAINT' THEN 2
				WHEN m.map_type = 'INDEX' THEN 3
				WHEN m.map_type = 'TRIGGER' THEN 1 
				ELSE 10
			   END AS sort_field			
		FROM dbms_redefinition.redef_object_map m
		WHERE m.obj_owner=uname
		  AND m.obj_name=orig_table
		  AND coalesce(m.notes,' ')<>'SELF_REGISTERED'
		ORDER BY  sort_field 
	) LOOP
	
		BEGIN
	
			IF l_redef_object_map_element.map_type = 'CONSTRAINT' THEN
				l_sql_text01:='ALTER TABLE '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(int_table)||' DROP CONSTRAINT IF EXISTS '||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			END IF;
			
			IF l_redef_object_map_element.map_type = 'INDEX' THEN
				l_sql_text01:='DROP INDEX IF EXISTS '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(l_redef_object_map_element.new_obj_name)||'; ';
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			END IF;
			
			IF l_redef_object_map_element.map_type = 'TRIGGER' THEN
				l_sql_text01:='DROP TRIGGER IF EXISTS '||quote_ident(l_redef_object_map_element.new_obj_name)||' ON '||quote_ident(l_redef_object_map_element.obj_owner)||'.'||quote_ident(int_table)||'; ';
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
				EXECUTE l_sql_text01;
			END IF;
		
		EXCEPTION
			WHEN OTHERS THEN
				l_notes:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
				RAISE NOTICE '%', l_notes;	
				l_status:='ERROR';
		END;
	END LOOP;	
	
	-- Drop incremental sync auxiliary objects
	PERFORM dbms_redefinition.internal_syncinc_aux_drop(
	  uname=>uname,
	  orig_table=>orig_table,
	  int_table=>int_table,
	  debug_mode=>debug_mode);

	UPDATE dbms_redefinition.redef_status SET 
	  last_operation='ABORT', 
	  last_operation_id=6, 
	  last_operation_status=l_status, 
	  last_operation_timestamp=NOW(), 
	  notes=l_notes 
	WHERE obj_owner=uname AND obj_name=orig_table;

	RAISE NOTICE 'Status: %', l_status;

END;
$$;	
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.cleanup(
	uname 		IN name,
	orig_table 	IN name
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
	DELETE FROM dbms_redefinition.redef_status 
	WHERE obj_owner=uname AND obj_name=orig_table;
	RAISE NOTICE 'Status: OK';
END;
$$;	
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.internal_syncinc_aux_create(
	uname 	IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT True
) RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	c_aux_name_prefix 			CONSTANT name := 'xxaux';
	rec_aux_obj_rec 			record;
	rec_pk_columns 				record;
	rec_col_map_rec 			record;
	l_pk_array 					text[];
	l_pk_col_list 				text;
	l_pk_col_list_with_datatype text;
	l_pk_col_list_with_prefix 	text;
	l_oid_new_name				name;
	l_notes 					text;
	l_cnt 						numeric;
	l_ok_flag 					boolean;
	l_sql_text01 				text;
	l_sql_text02 				text;
	l_sql_text03 				text;
	l_sql_text04 				text;
	l_sql_sel 					text;
	l_sql_ins 					text;
	l_sql_upd 					text;
	l_sql_del 					text;
	l_sql_where 				text;
	l_aux_table 				name;
	l_aux_trigger 				name;
	l_aux_function 				name;
	l_aux_procedure 			name;

BEGIN
   	
	l_notes:=null;
	l_ok_flag:=True;
	
	-- Check objects for incremental refresh
	SELECT count(*)
	INTO l_cnt 
	FROM dbms_redefinition.redef_object_map
	WHERE obj_owner=uname
	  AND obj_name=orig_table
	  AND map_type IN ('SYSTABLE','SYSTRIGGER','SYSTFUNCTION','SYSSFUNCTION');
	IF l_cnt > 1 THEN
		l_ok_flag:=False;
		RAISE NOTICE 'The objects were registered/created - unregister and drop them before calling this API again';
		FOR rec_aux_obj_rec IN (
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type IN ('SYSTABLE','SYSTRIGGER','SYSTFUNCTION','SYSSFUNCTION')
		  ORDER BY map_type DESC
		) LOOP
			CASE rec_aux_obj_rec.map_type
			WHEN 'SYSTABLE' THEN
				RAISE NOTICE '  TABLE %', uname||'.'||rec_aux_obj_rec.new_obj_name;
			WHEN 'SYSTRIGGER' THEN
				RAISE NOTICE '  TRIGGER %', uname||'.'||rec_aux_obj_rec.new_obj_name;
			WHEN 'SYSTFUNCTION' THEN
				RAISE NOTICE '  FUNCTION %', uname||'.'||rec_aux_obj_rec.new_obj_name;
			WHEN 'SYSSFUNCTION' THEN
				RAISE NOTICE '  FUNCTION %', uname||'.'||rec_aux_obj_rec.new_obj_name;
			END CASE;
		END LOOP;
		RETURN l_ok_flag;
	END IF;

	-- All OK, prepare DDL for auxiliary objects (log table + function + trigger)

	-- Get PK column list
	l_pk_col_list:='';
	l_pk_col_list_with_datatype:='';
	l_pk_col_list_with_prefix:='';
	l_cnt:=0;
	FOR rec_pk_columns IN(
		SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
		FROM   pg_index i
		JOIN   pg_class t ON i.indrelid = t.oid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE  t.relkind = 'r'
		  AND t.relname = orig_table
		  AND n.nspname = uname
		  AND i.indisprimary
		ORDER BY a.attnum  
	) LOOP
		l_cnt:=l_cnt+1;
		IF l_cnt >1 THEN
			l_pk_col_list				:=l_pk_col_list||',';
			l_pk_col_list_with_datatype	:=l_pk_col_list_with_datatype||',';
			l_pk_col_list_with_prefix	:=l_pk_col_list_with_prefix||',';
		END IF;
		l_pk_col_list				:=l_pk_col_list||quote_ident(rec_pk_columns.attname);	
		l_pk_col_list_with_datatype	:=l_pk_col_list_with_datatype||quote_ident(rec_pk_columns.attname)||' '||rec_pk_columns.data_type;
		l_pk_col_list_with_prefix	:=l_pk_col_list_with_prefix||'###.'||quote_ident(rec_pk_columns.attname);
	END LOOP;	
	
	IF l_cnt = 0 THEN
		-- PK not found, use OID
		BEGIN
			SELECT count(*)
			INTO STRICT l_cnt
			FROM pg_catalog.pg_class r
			LEFT JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
			WHERE 1=1
			  AND r.relkind = 'r'
			  AND r.relhasoids = true
			  AND n.nspname = uname 
			  AND r.relname = orig_table;
		EXCEPTION
			WHEN OTHERS THEN 
				l_cnt:=0;
		END;
		IF l_cnt = 0 THEN
			l_ok_flag:=False;
			RAISE NOTICE 'ERROR: Table does not have a primary key or OID column';
			RETURN l_ok_flag;	
		END IF;	

		-- Check mapping for OID COLUMNS
		BEGIN
			SELECT new_obj_name 
			INTO STRICT l_oid_new_name
			FROM dbms_redefinition.redef_object_map
			WHERE obj_owner=uname
			  AND obj_name=orig_table
			  AND map_type='COLUMN'
			  AND lower(orig_obj_name)='oid';
		EXCEPTION
			WHEN NO_DATA_FOUND THEN 
				l_ok_flag:=False;
				RAISE NOTICE 'ERROR: Mapping for OID column missing';
				RETURN l_ok_flag;	
		END;
		l_pk_col_list				:='oid';	
		l_pk_col_list_with_datatype	:='oid bigint';
		l_pk_col_list_with_prefix	:='###.oid';
	ELSE
		l_oid_new_name:=null;
	END IF;

	l_pk_array:=string_to_array(trim(l_pk_col_list), ',');

	-- Auxiliary object names
	l_aux_table		:= SUBSTR(c_aux_name_prefix||'_'||orig_table,1,63-5)||'_tlog';
	l_aux_trigger	:= SUBSTR(c_aux_name_prefix||'_'||orig_table,1,63-5)||'_trg';
	l_aux_function	:= SUBSTR(c_aux_name_prefix||'_'||orig_table,1,63-5)||'_func';
	l_aux_procedure	:= SUBSTR(c_aux_name_prefix||'_'||orig_table,1,63-5)||'_proc';
	
	-- Table DDL
	l_sql_text01:='CREATE TABLE '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||' ('||l_pk_col_list_with_datatype||', ltimestamp timestamp, loptype char(1), sync_flag char(1) default ''N'');';
	
	
	-- Trigger function DDL
	l_sql_text02:='create or replace function '||quote_ident(uname)||'.'||quote_ident(l_aux_function)||'() 
	  returns trigger  
	language plpgsql 
	as  
	$trgfuncbody$ 
	begin 
		if (TG_OP = ''DELETE'') then 
			insert into '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||'('||l_pk_col_list||', ltimestamp, loptype, sync_flag ) VALUES ('||replace(l_pk_col_list_with_prefix,'###.','OLD.')||', now()::timestamp, ''D'', ''N''); 
		elsif (TG_OP = ''UPDATE'') then 
			insert into '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||'('||l_pk_col_list||', ltimestamp, loptype, sync_flag ) VALUES ('||replace(l_pk_col_list_with_prefix,'###.','NEW.')||', now()::timestamp, ''U'', ''N''); 
		elsif (TG_OP = ''INSERT'') then 
			insert into '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||'('||l_pk_col_list||', ltimestamp, loptype, sync_flag ) VALUES ('||replace(l_pk_col_list_with_prefix,'###.','NEW.')||', now()::timestamp, ''I'', ''N''); 
		end if; 
		return NEW; 
	end; 
	$trgfuncbody$;';
	
	-- Trigger DDL
	--l_sql_text03:='create or replace trigger '||quote_ident(l_aux_trigger)||' 
	l_sql_text03:='create trigger '||quote_ident(l_aux_trigger)||' 
	after insert or update or delete on '||quote_ident(uname)||'.'||quote_ident(orig_table)||' 
	for each row 
	execute procedure '||quote_ident(uname)||'.'||quote_ident(l_aux_function)||'();';

	-- Procedure for sync data DDL
	l_sql_ins := 'insert into '||quote_ident(uname)||'.'||quote_ident(int_table)||'(';
	l_sql_sel := ' select ';
	l_cnt	  := 0;
	FOR rec_col_map_rec IN(
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type='COLUMN'
	) LOOP
		l_cnt:=l_cnt+1;
		IF l_cnt>1 THEN
			l_sql_ins:=l_sql_ins||',';
			l_sql_sel:=l_sql_sel||',';
		END IF;
		IF rec_col_map_rec.orig_obj_name IS NULL OR rec_col_map_rec.new_obj_name IS NULL THEN
			l_ok_flag:=False;
		END IF;	
		l_sql_ins:=l_sql_ins||COALESCE(rec_col_map_rec.new_obj_name,'?');
		l_sql_sel:=l_sql_sel||COALESCE(rec_col_map_rec.orig_obj_name,'?');
	END LOOP;
	l_sql_ins:=l_sql_ins||')';
	l_sql_sel:=l_sql_sel||' from '||quote_ident(uname)||'.'||quote_ident(orig_table);
	
	l_sql_ins:=l_sql_ins||l_sql_sel||' where 1=1';
	FOR i IN 1..CARDINALITY(l_pk_array) LOOP
		l_sql_ins:=l_sql_ins||' and '||l_pk_array[i]||'=rec_row.'||l_pk_array[i];
	END LOOP;

	l_sql_upd	:= 'update '||quote_ident(uname)||'.'||quote_ident(int_table)||' t2 set (';
	l_sql_sel	:= 'select ';
	l_sql_where	:= 'where 1=1';
	l_cnt:=0;
	FOR rec_col_map_rec IN(
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type='COLUMN'
	) LOOP
		IF NOT ( rec_col_map_rec.orig_obj_name = ANY (l_pk_array) )THEN
			l_cnt:=l_cnt+1;
			IF l_cnt>1 THEN
				l_sql_upd:=l_sql_upd||',';
				l_sql_sel:=l_sql_sel||',';
			END IF;
		END IF;
		IF rec_col_map_rec.orig_obj_name = ANY (l_pk_array) THEN
			l_sql_where:=l_sql_where||' and t1.'||rec_col_map_rec.orig_obj_name||'=t2.'||rec_col_map_rec.new_obj_name;
		ELSE
			IF rec_col_map_rec.orig_obj_name IS NULL OR rec_col_map_rec.new_obj_name IS NULL THEN
				l_ok_flag:=False;
			END IF;	
			l_sql_upd:=l_sql_upd||COALESCE(rec_col_map_rec.new_obj_name,'?');
			l_sql_sel:=l_sql_sel||COALESCE(rec_col_map_rec.orig_obj_name,'?');
		END IF;
	END LOOP;

    l_sql_upd:= l_sql_upd||') = ('||l_sql_sel||' from '||quote_ident(uname)||'.'||quote_ident(orig_table)||' t1 '||l_sql_where||' ) where 1=1';
	IF l_oid_new_name IS NULL THEN
		FOR i IN 1..CARDINALITY(l_pk_array) LOOP
			l_sql_upd:=l_sql_upd||' and '||l_pk_array[i]||'=rec_row.'||l_pk_array[i];
		END LOOP;
	ELSE
		l_sql_upd:=l_sql_upd||' and '||l_oid_new_name||'=rec_row.'||l_pk_array[1];
	END IF;
	
	l_sql_del:='delete from '||quote_ident(uname)||'.'||quote_ident(int_table)||' where 1=1';
	IF l_oid_new_name IS NULL THEN
		FOR i IN 1..CARDINALITY(l_pk_array) LOOP
			l_sql_del:=l_sql_del||' and '||l_pk_array[i]||'=rec_row.'||l_pk_array[i];
		END LOOP;
	ELSE
		l_sql_del:=l_sql_del||' and '||l_oid_new_name||'=rec_row.'||l_pk_array[1];
	END IF;
	
	l_sql_text04:='create or replace function '||quote_ident(uname)||'.'||quote_ident(l_aux_procedure)||'() returns void 
	language plpgsql 
	security invoker
	as  
	$procbody$ 
	declare
		rec_row record; 
		l_result char(1);
	begin 
		for rec_row in ( select ctid, * from '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||' where sync_flag=''N'' order by ltimestamp ) loop
			begin 
				case rec_row.loptype 
					when ''I'' then 
						'||l_sql_ins||'; 
					when ''U'' then 
						'||l_sql_upd||'; 
					when ''D'' then
						'||l_sql_del||'; 
				end case; 
				l_result:=''Y''; 
			exception 
			  when others then 
			    l_result:=''E''; 
			end; 
			update '||quote_ident(uname)||'.'||quote_ident(l_aux_table)||' set sync_flag=l_result where ctid=rec_row.ctid; 
	end loop;
	end; 
	$procbody$;';
	
	-- Create Table
	BEGIN
		IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
		EXECUTE l_sql_text01;
		DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=uname AND obj_name=orig_table AND map_type='SYSTABLE' AND new_obj_name=l_aux_table;
		INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, new_obj_name)
		  VALUES(uname, orig_table, 'SYSTABLE', l_aux_table);
	EXCEPTION
		WHEN OTHERS THEN
			l_notes:='Something seems to be wrong with this SQL: ['||l_sql_text01||']';
			l_ok_flag:=False;
			RAISE NOTICE '[%]', l_notes;
	END;

	IF 	l_ok_flag THEN
		-- Create Function
		BEGIN
			IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text02; END IF;
			EXECUTE l_sql_text02;
			DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=uname AND obj_name=orig_table AND map_type='SYSTFUNCTION' AND new_obj_name=l_aux_function;
			INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, new_obj_name)
			  VALUES(uname, orig_table, 'SYSTFUNCTION', l_aux_function);
		EXCEPTION
			WHEN OTHERS THEN
				l_ok_flag:=False;
				l_notes	 :='Something seems to be wrong with this SQL: ['||l_sql_text02||']';
				RAISE NOTICE '[%]', l_notes;
		END;
		
		IF 	l_ok_flag THEN
			-- Create Trigger
			BEGIN
				IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text03; END IF;
				EXECUTE l_sql_text03;
				DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=uname AND obj_name=orig_table AND map_type='SYSTRIGGER' AND new_obj_name=l_aux_trigger;
				INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, new_obj_name)
				  VALUES(uname, orig_table, 'SYSTRIGGER', l_aux_trigger);
			EXCEPTION
				WHEN OTHERS THEN
					l_ok_flag:=False;
					l_notes	 :='Something seems to be wrong with this SQL: ['||l_sql_text03||']';
					RAISE NOTICE '[%]', l_notes;
			END;

			IF 	l_ok_flag THEN
				-- Create Sync Function
				BEGIN
					IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text04; END IF;
					EXECUTE l_sql_text04;
					DELETE FROM dbms_redefinition.redef_object_map WHERE obj_owner=uname AND obj_name=orig_table AND map_type='SYSSFUNCTION' AND new_obj_name=l_aux_procedure;
					INSERT INTO dbms_redefinition.redef_object_map(obj_owner, obj_name, map_type, new_obj_name)
					  VALUES(uname, orig_table, 'SYSSFUNCTION', l_aux_procedure);
				EXCEPTION
					WHEN OTHERS THEN
						l_ok_flag:=False;
						l_notes	 :='Something seems to be wrong with this SQL: ['||l_sql_text04||']';
						RAISE NOTICE '[%]', l_notes;
				END;
			END IF;
		END IF;
		
	END IF;
	
	RETURN l_ok_flag;
	
END;
$$;	

----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbms_redefinition.internal_syncinc_aux_drop(
	uname 		IN name,
	orig_table 	IN name,
	int_table 	IN name,
	debug_mode 	IN boolean DEFAULT False
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
	rec_aux_obj_rec record;
	l_sql_text01 	text;
BEGIN
	-- Drop/unregister old auxiliary objects
	FOR rec_aux_obj_rec IN (
		SELECT * 
		FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type IN ('SYSTABLE','SYSTRIGGER','SYSTFUNCTION','SYSSFUNCTION')
		  ORDER BY map_type DESC
	) LOOP
		l_sql_text01:='DROP ';
		CASE rec_aux_obj_rec.map_type
		WHEN 'SYSTRIGGER' THEN
			l_sql_text01:=l_sql_text01||' TRIGGER IF EXISTS '||quote_ident(rec_aux_obj_rec.new_obj_name)||' ON '||quote_ident(uname)||'.'||quote_ident(orig_table); 
		WHEN 'SYSSFUNCTION' THEN
			l_sql_text01:=l_sql_text01||' FUNCTION IF EXISTS '||quote_ident(uname)||'.'||quote_ident(rec_aux_obj_rec.new_obj_name)||'()'; 
		WHEN 'SYSTFUNCTION' THEN
			l_sql_text01:=l_sql_text01||' FUNCTION IF EXISTS '||quote_ident(uname)||'.'||quote_ident(rec_aux_obj_rec.new_obj_name)||'()'; 
		WHEN 'SYSTABLE' THEN
			l_sql_text01:=l_sql_text01||' TABLE IF EXISTS ' ||quote_ident(uname)||'.'||quote_ident(rec_aux_obj_rec.new_obj_name); 
		END CASE;
		
		IF debug_mode THEN 	RAISE NOTICE '#DEBUG: %', l_sql_text01; END IF;
		
		EXECUTE l_sql_text01;
		DELETE FROM dbms_redefinition.redef_object_map
		WHERE obj_owner=uname
		  AND obj_name=orig_table
		  AND map_type=rec_aux_obj_rec.map_type
		  AND new_obj_name=rec_aux_obj_rec.new_obj_name;
	END LOOP;
END;
$$;	

----------------------------------------------------------------------
