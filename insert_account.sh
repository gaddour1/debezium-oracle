#!/bin/bash

export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export PATH=$ORACLE_HOME/bin:$PATH

sqlplus -s C##DBZUSER/dbz <<EOF

SET SERVEROUTPUT ON
SET FEEDBACK OFF
SET VERIFY OFF

BEGIN
    INSERT INTO ACCOUNT (RECID, XMLRECORD)
    VALUES (300022, '<row><c1>testtplsql</c1></row>');

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Inserted 300022');
END;
/

EXIT;
EOF

