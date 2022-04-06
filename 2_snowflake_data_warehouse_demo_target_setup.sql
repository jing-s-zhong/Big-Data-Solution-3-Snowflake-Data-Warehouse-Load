-- 
-- default database should be HST or HSTORY
--
USE DATABASE HST;

/********************************************************************
 ** Data warehouse Schema Create Section (Inmon Model)
 ********************************************************************/
--
-- Create DW tables
--
DROP SCHEMA IF EXISTS ONTOLOGY;
CREATE OR REPLACE SCHEMA ONTOLOGY;
DROP SCHEMA IF EXISTS HISTORY;
CREATE OR REPLACE SCHEMA HISTORY;

DROP TABLE IF EXISTS HISTORY.PERSON;
CREATE OR REPLACE TABLE HISTORY.PERSON (
	PERSON_ID NUMBER, 
	FULL_NAME TEXT, 
	FIRST_NAME TEXT, 
	LAST_NAME TEXT, 
	TITLE TEXT, 
	PHOTO_URL TEXT, 
	VIEWABLE BOOLEAN DEFAULT TRUE,
	SCORE FLOAT, 
    PLATFORM_ID NUMBER,
    ORGANIZATION_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.PERSON;
CREATE OR REPLACE TABLE ONTOLOGY.PERSON (
	PERSON_ID NUMBER IDENTITY, 
	FULL_NAME TEXT, 
	FIRST_NAME TEXT, 
	LAST_NAME TEXT, 
	TITLE TEXT, 
	PHOTO_URL TEXT, 
	VIEWABLE BOOLEAN DEFAULT TRUE,
	SCORE FLOAT, 
    PLATFORM_ID NUMBER,
    ORGANIZATION_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.EMAIL_MESSAGE;
CREATE OR REPLACE TABLE ONTOLOGY.EMAIL_MESSAGE (
	MESSAGE_ID NUMBER IDENTITY, 
	SUBJECT TEXT, 
	SENDER TEXT, 
	BODY TEXT, 
	REFERENCES VARIANT, 
	SENT_AT TIMESTAMP_NTZ, 
	PROBABLY_REPLY BOOLEAN, 
	AUTO_RESPONSE_TYPE TEXT, 
	IN_REPLY_TO TEXT, 
	ORIGINAL_ID TEXT, 
	THREAD_ID NUMBER, 
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.EMAIL_ADDRESS;
CREATE OR REPLACE TABLE ONTOLOGY.EMAIL_ADDRESS (
	EMAIL_ADDRESS_ID NUMBER IDENTITY, 
	EMAIL_ADDRESS VARCHAR, 
	DISPLAY_NAME VARIANT, 
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.PERSON_EMAIL_ADDRESS;
CREATE OR REPLACE TABLE ONTOLOGY.PERSON_EMAIL_ADDRESS (
	PERSON_EMAIL_ADDRESS_ID NUMBER IDENTITY, 
	PERSON_ID NUMBER, 
	EMAIL_ADDRESS_ID NUMBER, 
	ORGANIZATION_ID NUMBER, 
	PRIMARY_EMAIL BOOLEAN, 
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT;
CREATE OR REPLACE TABLE ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT (
	EMAIL_MESSAGE_PARTICIPATION_ID NUMBER IDENTITY,
	MESSAGE_ID NUMBER, 
	SENDER_EMAIL_ADDRESS_ID NUMBER, 
	SENDER_DISPLAY_NAME VARCHAR, 
	RECIPIENT_EMAIL_ADDRESS_ID NUMBER, 
	RECIPIENT_DISPLAY_NAME VARCHAR, 
    PARTICIPATION_TYPE VARCHAR,
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ONTOLOGY.PERSON_CONTACT;
CREATE OR REPLACE TABLE ONTOLOGY.PERSON_CONTACT (
	PERSON_CONTACT_ID NUMBER IDENTITY,
	PERSON_ID NUMBER, 
	CONTACT_ID NUMBER, 
    INTERNAL_CONTACT BOOLEAN,
    VIEWABLE_CONTACT BOOLEAN DEFAULT TRUE,
    RELATIONSHIP_SCORE FLOAT,
    LAST_ACTIVITY_TIME TIMESTAMP_NTZ,
	BUSINESS_RUN_DATE DATE, 
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

/********************************************************************
 ** Data Warehouse Load Target Configuration 
 ********************************************************************/
--
-- Create warehouse target config data
--
USE SCHEMA _METADATA;
/*
TRUNCATE TABLE CTRL_TARGET;
TRUNCATE TABLE CTRL_SOURCE;
*/

MERGE INTO CTRL_TARGET D
USING (
    SELECT $1 TARGET_LABEL,
        $2 TARGET_TYPE,
        $3 TARGET_DATA,
        $4 HISTORY_DATA,
        $5 PROCESS_PRIORITY,
        $6 SCD_TYPE,
        PARSE_JSON('{
            "DATA_KEY_FIELD": "DATA_KEY", 
            "DATA_HASH_FIELD": "DATA_HASH",
            "DATA_TIME_FIELD": "LOAD_TIME",
            "VALID_FROM_FIELD": "VALID_FROM",
            "VALID_TO_FIELD": "VALID_TO"
            }') CTRL_FIELD,
        PARSE_JSON($7) DATA_FIELD,
        PARSE_JSON($8) META_FIELD
    FROM VALUES 
    (
        'PERSON',
        'NODE',
        'ONTOLOGY.PERSON',
        'HISTORY.PERSON',
        1,
        4,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FULL_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "ORGANIZATION_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'EMAIL_MESSAGE',
        'NODE',
        'ONTOLOGY.EMAIL_MESSAGE',
        'HISTORY.EMAIL_MESSAGE',
        1,
        2,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SUBJECT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BODY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "REFERENCES",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENT_AT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PROBABLY_REPLY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "AUTO_RESPONSE_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "IN_REPLY_TO",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "ORIGINAL_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "THREAD_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'EMAIL_ADDRESS',
        'NODE',
        'ONTOLOGY.EMAIL_ADDRESS',
        'HISTORY.EMAIL_ADDRESS',
        1,
        2,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'PERSON_EMAIL_ADDRESS',
        'EDGE',
        'ONTOLOGY.PERSON_EMAIL_ADDRESS',
        'HISTORY.PERSON_EMAIL_ADDRESS',
        2,
        2,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "ORGANIZATION_ID",
                "FIELD_TRANS": "NULL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PRIMARY_EMAIL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'EMAIL_MESSAGE_PARTICIPATANT',
        'EDGE',
        'ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT',
        'HISTORY.EMAIL_MESSAGE_PARTICIPATANT',
        3,
        2,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "EMAIL_MESSAGE_PARTICIPATION_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "SENDER_EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER_DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "RECIPIENT_EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RECIPIENT_DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PARTICIPATION_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'PERSON_CONTACT',
        'EDGE',
        'ONTOLOGY.PERSON_CONTACT',
        'HISTORY.PERSON_CONTACT',
        4,
        2,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_CONTACT_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "CONTACT_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "INTERNAL_CONTACT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RELATIONSHIP_SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_ACTIVITY_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "DATE"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    )
) S
ON D.TARGET_DATA = S.TARGET_DATA
WHEN MATCHED THEN
    UPDATE SET
        TARGET_LABEL = S.TARGET_LABEL,
        TARGET_TYPE = S.TARGET_TYPE,
        HISTORY_DATA = S.HISTORY_DATA,
        PROCESS_PRIORITY = S.PROCESS_PRIORITY,
        SCD_TYPE = S.SCD_TYPE,
        CTRL_FIELD = S.CTRL_FIELD,
        DATA_FIELD = S.DATA_FIELD,
        META_FIELD = S.META_FIELD
WHEN NOT MATCHED THEN 
    INSERT (
        TARGET_LABEL, 
        TARGET_TYPE, 
        TARGET_DATA, 
        HISTORY_DATA, 
        PROCESS_PRIORITY, 
        SCD_TYPE, 
        CTRL_FIELD,
        DATA_FIELD, 
        META_FIELD
    )
    VALUES (
        S.TARGET_LABEL, 
        S.TARGET_TYPE, 
        S.TARGET_DATA, 
        S.HISTORY_DATA, 
        S.PROCESS_PRIORITY, 
        S.SCD_TYPE, 
        S.CTRL_FIELD,
        S.DATA_FIELD, 
        S.META_FIELD 
    );



 /********************************************************************
 ** Schema Update Manually
 ********************************************************************/
USE SCHEMA HST._METADATA;
/*
CALL CTRL_TASK_SCHEDULER('NODE', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('NODE', 'WORK');
CALL CTRL_TASK_SCHEDULER('EDGE', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('EDGE', 'WORK');
*/