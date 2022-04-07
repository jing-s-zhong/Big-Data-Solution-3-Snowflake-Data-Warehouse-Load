-- 
-- default database should be HST or HSTORY
--
USE DATABASE HST;

/********************************************************************
 ** Data Warehouse Load Target Configuration 
 ********************************************************************/
--
-- Create warehouse target config data
--
USE SCHEMA _METADATA;
/*
TRUNCATE TABLE CTRL_SOURCE;
*/

MERGE INTO CTRL_SOURCE D
USING (
    SELECT TC.TARGET_ID,
        SOURCE_LABEL,
        SOURCE_DATA,
        SOURCE_ENABLED,
        FIELD_MAP,
        TRANSFORMATION
    FROM (
        SELECT $1 TARGET_DATA,
            $2 SOURCE_LABEL,
            $3 SOURCE_DATA,
            $4 SOURCE_ENABLED,
            PARSE_JSON($5) FIELD_MAP,
            $6 TRANSFORMATION
        FROM VALUES 
        (
            'ONTOLOGY.PERSON',
            'INT.CILENT1_EMAIL_SALES.PERSON',
            'INT.CILENT1_EMAIL_SALES.DIGEST_PERSON',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.PERSON',
            'INT.CILENT2_EMAIL_TECHS.PERSON',
            'INT.CILENT2_EMAIL_TECHS.DIGEST_PERSON',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.EMAIL_MESSAGE',
            'INT.CILENT1_EMAIL_SALES.MESSAGE',
            'INT.CILENT1_EMAIL_SALES.DIGEST_MESSAGE',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.EMAIL_MESSAGE',
            'INT.CILENT2_EMAIL_TECHS.MESSAGE',
            'INT.CILENT2_EMAIL_TECHS.DIGEST_MESSAGE',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.EMAIL_ADDRESS',
            'INT.CILENT1_EMAIL_SALES.EMAIL_ADDRESS',
            'INT.CILENT1_EMAIL_SALES.DIGEST_EMAIL_ADDRESS',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.EMAIL_ADDRESS',
            'INT.CILENT2_EMAIL_TECHS.EMAIL_ADDRESS',
            'INT.CILENT2_EMAIL_TECHS.DIGEST_EMAIL_ADDRESS',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ONTOLOGY.PERSON_EMAIL_ADDRESS',
            'INT.CILENT1_EMAIL_SALES.PERSON_EMAIL',
            'INT.CILENT1_EMAIL_SALES.DIGEST_PERSON_EMAIL',
            TRUE,
            $${}$$,
            $$
            SELECT P.PERSON_ID,
                E.EMAIL_ADDRESS_ID,
                PI.ORGANIZATION_ID,
                E.DISPLAY_NAME[0] /*EMAIL_ADDRESS*/ = P.FULL_NAME PRIMARY_EMAIL,
                PE.PLATFORM_ID,
                PE.DATA_KEY,
                PE.DATA_HASH,
                PE.LOAD_TIME
            FROM INT.CILENT1_EMAIL_SALES.DIGEST_PERSON_EMAIL PE
            JOIN INT.CILENT1_EMAIL_SALES.PERSON PI
            ON PE.PERSON_ID = PI.PERSON_ID
            JOIN ONTOLOGY.PERSON P
            ON PI.DATA_KEY = P.DATA_KEY
            JOIN ONTOLOGY.EMAIL_ADDRESS E
            ON PE.EMAIL = E.EMAIL_ADDRESS
            $$
        ),
        (
            'ONTOLOGY.PERSON_EMAIL_ADDRESS',
            'INT.CILENT2_EMAIL_TECHS.PERSON_EMAIL',
            'INT.CILENT2_EMAIL_TECHS.DIGEST_PERSON_EMAIL',
            TRUE,
            $${}$$,
            $$
            SELECT P.PERSON_ID,
                E.EMAIL_ADDRESS_ID,
                PI.ORGANIZATION_ID,
                E.DISPLAY_NAME[0] /*EMAIL_ADDRESS*/ = P.FULL_NAME PRIMARY_EMAIL,
                PE.PLATFORM_ID,
                PE.DATA_KEY,
                PE.DATA_HASH,
                PE.LOAD_TIME
            FROM INT.CILENT2_EMAIL_TECHS.DIGEST_PERSON_EMAIL PE
            JOIN INT.CILENT2_EMAIL_TECHS.PERSON PI
            ON PE.PERSON_ID = PI.PERSON_ID
            JOIN ONTOLOGY.PERSON P
            ON PI.DATA_KEY = P.DATA_KEY
            JOIN ONTOLOGY.EMAIL_ADDRESS E
            ON PE.EMAIL = E.EMAIL_ADDRESS
            $$
        ),
        (
            'ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT',
            'INT.CILENT1_EMAIL_SALES.MESSAGE_EMAIL',
            'INT.CILENT1_EMAIL_SALES.DIGEST_MESSAGE_EMAIL',
            TRUE,
            $${}$$,
            $$
            SELECT M.MESSAGE_ID,
                S.EMAIL_ADDRESS_ID SENDER_EMAIL_ADDRESS_ID,
                S.DISPLAY_NAME[0]::VARCHAR SENDER_DISPLAY_NAME,
                R.EMAIL_ADDRESS_ID RECIPIENT_EMAIL_ADDRESS_ID,
                R.DISPLAY_NAME[0]::VARCHAR RECIPIENT_DISPLAY_NAME,
                ME.RECIPIENT_TYPE PARTICIPATION_TYPE,
                ME.PLATFORM_ID,
                ME.DATA_KEY,
                ME.DATA_HASH,
                ME.LOAD_TIME
            FROM INT.CILENT1_EMAIL_SALES.DIGEST_MESSAGE_EMAIL ME
            JOIN INT.CILENT1_EMAIL_SALES.MESSAGE MI
            ON ME.MESSAGE_ID = MI.MESSAGE_ID
            JOIN ONTOLOGY.EMAIL_MESSAGE M
            ON MI.DATA_KEY = M.DATA_KEY AND M.VALID_TO IS NULL
            JOIN ONTOLOGY.EMAIL_ADDRESS S
            ON ME.SENDER = S.EMAIL_ADDRESS AND S.VALID_TO IS NULL
            JOIN ONTOLOGY.EMAIL_ADDRESS R
            ON ME.RECIPIENT_EMAIL_ADDRESS = R.EMAIL_ADDRESS AND R.VALID_TO IS NULL
            $$
        ),
        (
            'ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT',
            'INT.CILENT2_EMAIL_TECHS.MESSAGE_EMAIL',
            'INT.CILENT2_EMAIL_TECHS.DIGEST_MESSAGE_EMAIL',
            TRUE,
            $${}$$,
            $$
            SELECT M.MESSAGE_ID,
                S.EMAIL_ADDRESS_ID SENDER_EMAIL_ADDRESS_ID,
                S.DISPLAY_NAME[0]::VARCHAR SENDER_DISPLAY_NAME,
                R.EMAIL_ADDRESS_ID RECIPIENT_EMAIL_ADDRESS_ID,
                R.DISPLAY_NAME[0]::VARCHAR RECIPIENT_DISPLAY_NAME,
                ME.RECIPIENT_TYPE PARTICIPATION_TYPE,
                ME.PLATFORM_ID,
                ME.DATA_KEY,
                ME.DATA_HASH,
                ME.LOAD_TIME
            FROM INT.CILENT2_EMAIL_TECHS.DIGEST_MESSAGE_EMAIL ME
            JOIN INT.CILENT2_EMAIL_TECHS.MESSAGE MI
            ON ME.MESSAGE_ID = MI.MESSAGE_ID
            JOIN ONTOLOGY.EMAIL_MESSAGE M
            ON MI.DATA_KEY = M.DATA_KEY AND M.VALID_TO IS NULL
            JOIN ONTOLOGY.EMAIL_ADDRESS S
            ON ME.SENDER = S.EMAIL_ADDRESS AND S.VALID_TO IS NULL
            JOIN ONTOLOGY.EMAIL_ADDRESS R
            ON ME.RECIPIENT_EMAIL_ADDRESS = R.EMAIL_ADDRESS AND R.VALID_TO IS NULL
            $$
        ),
        (
            'ONTOLOGY.PERSON_CONTACT',
            'ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT',
            'ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT',
            TRUE,
            $${}$$,
            $$
            SELECT S.PERSON_ID PERSON_ID,
                R.PERSON_ID CONTACT_ID,
                TRUE INTERNAL_CONTACT,
                TRUE VIEWABLE_CONTACT,
                1 RELATIONSHIP_SCORE,
                MP.LOAD_TIME LAST_ACTIVITY_TIME,
                MP.LOAD_TIME::DATE BUSINESS_RUN_DATE,
                MP.PLATFORM_ID,
                MP.DATA_KEY,
                MP.DATA_HASH,
                MP.LOAD_TIME
            FROM ONTOLOGY.EMAIL_MESSAGE_PARTICIPATANT MP
            JOIN ONTOLOGY.PERSON_EMAIL_ADDRESS S
            ON MP.SENDER_EMAIL_ADDRESS_ID = S.PERSON_EMAIL_ADDRESS_ID 
            AND S.VALID_TO IS NULL
            JOIN ONTOLOGY.PERSON_EMAIL_ADDRESS R
            ON MP.RECIPIENT_EMAIL_ADDRESS_ID = R.PERSON_EMAIL_ADDRESS_ID 
            AND R.VALID_TO IS NULL
            WHERE MP.SENDER_EMAIL_ADDRESS_ID != MP.RECIPIENT_EMAIL_ADDRESS_ID
            $$
        )
    ) SC
    JOIN _METADATA.CTRL_TARGET TC
    ON SC.TARGET_DATA = TC.TARGET_DATA
) S
ON D.SOURCE_DATA = S.SOURCE_DATA
WHEN MATCHED THEN
    UPDATE SET
        TARGET_ID = S.TARGET_ID,
        SOURCE_LABEL = S.SOURCE_LABEL,
        SOURCE_DATA = S.SOURCE_DATA,
        SOURCE_ENABLED = S.SOURCE_ENABLED,
        FIELD_MAP = S.FIELD_MAP,
        TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN 
    INSERT (
        TARGET_ID, 
        SOURCE_LABEL, 
        SOURCE_DATA, 
        SOURCE_ENABLED, 
        FIELD_MAP, 
        TRANSFORMATION
    )
    VALUES (
        S.TARGET_ID, 
        S.SOURCE_LABEL, 
        S.SOURCE_DATA, 
        S.SOURCE_ENABLED, 
        S.FIELD_MAP, 
        S.TRANSFORMATION
    );



 /********************************************************************
  ** Schedule The Data Warehouse Load Manually
  ********************************************************************/
USE SCHEMA HST._METADATA;

CALL CTRL_TASK_SCHEDULER('NODE', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('NODE', 'WORK');
CALL CTRL_TASK_SCHEDULER('EDGE', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('EDGE', 'WORK');
