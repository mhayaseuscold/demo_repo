USE SCHEMA TRANSFORM;
CREATE OR REPLACE PROCEDURE "SP_CDC_LOCATE_BACKUP_CLEAN_UP"("TABLE_NAME" VARCHAR(16777216), "STARTMNTDATE" VARCHAR(16777216), "ENDMNTDATE" VARCHAR(16777216), "DL_FILE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

snowflake.execute ({sqlText:`TRUNCATE TABLE STAGING.LOCATE_DATA_FIX_STG;`})
snowflake.execute ({sqlText:`COPY INTO STAGING.LOCATE_DATA_FIX_STG FROM (
SELECT
    $1:LOPNT,
	$1:LOCUS,
	$1:LOLOT,
	$1:LOLTX,
	$1:LOLOC,
	$1:LOQTY,
	$1:LOLIC,
	$1:LOPID,
	$1:LOTYP,
	$1:LOSTS,
	$1:LOTIME,
	$1:LOMDTE,
	$1:LOMPGM,
	$1:SOURCEDBMS,
	$1:DTL__CAPXRESTART1,
	$1:DTL__CAPXRESTART2,
	$1:DTL__CAPXUOW,
	$1:DTL__CAPXUSER,
	$1:DTL__CAPXTIMESTAMP,
	$1:DTL__CAPXACTION,
	$1:DTL__CAPXCASDELIND,
	$1:DTL__CAPXRRN,
	$1:CDC_INS_DT,
	$1:CDC_UPD_DT,
	$1:CDC_INS_USR,
	$1:CDC_UPD_USR,
	$1:DL_INS_DT,
	$1:DL_INS_USR,
	convert_timezone(''America/Los_Angeles'', ''America/New_York'', current_timestamp::timestamp_ntz),
	NULL,
	''ADF_USER'',
	NULL,
	''WMS''
  FROM @STAGING.sf_dl_wms_parquet_stage/CDC_LOCATE/`+ DL_FILE_NAME +`)
`})
  
  snowflake.execute ({sqlText:`MERGE INTO TRANSFORM.FACT_LOCATION_QUARANTINE TGT
USING
(
  SELECT TGT.* FROM TRANSFORM.FACT_LOCATION TGT
LEFT JOIN STAGING.LOCATE_DATA_FIX_STG STG
ON 	TGT.PLANT_ID = STG.LOPNT
AND TGT.CUSTOMER_ID = STG.LOCUS
AND TGT.LOT_ID = STG.LOLOT
AND TGT.LOT_EXTENDED_ID = STG.LOLTX
--AND TGT.LOCATION_ID = STG.LOLOC
AND TGT.LICENSE_ID = STG.LOLIC
AND TGT.SOURCEDBMS = STG.SOURCEDBMS
--AND TGT.LAST_MAINTENANCE_DATE = STG.LOMDTE
WHERE STG.LOPNT IS NULL AND TGT.LAST_MAINTENANCE_DATE BETWEEN `+ STARTMNTDATE +` AND `+ ENDMNTDATE +`
) STG
ON
  TGT.PLANT_ID = STG.PLANT_ID
AND TGT.CUSTOMER_ID = STG.CUSTOMER_ID
AND TGT.LOT_ID = STG.LOT_ID
AND TGT.LOT_EXTENDED_ID = STG.LOT_EXTENDED_ID
--AND TGT.LOCATION_ID = STG.LOCATION_ID
AND TGT.LICENSE_ID = STG.LICENSE_ID
AND TGT.SOURCEDBMS = STG.SOURCEDBMS
WHEN NOT MATCHED THEN
INSERT
(
	TGT.PLANT_ID,
	TGT.CUSTOMER_ID,
	TGT.LOT_ID,
	TGT.LOT_EXTENDED_ID,
	TGT.LOCATION_ID,
	TGT.LOCATION_QUANTITY,
	TGT.LICENSE_ID,
	TGT.PALLET_ID,
	TGT.LOCATION_TYPE,
	TGT.STATUS_FLAG,
	TGT.LAST_MAINTENANCE_TIME,
	TGT.LAST_MAINTENANCE_DATE,
	TGT.LAST_MAINTENANCE_PROGRAM,
	TGT.LAST_MAINTENANCE_DATETIME,
	TGT.SOURCEDBMS,
	TGT.SOURCE_SYSTEM,
	TGT.CDC_INS_DT,
	TGT.CDC_INS_USR,
	TGT.CDC_UPD_DT,
	TGT.CDC_UPD_USR,
	TGT.DL_INS_DT,
	TGT.DL_INS_USR,
	TGT.CDW_INS_DT,
	TGT.CDW_INS_USR,
	TGT.CDW_UPD_DT,
	TGT.CDW_UPD_USR,
	TGT.QUARANTINE_DATE
)
VALUES
(
	STG.PLANT_ID,
	STG.CUSTOMER_ID,
	STG.LOT_ID,
	STG.LOT_EXTENDED_ID,
	STG.LOCATION_ID,
	STG.LOCATION_QUANTITY,
	STG.LICENSE_ID,
	STG.PALLET_ID,
	STG.LOCATION_TYPE,
	STG.STATUS_FLAG,
	STG.LAST_MAINTENANCE_TIME,
	STG.LAST_MAINTENANCE_DATE,
	STG.LAST_MAINTENANCE_PROGRAM,
	STG.LAST_MAINTENANCE_DATETIME,
	STG.SOURCEDBMS,
	STG.SOURCE_SYSTEM,
	STG.CDC_INS_DT,
	STG.CDC_INS_USR,
	STG.CDC_UPD_DT,
	STG.CDC_UPD_USR,
	STG.DL_INS_DT,
	STG.DL_INS_USR,
	STG.CDW_INS_DT,
	STG.CDW_INS_USR,
	STG.CDW_UPD_DT,
	STG.CDW_UPD_USR,
  DATE(CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ))
  );
  `})
  snowflake.execute ({sqlText:`Delete from TRANSFORM.FACT_LOCATION tgt
USING
(SELECT * FROM TRANSFORM.FACT_LOCATION_QUARANTINE 
 WHERE  QUARANTINE_DATE  =  DATE(CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ))
) Qtgt
WHERE
  TGT.PLANT_ID = QTGT.PLANT_ID
AND TGT.CUSTOMER_ID = QTGT.CUSTOMER_ID
AND TGT.LOT_ID = QTGT.LOT_ID
AND TGT.LOT_EXTENDED_ID = QTGT.LOT_EXTENDED_ID
--AND TGT.LOCATION_ID = QTGT.LOCATION_ID
AND TGT.LICENSE_ID = QTGT.LICENSE_ID
AND TGT.SOURCEDBMS = QTGT.SOURCEDBMS
AND TGT.LAST_MAINTENANCE_DATE = QTGT.LAST_MAINTENANCE_DATE
    `})
  
   snowflake.execute ({sqlText:`MERGE INTO TRANSFORM.FACT_LOCATION TGT
USING
(
  SELECT
  A.LOPNT,
	A.LOCUS,
	A.LOLOT,
	A.LOLTX,
	A.LOLOC,
	A.LOQTY,
	A.LOLIC,
	A.LOPID,
	A.LOTYP,
	A.LOSTS,
	A.LOTIME,
	A.LOMDTE,
	A.LOMPGM,
	TRY_TO_TIMESTAMP(''20''||SUBSTRING(LPAD(A.LOMDTE,6,0),1,2)||''-''||SUBSTRING(LPAD(A.LOMDTE,6,0),3,2)||''-''||SUBSTRING(LPAD(A.LOMDTE,6,0),5,2)
	||'' ''||SUBSTRING(LPAD(A.LOTIME,6,0),1,2)||'':''||SUBSTRING(LPAD(A.LOTIME,6,0),3,2)||'':''||SUBSTRING(LPAD(A.LOTIME,6,0),5,2))	AS LAST_MAINTENANCE_DATETIME,
	A.SOURCEDBMS,
	A.SOURCE_SYSTEM,
	A.CDC_INS_DT,
	A.CDC_INS_USR,
	A.CDC_UPD_DT,
	A.CDC_UPD_USR,
	A.DL_INS_DT,
	A.DL_INS_USR
FROM
	STAGING.LOCATE_DATA_FIX_STG A
INNER JOIN 
(SELECT LOPNT,LOCUS,LOLOT,LOLTX,LOLIC,SOURCEDBMS FROM STAGING.LOCATE_DATA_FIX_STG 
GROUP BY LOPNT,LOCUS,LOLOT,LOLTX,LOLIC,SOURCEDBMS
HAVING COUNT(*) = 1) B
ON
    A.LOPNT = B.LOPNT
AND A.LOCUS = B.LOCUS
AND A.LOLOT = B.LOLOT
AND A.LOLTX = B.LOLTX
--AND A.LOLOC = B.LOLOC
AND A.LOLIC = B.LOLIC
AND A.SOURCEDBMS = B.SOURCEDBMS
WHERE TO_DATE(LAST_MAINTENANCE_DATETIME) < TO_DATE(CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ))
ORDER BY A.SOURCEDBMS,A.LOPNT,A.LOCUS,A.LOLOT,A.LOLTX,A.LOLIC
) STG
ON
	TGT.PLANT_ID = STG.LOPNT
AND TGT.CUSTOMER_ID = STG.LOCUS
AND TGT.LOT_ID = STG.LOLOT
AND TGT.LOT_EXTENDED_ID = STG.LOLTX
--AND TGT.LOCATION_ID = STG.LOLOC
AND TGT.LICENSE_ID = STG.LOLIC
AND TGT.SOURCEDBMS = STG.SOURCEDBMS
WHEN MATCHED AND STG.LAST_MAINTENANCE_DATETIME > TGT.LAST_MAINTENANCE_DATETIME THEN
UPDATE
SET
	TGT.PLANT_ID = STG.LOPNT,
	TGT.CUSTOMER_ID = STG.LOCUS,
	TGT.LOT_ID = STG.LOLOT,
	TGT.LOT_EXTENDED_ID = STG.LOLTX,
	TGT.LOCATION_ID = STG.LOLOC,
	TGT.LOCATION_QUANTITY = STG.LOQTY,
	TGT.LICENSE_ID = STG.LOLIC,
	TGT.PALLET_ID = STG.LOPID,
	TGT.LOCATION_TYPE = STG.LOTYP,
	TGT.STATUS_FLAG = STG.LOSTS,
	TGT.LAST_MAINTENANCE_TIME = STG.LOTIME,
	TGT.LAST_MAINTENANCE_DATE = STG.LOMDTE,
	TGT.LAST_MAINTENANCE_PROGRAM = STG.LOMPGM,
	TGT.LAST_MAINTENANCE_DATETIME = STG.LAST_MAINTENANCE_DATETIME,
	TGT.SOURCEDBMS = STG.SOURCEDBMS,
	TGT.SOURCE_SYSTEM = STG.SOURCE_SYSTEM,
	TGT.CDC_INS_DT = STG.CDC_INS_DT,
	TGT.CDC_INS_USR = STG.CDC_INS_USR,
	TGT.CDC_UPD_DT = STG.CDC_UPD_DT,
	TGT.CDC_UPD_USR = STG.CDC_UPD_USR,
	TGT.DL_INS_DT = STG.DL_INS_DT,
	TGT.DL_INS_USR = STG.DL_INS_USR,
	TGT.CDW_UPD_DT = CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ),
	TGT.CDW_UPD_USR = ''ADF_FIX''
WHEN NOT MATCHED THEN
INSERT
(
	TGT.PLANT_ID,
	TGT.CUSTOMER_ID,
	TGT.LOT_ID,
	TGT.LOT_EXTENDED_ID,
	TGT.LOCATION_ID,
	TGT.LOCATION_QUANTITY,
	TGT.LICENSE_ID,
	TGT.PALLET_ID,
	TGT.LOCATION_TYPE,
	TGT.STATUS_FLAG,
	TGT.LAST_MAINTENANCE_TIME,
	TGT.LAST_MAINTENANCE_DATE,
	TGT.LAST_MAINTENANCE_PROGRAM,
	TGT.LAST_MAINTENANCE_DATETIME,
	TGT.SOURCEDBMS,
	TGT.SOURCE_SYSTEM,
	TGT.CDC_INS_DT,
	TGT.CDC_INS_USR,
	TGT.CDC_UPD_DT,
	TGT.CDC_UPD_USR,
	TGT.DL_INS_DT,
	TGT.DL_INS_USR,
	TGT.CDW_INS_DT,
	TGT.CDW_INS_USR,
	TGT.CDW_UPD_DT,
	TGT.CDW_UPD_USR
)
VALUES
(
	STG.LOPNT,
	STG.LOCUS,
	STG.LOLOT,
	STG.LOLTX,
	STG.LOLOC,
	STG.LOQTY,
	STG.LOLIC,
	STG.LOPID,
	STG.LOTYP,
	STG.LOSTS,
	STG.LOTIME,
	STG.LOMDTE,
	STG.LOMPGM,
	STG.LAST_MAINTENANCE_DATETIME,
	STG.SOURCEDBMS,
	STG.SOURCE_SYSTEM,
	STG.CDC_INS_DT,
	STG.CDC_INS_USR,
	STG.CDC_UPD_DT,
	STG.CDC_UPD_USR,
	STG.DL_INS_DT,
	STG.DL_INS_USR,
	CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ),
	''ADF_FIX'',
	NULL,	
	NULL
);
  `})
return ''done'';

';
