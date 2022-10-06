USE SCHEMA TRANSFORM;

CREATE OR REPLACE PROCEDURE "SP_BACKUP_HSDETL_HOURLY_LOAD"("DL_FILE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

// Truncate staging table

snowflake.execute ({sqlText:
`
TRUNCATE TABLE STAGING.HSDETL_STG;
`
})

// Load staging atble

snowflake.execute ({sqlText:
`
COPY INTO STAGING.HSDETL_STG FROM(
SELECT
	$1:HSPNT,
	$1:HSCUS,
	$1:HSTIC,
	$1:HSLOT,
	$1:HSLTX,
	$1:HSQTY,
	$1:HSDIF,
	$1:HSSCE,
	$1:HSSDT,
	$1:HSFLG,
	$1:HSTWT,
	$1:HSSTS,
	$1:HSTIME,
	$1:HSMDTE,
	$1:HSMPGM,
	$1:SOURCEDBMS,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	$1:RRN,
	NULL,
	NULL,
	NULL,
	NULL,
	$1:DL_INS_DT,
	$1:DL_INS_USR,
	convert_timezone(''America/Los_Angeles'', ''America/New_York'', current_timestamp::timestamp_ntz),
	NULL,
	''ADF_USER'',
	NULL,
	''WMS''
FROM @STAGING.sf_dl_wms_parquet_stage/HSDETL/`+ DL_FILE_NAME +`);
`
})

// Load fact atble

snowflake.execute ({sqlText:
`
MERGE INTO TRANSFORM.FACT_ORDER_HISTORY_DETAILS TGT
USING
(
SELECT
	A.HSPNT,
	A.HSCUS,
	A.HSTIC,
	A.HSLOT,
	A.HSLTX,
	A.HSQTY,
	A.HSDIF,
	A.HSSCE,
	A.HSSDT,
	TRY_TO_DATE(A.HSSCE||SUBSTRING(LPAD(A.HSSDT,6,0),1,2)||''-''||SUBSTRING(LPAD(A.HSSDT,6,0),3,2)||''-''||SUBSTRING(LPAD(A.HSSDT,6,0),5,2)) AS SHIP_DATE,
	A.HSFLG,
	A.HSTWT,
	A.HSSTS,
	A.HSTIME,
	A.HSMDTE,
	A.HSMPGM,
	TRY_TO_TIMESTAMP(''20''||SUBSTRING(LPAD(A.HSMDTE,6,0),1,2)||''-''||SUBSTRING(LPAD(A.HSMDTE,6,0),3,2)||''-''||SUBSTRING(LPAD(A.HSMDTE,6,0),5,2)
||'' ''||SUBSTRING(LPAD(A.HSTIME,6,0),1,2)||'':''||SUBSTRING(LPAD(A.HSTIME,6,0),3,2)||'':''||SUBSTRING(LPAD(A.HSTIME,6,0),5,2))	AS LAST_MAINTENANCE_DATETIME,
	A.DTL__CAPXRRN,
	A.SOURCEDBMS,
	A.SOURCE_SYSTEM,
	A.CDC_INS_DT,
	A.CDC_INS_USR,
	A.CDC_UPD_DT,
	A.CDC_UPD_USR,
	A.DL_INS_DT,
	A.DL_INS_USR
FROM
	STAGING.HSDETL_STG A
JOIN 
(SELECT HSPNT,HSCUS,HSTIC,HSLOT,HSLTX,HSSDT,DTL__CAPXRRN,SOURCEDBMS FROM STAGING.HSDETL_STG 
GROUP BY HSPNT,HSCUS,HSTIC,HSLOT,HSLTX,HSSDT,DTL__CAPXRRN,SOURCEDBMS
HAVING COUNT(*) = 1) B
ON
    A.HSPNT = B.HSPNT
AND A.HSCUS = B.HSCUS
AND A.HSTIC = B.HSTIC
AND A.HSLOT = B.HSLOT
AND A.HSLTX = B.HSLTX
AND A.HSSDT = B.HSSDT
AND A.DTL__CAPXRRN = B.DTL__CAPXRRN
AND A.SOURCEDBMS = B.SOURCEDBMS
ORDER BY SOURCEDBMS,HSPNT,HSCUS,HSTIC,HSLOT,HSLTX,HSSDT
) STG
ON
	TGT.PLANT_ID = STG.HSPNT
AND TGT.CUSTOMER_ID = STG.HSCUS
AND TGT.TICKET_ID = STG.HSTIC
AND TGT.LOT_ID = STG.HSLOT
AND TGT.LOT_EXTENDED_ID = STG.HSLTX
AND TGT.SHIP_DATE = STG.SHIP_DATE
AND TGT.RRN = STG.DTL__CAPXRRN
AND TGT.SOURCEDBMS = STG.SOURCEDBMS
WHEN MATCHED THEN
UPDATE
SET
	TGT.PLANT_ID = STG.HSPNT,
	TGT.CUSTOMER_ID = STG.HSCUS,
	TGT.TICKET_ID = STG.HSTIC,
	TGT.LOT_ID = STG.HSLOT,
	TGT.LOT_EXTENDED_ID = STG.HSLTX,
	TGT.QUANTITY = STG.HSQTY,
	TGT.DIFFERENCE_FLAG = STG.HSDIF,
	TGT.SHIP_CENTURY = STG.HSSCE,
	TGT.SHIP_DATE_NUMERIC = STG.HSSDT,
	TGT.SHIP_DATE = STG.SHIP_DATE,
	TGT.INDICATOR_FLAG = STG.HSFLG,
	TGT.TAKE_WEIGHT_FLAG = STG.HSTWT,
	TGT.STATUS_FLAG = STG.HSSTS,
	TGT.LAST_MAINTENANCE_TIME = STG.HSTIME,
	TGT.LAST_MAINTENANCE_DATE = STG.HSMDTE,
	TGT.LAST_MAINTENANCE_PROGRAM = STG.HSMPGM,
	TGT.LAST_MAINTENANCE_DATETIME = STG.LAST_MAINTENANCE_DATETIME,
	TGT.RRN = STG.DTL__CAPXRRN,
	TGT.SOURCEDBMS = STG.SOURCEDBMS,
	TGT.SOURCE_SYSTEM = STG.SOURCE_SYSTEM,
	TGT.CDC_INS_DT = STG.CDC_INS_DT,
	TGT.CDC_INS_USR = STG.CDC_INS_USR,
	TGT.CDC_UPD_DT = STG.CDC_UPD_DT,
	TGT.CDC_UPD_USR = STG.CDC_UPD_USR,
	TGT.DL_INS_DT = STG.DL_INS_DT,
	TGT.DL_INS_USR = STG.DL_INS_USR,
	TGT.CDW_UPD_DT = CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ),
	TGT.CDW_UPD_USR = ''ADF_USER''
WHEN NOT MATCHED THEN
INSERT
(
	TGT.PLANT_ID,
	TGT.CUSTOMER_ID,
	TGT.TICKET_ID,
	TGT.LOT_ID,
	TGT.LOT_EXTENDED_ID,
	TGT.QUANTITY,
	TGT.DIFFERENCE_FLAG,
	TGT.SHIP_CENTURY,
	TGT.SHIP_DATE_NUMERIC,
	TGT.SHIP_DATE,
	TGT.INDICATOR_FLAG,
	TGT.TAKE_WEIGHT_FLAG,
	TGT.STATUS_FLAG,
	TGT.LAST_MAINTENANCE_TIME,
	TGT.LAST_MAINTENANCE_DATE,
	TGT.LAST_MAINTENANCE_PROGRAM,
	TGT.LAST_MAINTENANCE_DATETIME,
	TGT.RRN,
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
	STG.HSPNT,
	STG.HSCUS,
	STG.HSTIC,
	STG.HSLOT,
	STG.HSLTX,
	STG.HSQTY,
	STG.HSDIF,
	STG.HSSCE,
	STG.HSSDT,
	STG.SHIP_DATE,
	STG.HSFLG,
	STG.HSTWT,
	STG.HSSTS,
	STG.HSTIME,
	STG.HSMDTE,
	STG.HSMPGM,
	STG.LAST_MAINTENANCE_DATETIME,
	STG.DTL__CAPXRRN,
	STG.SOURCEDBMS,
	STG.SOURCE_SYSTEM,
	STG.CDC_INS_DT,
	STG.CDC_INS_USR,
	STG.CDC_UPD_DT,
	STG.CDC_UPD_USR,
	STG.DL_INS_DT,
	STG.DL_INS_USR,
	CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP::TIMESTAMP_NTZ),
	''ADF_USER'',
	NULL,	
	NULL
);
`
})
return ''Table loaded'';
';
