-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###1 .Vendor
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vm_vendor             | Name_in_target table |
-- MAGIC |---------------------------|----------------------|
-- MAGIC | t_last_user_id            |   Tech_Last_User_Id                   |
-- MAGIC | t_creator_id              |  Tech_Creator_Id                    |
-- MAGIC | t_creation_date           |  Tech_Create_Date_Ts                    |
-- MAGIC | t_last_write              |  Tech_Last_Write_Ts                    |
-- MAGIC | enterprisevendorid        | Ent_Vendor_Id                     |
-- MAGIC | name                      | Vendor_Name                     |
-- MAGIC | businessparty_            | Bus_Party_Id_                     |
-- MAGIC | vendortype_               | Vendor_Type_Code                     |
-- MAGIC | unfiaccountnumber         |Unfi_Acct_Nbr                      |
-- MAGIC | incoterms_                |Incoterms_Type_Code                      |
-- MAGIC | startdate                 |Row_Start_Date                      |
-- MAGIC | enddate                   |Row_End_Date                      |
-- MAGIC | technical_rowinactivedate |Tech_Row_Inactv_Date                      |
-- MAGIC

-- COMMAND ----------




CREATE OR REFRESH LIVE TABLE Vendor
(
	Ent_Vendor_Id BIGINT,
	Vendor_Name string,
	Bus_Party_Org_Id_ BIGINT,
  Bus_Party_Ind_Id_ BIGINT,
	Vendor_Type_Code STRING,
	Unfi_Acct_Nbr STRING,
	Incoterms_Type_Code STRING,
	Pmt_Terms_Code STRING,
	Pmt_Method_Code STRING,
	Row_Start_Date TIMESTAMP,
	Row_End_Date TIMESTAMP,
	Tech_Row_Inactv_Date TIMESTAMP,
	Tech_Last_User_Id STRING,
	Tech_Last_Write_Ts TIMESTAMP,
	Tech_Creator_Id STRING,
	Tech_Create_Date_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP 
  )
COMMENT 'table for the vendor master main table'
AS 
SELECT
  cast(enterprisevendorid AS BIGINT) AS Ent_Vendor_Id,
  cast(name AS string ) AS Vendor_Name,
  cast(businesspartyorganization_ AS BIGINT) AS Bus_Party_Org_Id_,
  cast(businesspartyindividual_ AS BIGINT) AS Bus_Party_Ind_Id_,
  cast(vendortype_ AS STRING) AS Vendor_Type_Code,
  cast(unfiaccountnumber AS STRING) AS Unfi_Acct_Nbr,
  cast(incoterms_ AS STRING) AS Incoterms_Type_Code,
  null AS Pmt_Terms_Code,                             -- NULL EXTRA
  null AS Pmt_Method_Code,                            -- NULL EXTRA  
  cast(startdate AS TIMESTAMP) AS Row_Start_Date,
  cast(enddate AS TIMESTAMP) AS Row_End_Date,
  cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
  cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
  cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
  cast(t_creator_id AS STRING) AS Tech_Creator_Id,
  cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
  'MDM' AS Audit_Ingest_Src_Sys_Code,
  'MDM' AS Audit_Orig_Src_Sys_Code,
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vm_vendor;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###2. Vendor_Bus_Sys_Relnshp

-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE Vendor_Bus_Sys_Relnshp
(
  Ent_Vendor_Id BIGINT ,
  Bus_Src_Sys_Id BIGINT ,
  Vendor_Legacy_Id STRING COMMENT 'aka "VendorCode" column in the Vendor Business System Relationship table is the Legacy Vendor Number',
  Row_Start_Date TIMESTAMP,
  Row_End_Date TIMESTAMP,
  Tech_Row_Inactv_Date TIMESTAMP,
  Tech_Last_User_Id STRING ,
  Tech_Last_Write_Ts TIMESTAMP, 
  Tech_Creator_Id STRING ,
  Tech_Create_Ts TIMESTAMP, 
  Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "National" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
  Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
  Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
  Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP 
)
COMMENT 'Table for the vendor master business system relationship'
AS
SELECT
  cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
  cast(t_creator_id AS STRING) AS Tech_Creator_Id,
  cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Ts,
  cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
  cast(vendorcode AS STRING) AS Vendor_Legacy_Id,
  cast(businesssystem_ AS BIGINT) AS Bus_Src_Sys_Id,
  cast(vendor_ AS BIGINT) AS Ent_Vendor_Id,
  cast(startdate AS TIMESTAMP) AS Row_Start_Date,
  cast(enddate AS TIMESTAMP) AS Row_End_Date,
  cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
  'MDM' as Audit_Ingest_Src_Sys_Code,
  'MDM' as Audit_Orig_Src_Sys_Code,
  current_timestamp() as Audit_Row_Load_Ts,
  cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vm_vendor_bussystem_rel; -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###3. Reference Vendor Relationship Type

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Ref_Vendor_Relnshp_Type
(
	Relnshp_Type_Code STRING,
	Relnshp_Type_Desc_Text STRING,
	Tech_Row_Inactv_Date TIMESTAMP,
	Tech_Last_User_Id STRING,
	Tech_Last_Write_Ts TIMESTAMP,
	Tech_Creator_Id STRING,
	Tech_Create_Date_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
	CDC_Tracker TIMESTAMP 
)
COMMENT 'Reference table for the vendor relationship types'
AS
SELECT
	cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
	cast(t_creator_id AS STRING) AS Tech_Creator_Id,
	cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
	cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
	cast(relationshiptype AS STRING) AS Relnshp_Type_Code,
	cast(description AS STRING) AS Relnshp_Type_Desc_Text,
	cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
	'MDM' as Audit_Ingest_Src_Sys_Code,
  'MDM' as Audit_Orig_Src_Sys_Code,
  current_timestamp() as Audit_Row_Load_Ts,
  null as Audit_Last_Update_Ts,
	cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vr_vend_rel_type; -- Replace 'source_table' with the actual source table name
 -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###4. Reference Vendor Type

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Ref_Vendor_Type
(
	Vendor_Type_Code STRING,
	Vendor_Type_Desc_Text STRING,
	Tech_Row_Inactv_Date TIMESTAMP,
	Tech_Last_User_Id STRING,
	Tech_Last_Write_Ts TIMESTAMP,
	Tech_Creator_Id STRING,
	Tech_Create_Date_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
	CDC_Tracker TIMESTAMP

)
COMMENT 'Reference table for the vendor types'
AS
SELECT
	cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
	cast(t_creator_id AS STRING) AS Tech_Creator_Id,
	cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
	cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
	cast(vendortype AS STRING) AS Vendor_Type_Code,
	cast(description AS STRING) AS Vendor_Type_Desc_Text,
	cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
	'MDM' as Audit_Ingest_Src_Sys_Code,
  'MDM' as Audit_Orig_Src_Sys_Code,
  current_timestamp() as Audit_Row_Load_Ts,
  null as Audit_Last_Update_Ts,
	cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vr_vend_type; -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###5. Vendor Individual Relationship 

-- COMMAND ----------



-- CREATE OR REFRESH LIVE TABLE Vendor_Indvdl_Relnshp
-- (
--   Ent_Vendor_Id BIGINT NOT NULL,
--   Ent_Bus_Party_Id BIGINT NOT NULL,
--   Bus_Party_Role_Code STRING,
--   Row_Start_Date TIMESTAMP,
--   Row_End_Date TIMESTAMP,
--   Tech_Row_Inactv_Date TIMESTAMP,
--   Tech_Last_User_Id STRING,
--   Tech_Last_Write_Ts TIMESTAMP,
--   Tech_Creator_Id STRING,
--   Tech_Create_Date_Ts TIMESTAMP,
--   Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
--   Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
--   Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
--   Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.'
-- )
-- COMMENT 'Table for the vendor masterindividual relationship'
-- AS
-- SELECT
-- 	cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
-- 	cast(t_creator_id AS STRING) AS Tech_Creator_Id,
-- 	cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
-- 	cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
-- 	cast(vendor_ AS BIGINT) AS Ent_Vendor_Id,
-- 	cast(individual_ AS BIGINT) AS Ent_Bus_Party_Id,
-- 	cast(role_ AS STRING) AS Bus_Party_Role_Code,
-- 	cast(startdate AS TIMESTAMP) AS Row_Start_Date,
-- 	cast(enddate AS TIMESTAMP) AS Row_End_Date,
-- 	cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
-- 	'MDM' as Audit_Ingest_Src_Sys_Code,
--   'MDM' as Audit_Orig_Src_Sys_Code,
--   current_timestamp() as Audit_Row_Load_Ts,
--   null as Audit_Last_Update_Ts
-- FROM dev.silver_erp.ebx_vm_vendor_ind_rel 





-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###6. Vendor Vendor Relationship 

-- COMMAND ----------



-- CREATE OR REFRESH LIVE TABLE Vendor_Vendor_Relnshp
-- (
-- 	Ent_Vendor_Id BIGINT NOT NULL,
-- 	Related_Vendor_Id_ BIGINT,
-- 	Relnshp_Type_Code STRING,
-- 	Row_Start_Date DATE,
-- 	Row_End_Date DATE,
-- 	Tech_Row_Inactv_Date DATE,
-- 	Tech_Last_User_Id STRING,
-- 	Tech_Last_Write_Ts TIMESTAMP,
-- 	Tech_Creator_Id STRING,
-- 	Tech_Create_Date_Ts TIMESTAMP,
-- 	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
-- 	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
-- 	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
-- 	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.'
-- )
-- COMMENT 'Table for the vendor-vendor relationship'
-- AS
-- SELECT
-- 	cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
-- 	cast(t_creator_id AS STRING) AS Tech_Creator_Id,
-- 	cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
-- 	cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
-- 	cast(vendor_ AS BIGINT) AS Ent_Vendor_Id,
-- 	cast(relatedvendor_ AS BIGINT) AS Related_Vendor_Id_,
-- 	cast(relationshiptype_ AS STRING) AS Relnshp_Type_Code,
-- 	cast(startdate AS DATE) AS Row_Start_Date,
-- 	cast(enddate AS DATE) AS Row_End_Date,
-- 	cast(technical_rowinactivedate AS DATE) AS Tech_Row_Inactv_Date,
-- 	'MDM' as Audit_Ingest_Src_Sys_Code,
--   'MDM' as Audit_Orig_Src_Sys_Code,
--   current_timestamp() as Audit_Row_Load_Ts,
--   null as Audit_Last_Update_Ts
-- FROM dev.silver_erp.ebx_vm_vendor_vendor_rel; -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###7. ebx_vm_vendor_org_rel	---> Vendor Organization Relationship

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vm_vendor_org_rel     |  Name in target table | Vendor Organization Relationship     |
-- MAGIC |---------------------------|-----------------------|--------------------------------------|
-- MAGIC | t_last_user_id            |  Tech_Last_User_Id | Technical Last User Identifier       |
-- MAGIC | t_creator_id             | Tech_Creator_Id  | Technical Creator Identifier         |
-- MAGIC | t_creation_date           | Tech_Create_Date_Ts  | Technical Creation Timestamp         |
-- MAGIC | t_last_write              | Tech_Last_Write_Ts  | Technical Last Write Timestamp       |
-- MAGIC | vendor_                   | Ent_Vendor_Id  | Enterprise Vendor Identifier         |
-- MAGIC | organization_             | Ent_Bus_Party_Id  | Enterprise Business Party Identifier |
-- MAGIC | role_                     |  Bus_Party_Role_Code | Business Party Role Code             |
-- MAGIC | startdate                 | Row_Start_Date  | Row Start Date                       |
-- MAGIC | enddate                   | Row_End_Date  | Row End Date                         |
-- MAGIC | technical_rowinactivedate | Tech_Row_Inactv_Date  | Technical Row Inactive Date          |

-- COMMAND ----------


-- CREATE OR REFRESH LIVE TABLE Vendor_Org_Relnshp
-- (
-- 	Ent_Vendor_Id BIGINT NOT NULL,
-- 	Ent_Bus_Party_Id BIGINT NOT NULL,
-- 	Bus_Party_Role_Code STRING,
-- 	Tech_Last_User_Id STRING,
-- 	Tech_Creator_Id STRING,
-- 	Tech_Create_Date_Ts TIMESTAMP,
-- 	Tech_Last_Write_Ts TIMESTAMP,
-- 	Row_Start_Date DATE,
-- 	Row_End_Date DATE,
-- 	Tech_Row_Inactv_Date DATE,
-- 	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
-- 	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
-- 	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
-- 	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.'
-- )
-- COMMENT 'Table for the vendor master organization relationship'
-- AS
-- SELECT
-- 	cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
-- 	cast(t_creator_id AS STRING) AS Tech_Creator_Id,
-- 	cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
-- 	cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
-- 	cast(vendor_ AS BIGINT) AS Ent_Vendor_Id,
-- 	cast(organization_ AS BIGINT) AS Ent_Bus_Party_Id,
-- 	cast(role_ AS STRING) AS Bus_Party_Role_Code,
-- 	cast(startdate AS DATE) AS Row_Start_Date,
-- 	cast(enddate AS DATE) AS Row_End_Date,
-- 	cast(technical_rowinactivedate AS DATE) AS Tech_Row_Inactv_Date,
-- 	'MDM' as Audit_Ingest_Src_Sys_Code,
--   'MDM' as Audit_Orig_Src_Sys_Code,
--   current_timestamp() as Audit_Row_Load_Ts,
--   null as Audit_Last_Update_Ts
-- FROM dev.silver_erp.ebx_vm_vendor_org_rel; -- Replace 'source_table' with the actual source table name




-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###8. ebx_vm_vendor_paymentdata_paymentmethods-->	Vendor Payment Data Payment Methods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vm_vendor_paymentdata_paymentmethods | Name_in_target table  | 
-- MAGIC |------------------------------------------|---|
-- MAGIC | enterprisevendorid                       | Ent_Vendor_Id  |
-- MAGIC | idx                                      | Mdm_Sys_Vendor_Id| 
-- MAGIC | paymentdata_paymentmethods_              | Pmt_Method_Code  | 

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Vendor_Pmt_Data_Pmt_Methods
(
	Ent_Vendor_Id BIGINT NOT NULL,
	Mdm_Sys_Vendor_Id BIGINT NOT NULL,
	Pmt_Method_Code STRING,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
	CDC_Tracker TIMESTAMP
)
COMMENT 'Table for the vendor master payment methods'
AS
SELECT
	cast(enterprisevendorid AS BIGINT) AS Ent_Vendor_Id,
	cast(idx AS BIGINT) AS Mdm_Sys_Vendor_Id,
	cast(paymentdata_paymentmethods_ AS STRING) AS Pmt_Method_Code,
	'MDM' as Audit_Ingest_Src_Sys_Code,
  'MDM' as Audit_Orig_Src_Sys_Code,
  current_timestamp() as Audit_Row_Load_Ts,
  null as Audit_Last_Update_Ts,
	cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vm_vendor_paymentdata_paymentmethods; --Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###9.ebx_vm_vendor_paymentdata_paymentterms	--> Vendor Payment Data Payment Terms 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vm_vendor_paymentdata_paymentterms | Name_in_target table  |
-- MAGIC |----------------------------------------|-----------------|
-- MAGIC | enterprisevendorid                     |Ent_Vendor_Id   |
-- MAGIC | idx                                    | Mdm_Sys_Vendor_Id  |
-- MAGIC | paymentdata_paymentterms_              | Pmt_Terms_Code  |

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Vendor_Pmt_Data_Pmt_Terms
(
	Ent_Vendor_Id BIGINT NOT NULL,
	Mdm_Sys_Vendor_Id BIGINT,
	Pmt_Terms_Code STRING,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
	CDC_Tracker TIMESTAMP
)
COMMENT 'Table for the vendor master payment terms'
AS
SELECT
	cast(enterprisevendorid AS BIGINT) AS Ent_Vendor_Id,
	cast(idx AS BIGINT) AS Mdm_Sys_Vendor_Id,
	cast(paymentdata_paymentterms_ AS STRING) AS Pmt_Terms_Code,
	'MDM' as Audit_Ingest_Src_Sys_Code,
  'MDM' as Audit_Orig_Src_Sys_Code,
  current_timestamp() as Audit_Row_Load_Ts,
  null as Audit_Last_Update_Ts,
	cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vm_vendor_paymentdata_paymentterms; -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###10. ebx_vr_pay_method	-->  Reference Vendor Payment Method

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vr_pay_method         | Name_in_target table  |
-- MAGIC |---------------------------|---|
-- MAGIC | t_last_user_id            | Tech_Last_User_Id  |
-- MAGIC | t_creator_id              | Tech_Creator_Id  |
-- MAGIC | t_creation_date           | Tech_Create_Date_Ts  |
-- MAGIC | t_last_write              | Tech_Last_Write_Ts  |
-- MAGIC | paymentmethod             | Pmt_Method_Code  |
-- MAGIC | description               | Pmt_Method_Desc_Text  |
-- MAGIC | technical_rowinactivedate |Tech_Row_Inactv_Date  |

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Ref_Vendor_Pmt_Method
(
  Pmt_Method_Code STRING,
  Pmt_Method_Desc_Text STRING,
  Tech_Last_User_Id STRING ,
  Tech_Creator_Id STRING ,
  Tech_Create_Date_Ts TIMESTAMP ,
  Tech_Last_Write_Ts TIMESTAMP ,
  Tech_Row_Inactv_Date TIMESTAMP,
  Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server, "CDW_UCS" = UCS data from the CDW SQL Server database.',
  Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
  Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
  Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP
)
COMMENT 'Reference table for the vendor payment methods'
AS
SELECT
  cast(paymentmethod AS STRING) AS Pmt_Method_Code,
  cast(description AS STRING) AS Pmt_Method_Desc_Text,
  cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
  cast(t_creator_id AS STRING) AS Tech_Creator_Id,
  cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
  cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
  cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
  'MDM' AS Audit_Ingest_Src_Sys_Code,
  'MDM' AS Audit_Orig_Src_Sys_Code,
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_vr_pay_method; -- Replace 'source_table' with the actual source table name
 -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###11. ebx_vr_pay_terms -->	Reference Vendor Payment Terms 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_vr_pay_terms          | Name_in_target table |
-- MAGIC |---------------------------|----------------------|
-- MAGIC | t_last_user_id            | Tech_Last_User_Id                     |
-- MAGIC | t_creator_id              |  Tech_Creator_Id                    |
-- MAGIC | t_creation_date           |  Tech_Create_Date_Ts                    |
-- MAGIC | t_last_write              |  Tech_Last_Write_Ts                    |
-- MAGIC | paymentterms              | Pmt_Terms_Code                     |
-- MAGIC | description               | Pmt_Terms_Desc                     |
-- MAGIC | technical_rowinactivedate | Tech_Row_Inactv_Date                     |
-- MAGIC | alternatedescription      | Pmt_Terms_Altrnt_Desc                     |
-- MAGIC
-- MAGIC

-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE  Ref_Vendor_Pmt_Terms
(
	Pmt_Terms_Code STRING,
	Pmt_Terms_Desc STRING,
	Pmt_Terms_Altrnt_Desc STRING,
	Tech_Row_Inactv_Date TIMESTAMP,
	Tech_Last_User_Id STRING,
	Tech_Last_Write_Ts TIMESTAMP,
	Tech_Creator_Id STRING,
	Tech_Create_Date_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP
)
COMMENT 'Reference table for the vendor payment terms'
SELECT
  cast(paymentterms AS STRING) AS Pmt_Terms_Code,
  cast(description AS STRING) AS Pmt_Terms_Desc,
  null AS Pmt_Terms_Altrnt_Desc,                                  --  turned this to null 
  cast(technical_rowinactivedate AS TIMESTAMP) AS Tech_Row_Inactv_Date,
  cast(t_last_user_id AS STRING) AS Tech_Last_User_Id,
  cast(t_creator_id AS STRING) AS Tech_Creator_Id,
  cast(t_creation_date AS TIMESTAMP) AS Tech_Create_Date_Ts,
  cast(t_last_write AS TIMESTAMP) AS Tech_Last_Write_Ts,
  'MDM' AS Audit_Ingest_Src_Sys_Code,
  'MDM' AS Audit_Orig_Src_Sys_Code,
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(created_on as Timestamp) as CDC_Tracker
FROM  dev.silver_stg_erp.ebx_vr_pay_terms; -- Replace 'source_table' with the actual source table name


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ###12. ebx_bp_organization -->	Business Party Organization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_bp_organization                                | Name_in_target table |
-- MAGIC |----------------------------------------------------|----------------------|
-- MAGIC | enterprisebusinesspartyid                          |  Ent_Bus_Party_Id                    |
-- MAGIC | name                                               |   Org_Name                   |
-- MAGIC | federalid                                          |  Fed_Id                    |
-- MAGIC | dunsnumber                                         | Duns_Nbr                     |
-- MAGIC | isfinancialinstitution                             |  Is_Fncl_Institution_Flag                |
-- MAGIC | taxjurisdiction_UKN                                   | Org_Tax_Jurisdiction_Nbr    |
-- MAGIC | minoritybusinesspartyowners_UKN                       | Minority_Bus_Party_Owners_Name_  |  
-- MAGIC | websites_UKN                                           |  Website_Addr_Text |
-- MAGIC | name_UKN                                              | Org_Cntct_Name  |
-- MAGIC |  phonenumber_UKN                                      | Org_Phone_Nbr|
-- MAGIC | emailaddress_UKN                                      | Org_Email_Addr_Text|
-- MAGIC | row_active_date_DOUBT                                 | Row_Active_Date    |
-- MAGIC | row_load_timestamp_DOUBT                              | Row_Load_Ts        | 
-- MAGIC | row_update_timestamp_DOUBT                            | Row_Update_Ts      |   
-- MAGIC | address_                                           |     Org_Addr                 |
-- MAGIC | bankdata_bankcountry_                              |  Bank_Cntry_Code_                     |
-- MAGIC | bankdata_bankaccountnumber                         |  Bank_Acct_Nbr                    |
-- MAGIC | bankdata_internationalbanknumber                   |  International_Bank_Acct_Nbr                    |
-- MAGIC | financialinstitutiondata_financialinstitutiontype_ |  Fncl_Institution_Type_Code_                    |
-- MAGIC | financialinstitutiondata_achrountingnumber         |  Fncl_Institution_Ach_Routing_Nbr                    |
-- MAGIC | startdate                                          |  Row_Start_Date                    |
-- MAGIC | enddate                                            |  Row_End_Date                    |
-- MAGIC | technical_rowinactivedate                          |  Row_Inactv_Date                    |

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Bus_Party_Org_Archive
(
	Ent_Bus_Party_Id BIGINT NOT NULL,
	Org_Name STRING,
	Fed_Id STRING,
	Duns_Nbr STRING,
	Org_Tax_Jurisdiction_Nbr STRING,
	Minority_Bus_Party_Owners_Name_ STRING,
	Website_Addr_Text STRING,
	Is_Fncl_Institution_Flag BOOLEAN,
	Ent_Addr_Id BIGINT,
	Org_Phone_Nbr STRING,
	Org_Email_Addr_Text STRING,
	Org_Cntct_Name STRING,
	Bank_Cntry_Code_ STRING,
	Bank_Acct_Nbr STRING,
	International_Bank_Acct_Nbr STRING,
	Fncl_Institution_Type_Code_ STRING,
	Fncl_Institution_Ach_Routing_Nbr STRING,
	Org_Addr STRING,
	Row_Start_Date timestamp,
	Row_End_Date timestamp ,
	Row_Active_Date timestamp ,
	Row_Inactv_Date timestamp ,
	Row_Load_Ts TIMESTAMP,
	Row_Update_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP
)
COMMENT 'busines party organization table'
SELECT
  cast(t1.enterprisebusinesspartyid AS BIGINT) AS Ent_Bus_Party_Id,
  cast(t1.name AS STRING) AS Org_Name,
  cast(t1.federalid AS STRING) AS Fed_Id,
  cast(t1.dunsnumber AS STRING) AS Duns_Nbr,
  null AS Org_Tax_Jurisdiction_Nbr, --cast(t2.taxjurisdiction_ AS STRING) AS Org_Tax_Jurisdiction_Nbr,                    
  null AS Minority_Bus_Party_Owners_Name_,--cast(t3.minoritybusinesspartyowners_ AS STRING) AS Minority_Bus_Party_Owners_Name_,
  null  AS Website_Addr_Text,-- cast(t4.websites AS STRING) AS Website_Addr_Text,
  cast(t1.isfinancialinstitution AS BOOLEAN) AS Is_Fncl_Institution_Flag,
  null AS Ent_Addr_Id,
  cast(t6.phonenumber AS STRING) AS Org_Phone_Nbr,
  cast(t7.emailaddress AS STRING) AS Org_Email_Addr_Text,
  cast(t5.name AS STRING) AS Org_Cntct_Name,
  cast(t1.bankdata_bankcountry_ AS STRING) AS Bank_Cntry_Code_,
  cast(t1.bankdata_bankaccountnumber AS STRING) AS Bank_Acct_Nbr,
  cast(t1.bankdata_internationalbanknumber AS STRING) AS International_Bank_Acct_Nbr,
  cast(t1.financialinstitutiondata_financialinstitutiontype_ AS STRING) AS Fncl_Institution_Type_Code_,
  cast(t1.financialinstitutiondata_achroutingnumber AS STRING) AS Fncl_Institution_Ach_Routing_Nbr,
  cast(t1.address_ AS STRING) AS Org_Addr,
  cast(t1.startdate AS timestamp) AS Row_Start_Date,
  cast(t1.enddate AS timestamp) AS Row_End_Date,
  null as Row_Active_Date,                                         --- NULL EXTRA      
  cast(t1.technical_rowinactivedate AS timestamp) AS Row_Inactv_Date,
  null  AS Row_Load_Ts,                                            --- NULL EXTRA    
  null AS Row_Update_Ts,                                           --- NULL EXTRA       
  'MDM' AS Audit_Ingest_Src_Sys_Code,     
  'MDM' AS Audit_Orig_Src_Sys_Code,      
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(t1.created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_bp_organization t1 
-- LEFT JOIN dev.silver_erp.ebx_bp_org_tax_jurisdiction t2  ON t1.enterprisebusinesspartyid=t2.id         -- TURNING INTO NULL
-- LEFT JOIN dev.silver_erp.ebx_bp_organization_minoritybusinesspartyowners t3 ON t1.enterprisebusinesspartyid=t3.enterprisebusinesspartyid
-- LEFT JOIN dev.silver_erp.ebx_bp_organization_websites t4 ON t1.enterprisebusinesspartyid =t4.enterprisebusinesspartyid 
LEFT JOIN dev.silver_stg_erp.ebx_bp_oranization_contact t5 ON   t1.enterprisebusinesspartyid =t5.id
LEFT JOIN dev.silver_stg_erp.ebx_bp_org_phone_number  t6  ON t1.enterprisebusinesspartyid =t6.id
LEFT JOIN dev.silver_stg_erp.ebx_bp_org_email_addr t7 ON t1.enterprisebusinesspartyid =t7.id ; 


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###13. ebx_rf_address	---> Business Party Address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_rf_address                               | Name_in_target table |
-- MAGIC |----------------------------------------------|----------------------|
-- MAGIC | enterpriseaddressid                          |  Ent_Addr_Id                    |
-- MAGIC | addressattentionto                           |  Addr_Attn_To_Text                    |
-- MAGIC | addressline1                                 |  Addr_Ln_1_Text                    |
-- MAGIC | addressline2                                 |  Addr_Ln_2_Text                    |
-- MAGIC | aptartmentsuite                               |  Apartment_Suite_Addr_Text                    |
-- MAGIC | cityname                                     |  City_Name                     |
-- MAGIC | stateprovince                                |  State_Province_Code                    |
-- MAGIC | postalcode                                   |  Postal_Code                    |
-- MAGIC | country_                                     |  Cntry_Code_                    |
-- MAGIC | addresslatitude                              |  Latitude_Coord_Nbr                    |
-- MAGIC | addresslongitude                             |  Longitude_Coord_Nbr                     |
-- MAGIC | militaryaddressinformation_poboxmilitaryunit |   Pobox_Military_Unit_Nbr                   |
-- MAGIC | militaryaddressinformation_fpoapo            |  Fpo_Apo_Text                    |
-- MAGIC | technical_rowinactivedate                    |  Row_Inactv_Date                    |
-- MAGIC |row_active_date_DOUBT	|Row_Active_Date|
-- MAGIC |row_load_timestamp_DOUBT	|Row_Load_Ts|
-- MAGIC |row_update_timestamp_DOUBT	|Row_Update_Ts|

-- COMMAND ----------

 

CREATE  OR REFRESH LIVE TABLE Bus_Party_Addr
(
	Ent_Addr_Id BIGINT NOT NULL,
	Addr_Attn_To_Text STRING,
	Addr_Ln_1_Text STRING,
	Addr_Ln_2_Text STRING,
	Apartment_Suite_Addr_Text STRING,
	City_Name STRING,
	State_Province_Code STRING,
	Postal_Code STRING,
	Cntry_Code_ STRING,
	Latitude_Coord_Nbr STRING,
	Longitude_Coord_Nbr STRING,
	Formatted_Addr_Text STRING,
	Pobox_Military_Unit_Nbr STRING,
	Fpo_Apo_Text STRING,
	Row_Active_Date timestamp ,
	Row_Inactv_Date timestamp ,
	Row_Load_Ts TIMESTAMP,
	Row_Update_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
	CDC_Tracker TIMESTAMP,
	ISO_3_Cntry_Code string
)
COMMENT 'Busines party address table'
AS
SELECT
  cast(enterpriseaddressid AS BIGINT) AS Ent_Addr_Id,
  cast(addressattentionto AS STRING) AS Addr_Attn_To_Text,
  cast(addressline1 AS STRING) AS Addr_Ln_1_Text,
  cast(addressline2 AS STRING) AS Addr_Ln_2_Text,
  cast(aptartmentsuite AS STRING) AS Apartment_Suite_Addr_Text,
  cast(cityname AS STRING) AS City_Name,
  cast(stateprovince AS STRING) AS State_Province_Code,
  cast(postalcode AS STRING) AS Postal_Code,
  cast(country_ AS STRING) AS Cntry_Code_,
  cast(addresslatitude AS STRING) AS Latitude_Coord_Nbr,
  cast(addresslongitude AS STRING) AS Longitude_Coord_Nbr,
  cast(militaryaddressinformation_poboxmilitaryunit AS STRING) AS Pobox_Military_Unit_Nbr,
  cast(militaryaddressinformation_fpoapo AS STRING) AS Fpo_Apo_Text,
  cast(technical_rowinactivedate AS timestamp ) AS Row_Inactv_Date,
  null as Row_Active_Date ,    --!! NULL COLUMN
	null as Row_Load_Ts ,         --!! NULL COLUMN  
	null as Row_Update_Ts ,        --!! NULL COLUMN
  'MDM' AS Audit_Ingest_Src_Sys_Code,
  'MDM' AS Audit_Orig_Src_Sys_Code,
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(ebx_rf_address.created_on as Timestamp) as CDC_Tracker,
  cast(ebx_rf_country.iso3countrycd as STRING) as ISO_3_Cntry_Code     ---  new column added as per Vlada's request on 30/01/2024

FROM dev.silver_stg_erp.ebx_rf_address ebx_rf_address   
left join dev.silver_stg_erp.ebx_rf_country on ebx_rf_address.country_ = ebx_rf_country.iso3countrycd    -- needed join for it 


-- previous query used till 29 / 01 / 2024
-- previous record count till 29 / 01 / 2024 , 6 pm IST : -- 147366

-- SELECT
--   cast(enterpriseaddressid AS BIGINT) AS Ent_Addr_Id,
--   cast(addressattentionto AS STRING) AS Addr_Attn_To_Text,
--   cast(addressline1 AS STRING) AS Addr_Ln_1_Text,
--   cast(addressline2 AS STRING) AS Addr_Ln_2_Text,
--   cast(aptartmentsuite AS STRING) AS Apartment_Suite_Addr_Text,
--   cast(cityname AS STRING) AS City_Name,
--   cast(stateprovince AS STRING) AS State_Province_Code,
--   cast(postalcode AS STRING) AS Postal_Code,
--   cast(country_ AS STRING) AS Cntry_Code_,
--   cast(addresslatitude AS STRING) AS Latitude_Coord_Nbr,
--   cast(addresslongitude AS STRING) AS Longitude_Coord_Nbr,
--   cast(militaryaddressinformation_poboxmilitaryunit AS STRING) AS Pobox_Military_Unit_Nbr,
--   cast(militaryaddressinformation_fpoapo AS STRING) AS Fpo_Apo_Text,
--   cast(technical_rowinactivedate AS timestamp ) AS Row_Inactv_Date,
--   null as Row_Active_Date ,    --!! NULL COLUMN
-- 	null as Row_Load_Ts ,         --!! NULL COLUMN  
-- 	null as Row_Update_Ts ,        --!! NULL COLUMN
--   'MDM' AS Audit_Ingest_Src_Sys_Code,
--   'MDM' AS Audit_Orig_Src_Sys_Code,
--   current_timestamp() AS Audit_Row_Load_Ts,
--   null AS Audit_Last_Update_Ts,
-- 	cast(created_on as Timestamp) as CDC_Tracker
-- FROM dev.silver_stg_erp.ebx_rf_address;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 14.Business Party Individual

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | ebx_bp_individual          | Name_in_target table |
-- MAGIC |---------------------------|----------------------|
-- MAGIC | enterprisebusinesspartyid | Ent_Bus_Party_Id                     |
-- MAGIC | firstname                 | First_Name                     |
-- MAGIC | middlename                | Mid_Name                     |
-- MAGIC | lastname                  | Last_Name                     |
-- MAGIC | title                     | Title_Name                     |
-- MAGIC | nickname                  | Nick_Name                     |
-- MAGIC | socialsecuritynumber      | Ssn                     |
-- MAGIC | address_                  | Ent_Addr_Id                     |
-- MAGIC | startdate                 | Row_Start_Date                     |
-- MAGIC | enddate                   | Row_End_Date                     |
-- MAGIC | technical_rowinactivedate | Tech_Row_Inactv_Date                     |
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE Bus_Party_Indvdl_Archive
(
	Ent_Bus_Party_Id BIGINT NOT NULL,
	Bus_Party_Name STRING,    
	First_Name STRING, 
	Mid_Name STRING,
	Last_Name STRING,
	Title_Name STRING,
	Nick_Name STRING,
	Ssn STRING,
	Ent_Addr_Id INTEGER,
	Indvdl_Cntct_Name STRING,
	Indvdl_Addresses_Text STRING,
	Indvdl_Phone_Nbr STRING,
	Indvdl_Email_Addr_Text STRING,
	Row_Start_Date timestamp,
	Row_End_Date timestamp,
	Tech_Row_Active_Date timestamp,
	Tech_Row_Inactv_Date timestamp,
	Tech_Row_Load_Ts TIMESTAMP,
	Tech_Row_Update_Ts TIMESTAMP,
	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.',
  CDC_Tracker TIMESTAMP
)
COMMENT 'business party individual table'
AS
SELECT
  cast(enterprisebusinesspartyid AS BIGINT) AS Ent_Bus_Party_Id,
  null as Bus_Party_Name,             --!! NULL COLUMN
  cast(firstname AS STRING) AS First_Name,
  cast(middlename AS STRING) AS Mid_Name,
  cast(lastname AS STRING) AS Last_Name,
  cast(title AS STRING) AS Title_Name,
  cast(nickname AS STRING) AS Nick_Name,
  cast(socialsecuritynumber AS STRING) AS Ssn,
  cast(address_ AS INTEGER) AS Ent_Addr_Id,
  null AS Indvdl_Cntct_Name,-- should put null or concat(First_Name, ' ', Last_Name)   --!! NULL COLUMN
  cast(address_ AS STRING) AS Indvdl_Addresses_Text,
  null AS Indvdl_Phone_Nbr,       --!! NULL COLUMN
  null AS Indvdl_Email_Addr_Text, --!! NULL COLUMN
  cast(startdate AS TIMESTAMP) AS Row_Start_Date,
  cast(enddate AS TIMESTAMP) AS Row_End_Date,
  null AS Tech_Row_Active_Date, --!! NULL COLUMN
  cast(technical_rowinactivedate AS timestamp ) AS Tech_Row_Inactv_Date, 
  null AS Tech_Row_Load_Ts,     --!! NULL COLUMN
  null AS Tech_Row_Update_Ts,   --!! NULL COLUMN
  'MDM' AS Audit_Ingest_Src_Sys_Code,
  'MDM' AS Audit_Orig_Src_Sys_Code,
  current_timestamp() AS Audit_Row_Load_Ts,
  null AS Audit_Last_Update_Ts,
  cast(created_on as Timestamp) as CDC_Tracker
FROM dev.silver_stg_erp.ebx_bp_individual;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 15.ebx_br_bus_party_role   -->Ref_Bus_Party_Role

-- COMMAND ----------

-- CREATE OR REFRESH LIVE TABLE Ref_Bus_Party_Role
-- (
--   Bus_Party_Role_Code STRING NOT NULL,
--   Bus_Party_Role_Desc_Text  STRING,
--   Row_Active_Date  STRING,
--   Row_Inactv_Date timestamp,
--   Row_Load_Ts  timestamp,
--   Row_Update_Ts timestamp,
-- 	Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
-- 	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
-- 	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
-- 	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.'
-- )
-- COMMENT 'business party individual table'
-- AS
-- SELECT 
-- cast(businesspartyrole as timestamp)  as Bus_Party_Role_Code,
-- cast(description as timestamp) as Bus_Party_Role_Desc_Text, 
-- cast(technical_rowinactivedate as timestamp) as Row_Inactv_Date,
-- null AS Tech_Row_Active_Date, --!! NULL COLUMN
-- null AS Row_Load_Ts,     --!! NULL COLUMN
-- null AS Row_Update_Ts,   --!! NULL COLUMN
-- 'MDM' AS Audit_Ingest_Src_Sys_Code,
-- 'MDM' AS Audit_Orig_Src_Sys_Code,
-- current_timestamp() AS Audit_Row_Load_Ts,
-- null AS Audit_Last_Update_Ts 
-- FROM dev.silver_erp.ebx_br_bus_party_role;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Vendor_Override_Pmt_Terms
(
    Ent_Vendor_Id BIGINT NOT NULL,
    Ent_DC_Id INT NOT NULL,
    Ent_Bus_Src_Sys_Id BIGINT NOT NULL,
    Pmt_Terms_Code STRING NOT NULL,
    Prod_Group_Code STRING,
    Start_Date DATE,
    End_Date DATE,
    Tech_Row_Inactv_Date DATE,
    Tech_Last_User_Id STRING,
    Tech_Creator_Id STRING,
    Tech_Create_Date_Ts TIMESTAMP,
    Tech_Last_Write_Ts TIMESTAMP,
    Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code which identifies an additional Database Data Store where the data was sourced from. This code further delineates the ingestion source system data record. Examples: "EDM_UBS" = UBS data from the East DataMart in SQL Server, "WDM_WBS" WBS data from the West "Natonal" Datamart System (WDM) in SQL Server , "CDW_UCS" = UCS data from the CDW SQL Server database.',
	Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Business Source System Code. �This code defines the originating business system source. Examples "UBS", "WBS", "UCS", "ALB", "TFF".',
	Audit_Row_Load_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was loaded.',
	Audit_Last_Update_Ts TIMESTAMP COMMENT 'Audit timestamp of when the record was last updated.'
)
AS 
SELECT
    CAST(vendor_ AS bigint) AS Ent_Vendor_Id,
    CAST(distributioncenter_ AS int) AS Ent_DC_Id,
    CAST(businesssystem_ AS bigint) AS Ent_Bus_Src_Sys_Id,
    CAST(paymentterm_ AS string) AS Pmt_Terms_Code,
    CAST(productgroup_ AS string) AS Prod_Group_Code,
    CAST(startdate AS date) AS Start_Date,
    CAST(enddate AS date) AS End_Date,
    CAST(technical_rowinactivedate AS date) AS Tech_Row_Inactv_Date,
    CAST(t_last_user_id AS string) AS Tech_Last_User_Id,
    CAST(t_creator_id AS string) AS Tech_Creator_Id,
    CAST(t_creation_date AS timestamp) AS Tech_Create_Date_Ts,
    CAST(t_last_write AS timestamp) AS Tech_Last_Write_Ts,
    'MDM' AS Audit_Ingest_Src_Sys_Code,
    'MDM' AS Audit_Orig_Src_Sys_Code,
    current_timestamp() AS Audit_Row_Load_Ts,
    null AS Audit_Last_Update_Ts
FROM
    dev.silver_stg_erp.ebx_vm_vendor_overpaytrm;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Vendor_Altnt_Name
(
    Ent_Vendor_Id BIGINT NOT NULL,
    Altrnt_Name_Type_Code STRING NOT NULL,
    Altrnt_Name STRING,
    Start_Date DATE,
    End_Date DATE,
    Tech_Row_Inactv_Date DATE,
    Tech_Last_User_Id STRING,
    Tech_Creator_Id STRING,
    Tech_Create_Date_Ts TIMESTAMP,
    Tech_Last_Write_Ts TIMESTAMP,
    Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code. Identifies the database or data store from which the data was sourced.',
    Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Source System Code. Specifies the original source of the data, such as a specific business system or application.',
    Audit_Row_Load_Ts TIMESTAMP COMMENT 'Timestamp of when the record was loaded into the table.',
    Audit_Last_Update_Ts TIMESTAMP COMMENT 'Timestamp of the last update made to the record.'
)
AS 
SELECT
    CAST(vendor_ AS bigint) AS Ent_Vendor_Id,
    CAST(alternatenametype_ AS string) AS Altrnt_Name_Type_Code,
    CAST(alternatename AS string) AS Altrnt_Name,
    CAST(startdate AS date) AS Start_Date,
    CAST(enddate AS date) AS End_Date,
    CAST(technical_rowinactivedate AS date) AS Tech_Row_Inactv_Date,
    CAST(t_last_user_id AS string) AS Tech_Last_User_Id,
    CAST(t_creator_id AS string) AS Tech_Creator_Id,
    CAST(t_creation_date AS timestamp) AS Tech_Create_Date_Ts,
    CAST(t_last_write AS timestamp) AS Tech_Last_Write_Ts,
   'MDM' AS Audit_Ingest_Src_Sys_Code,
    'MDM' AS Audit_Orig_Src_Sys_Code,
    current_timestamp() AS Audit_Row_Load_Ts,
    null AS Audit_Last_Update_Ts
FROM
    dev.silver_stg_erp.EBX_VM_VENDOR_ALT_NAME;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Ref_Prod_Group
(
    Prod_Group_Code STRING NOT NULL,
    Prod_Group_Desc STRING,
    Tech_Last_User_Id STRING,
    Tech_Creator_Id STRING,
    Tech_Create_Date_Ts TIMESTAMP,
    Tech_Last_Write_Ts TIMESTAMP,
    Tech_Row_Inactv_Date DATE,
    Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code. Identifies the database or data store from which the data was sourced.',
    Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Source System Code. Specifies the original source of the data, such as a specific business system or application.',
    Audit_Row_Load_Ts TIMESTAMP COMMENT 'Timestamp of when the record was loaded into the table.',
    Audit_Last_Update_Ts TIMESTAMP COMMENT 'Timestamp of the last update made to the record.'
)
AS 
SELECT
    CAST(productgroup AS string) AS Prod_Group_Code,
    CAST(description AS string) AS Prod_Group_Desc,
    CAST(t_last_user_id AS string) AS Tech_Last_User_Id,
    CAST(t_creator_id AS string) AS Tech_Creator_Id,
    CAST(t_creation_date AS timestamp) AS Tech_Create_Date_Ts,
    CAST(t_last_write AS timestamp) AS Tech_Last_Write_Ts,
    CAST(technical_rowinactivedate AS date) AS Tech_Row_Inactv_Date,
    'MDM' AS Audit_Ingest_Src_Sys_Code,
    'MDM' AS Audit_Orig_Src_Sys_Code,
    current_timestamp() AS Audit_Row_Load_Ts,
    null AS Audit_Last_Update_Ts
FROM
    dev.silver_stg_erp.ebx_vr_vend_product_group;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Ref_Vendor_Altnt_Name_Type
(
    Altrnt_Name_Type_Code STRING NOT NULL,
    Altrnt_Name_Type_Desc STRING,
    Tech_Last_User_Id STRING,
    Tech_Creator_Id STRING,
    Tech_Create_Date_Ts TIMESTAMP,
    Tech_Last_Write_Ts TIMESTAMP,
    Tech_Row_Inactv_Date DATE,
    Audit_Ingest_Src_Sys_Code STRING COMMENT 'Audit Ingestion Source System Code. Identifies the database or data store from which the data was sourced.',
    Audit_Orig_Src_Sys_Code STRING COMMENT 'Audit Originating Source System Code. Specifies the original source of the data, such as a specific business system or application.',
    Audit_Row_Load_Ts TIMESTAMP COMMENT 'Timestamp of when the record was loaded into the table.',
    Audit_Last_Update_Ts TIMESTAMP COMMENT 'Timestamp of the last update made to the record.'
)
AS 
SELECT
    CAST(alternatenametype AS string) AS Altrnt_Name_Type_Code,
    CAST(description AS string) AS Altrnt_Name_Type_Desc,
    CAST(t_last_user_id AS string) AS Tech_Last_User_Id,
    CAST(t_creator_id AS string) AS Tech_Creator_Id,
    CAST(t_creation_date AS timestamp) AS Tech_Create_Date_Ts,
    CAST(t_last_write AS timestamp) AS Tech_Last_Write_Ts,
    CAST(technical_rowinactivedate AS date) AS Tech_Row_Inactv_Date,
    'MDM' AS Audit_Ingest_Src_Sys_Code,
    'MDM' AS Audit_Orig_Src_Sys_Code,
    current_timestamp() AS Audit_Row_Load_Ts,
    null AS Audit_Last_Update_Ts
FROM
    dev.silver_stg_erp.ebx_vr_vend_alt_name_type;


-- COMMAND ----------


