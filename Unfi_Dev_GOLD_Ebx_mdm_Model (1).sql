-- Databricks notebook source

CREATE OR REFRESH  LIVE TABLE Dim_Vendor 
( 
  Ent_Vendor_id  BIGINT Not NULL,
  Vendor_Type_code STRING ,
  Vendor_Name STRING ,
  Bus_Party_Role_Code STRING ,
  Bus_Party_Role_Desc_Text STRING ,
  Pmt_Terms_Code STRING,
  Pmt_Method_Code STRING,
  Incoterms_Type_Code STRING,
  -- Fed_Id STRING,
  Duns_Nbr STRING,
  Bank_Cntry_Code_ STRING,
  --  Fncl_Institution_Ach_Routing_Nbr STRING,
  -- Bank_Acct_Nbr STRING,
  Minority_Bus_Party_Owners_Name_ STRING,
  Addr_Ln_1_Text STRING,
  Addr_Ln_2_Text STRING,
  City_Name STRING,
  State_Province_Code STRING,
  Postal_Code STRING,
  Cntry_Code_ STRING,
  CDC_Tracker Timestamp

)
AS  
SELECT 
t1.Ent_Vendor_Id,
t1.Vendor_Type_Code, 
t1.Vendor_Name,
'null' as Bus_Party_Role_Code ,-- dev.silver_erp.Ref_Bus_Party_Role.Bus_Party_Role_Code,
 NULL as Bus_Party_Role_Desc_Text,-- dev.silver_erp.Ref_Bus_Party_Role.Bus_Party_Role_Desc_Text, 
t4.Pmt_Terms_Code,
t5.Pmt_Method_Code,
t1.Incoterms_Type_Code, 
-- t3.Fed_Id, 
Cast(t3.Duns_Nbr as string) as Duns_Nbr, 
t3.Bank_Data_Bank_Cntry_Code as Bank_Cntry_Code_ , 
--t3.Fncl_Institution_Ach_Routing_Nbr, 
-- t3.Bank_Acct_Nbr,
t3.Minority_Bus_Party_Owners_Name_,
t2.Addr_Ln_1_Text,
t2.Addr_Ln_2_Text,
t2.City_Name,
t2.State_Province_Code, 
t2.Postal_Code, 
t2.Cntry_Code_,
t1.CDC_Tracker
FROM dev.silver_erp.Vendor t1
LEFT JOIN dev.silver_erp.Bus_Party_Indvdl t6 ON t1.Bus_Party_Ind_Id_ =  t6.Ent_Bus_Party_Id 
LEFT JOIN dev.silver_erp.Bus_Party_Addr t2 ON t6.Ent_Addr_Id =t2.Ent_Addr_Id
LEFT JOIN dev.silver_erp.Bus_Party_Org t3 ON  t1.Bus_Party_Org_Id_ = t3.Ent_Bus_Party_Id     
LEFT JOIN dev.silver_erp.Vendor_Pmt_Data_Pmt_Terms t4 ON  t1.Ent_Vendor_Id= t4.Ent_Vendor_Id 
LEFT JOIN dev.silver_erp.Vendor_Pmt_Data_Pmt_Methods t5 ON t1.Ent_Vendor_Id= t5.Ent_Vendor_Id    ;
--,dev.silver_erp.Ref_Bus_Party_Role;


-- COMMAND ----------


-- CREATE OR REFRESH  LIVE TABLE Dim_Vendor 
-- ( 
--   Ent_Vendor_id  BIGINT NOT NULL,
--   Vendor_Type_code STRING ,
--   Vendor_Name STRING ,
--   Bus_Party_Role_Code STRING NOT NULL,
--   Bus_Party_Role_Desc_Text STRING ,
--   Pmt_Terms_Code STRING,
--   Pmt_Method_Code STRING,
--   Incoterms_Type_Code STRING,
--   Fed_Id STRING,
--   Duns_Nbr STRING,
--   Bank_Cntry_Code_ STRING,
--   Fncl_Institution_Ach_Routing_Nbr STRING,
--   Bank_Acct_Nbr STRING,
--   Minority_Bus_Party_Owners_Name_ STRING,
--   Addr_Ln_1_Text STRING,
--   Addr_Ln_2_Text STRING,
--   City_Name STRING,
--   State_Province_Code STRING,
--   Postal_Code STRING,
--   Cntry_Code_ STRING

-- )
-- AS 
-- SELECT 
-- dev.silver_erp.Vendor.Ent_Vendor_Id,
-- dev.silver_erp.Vendor.Vendor_Type_Code, 
-- dev.silver_erp.Vendor.Vendor_Name,
-- 'null' as Bus_Party_Role_Code ,-- dev.silver_erp.Ref_Bus_Party_Role.Bus_Party_Role_Code,
-- null as Bus_Party_Role_Desc_Text,-- dev.silver_erp.Ref_Bus_Party_Role.Bus_Party_Role_Desc_Text, 
-- dev.silver_erp.Vendor_Pmt_Data_Pmt_Terms.Pmt_Terms_Code,
-- dev.silver_erp.Vendor_Pmt_Data_Pmt_Methods.Pmt_Method_Code,
-- dev.silver_erp.Vendor.Incoterms_Type_Code, 
-- dev.silver_erp.Bus_Party_Org.Fed_Id, 
-- dev.silver_erp.Bus_Party_Org.Duns_Nbr, 
-- dev.silver_erp.Bus_Party_Org.Bank_Cntry_Code_, 
-- dev.silver_erp.Bus_Party_Org.Fncl_Institution_Ach_Routing_Nbr, 
-- dev.silver_erp.Bus_Party_Org.Bank_Acct_Nbr,
-- dev.silver_erp.Bus_Party_Org.Minority_Bus_Party_Owners_Name_,
-- dev.silver_erp.Bus_Party_Addr.Addr_Ln_1_Text,
-- dev.silver_erp.Bus_Party_Addr.Addr_Ln_2_Text,
-- dev.silver_erp.Bus_Party_Addr.City_Name,
-- dev.silver_erp.Bus_Party_Addr.State_Province_Code, 
-- dev.silver_erp.Bus_Party_Addr.Postal_Code, 
-- dev.silver_erp.Bus_Party_Addr.Cntry_Code_
-- FROM dev.silver_erp.Vendor,
-- dev.silver_erp.Bus_Party_Addr,
-- dev.silver_erp.Bus_Party_Org,
-- dev.silver_erp.Vendor_Pmt_Data_Pmt_Terms,
-- dev.silver_erp.Vendor_Pmt_Data_Pmt_Methods ;
-- ,dev.silver_erp.Ref_Bus_Party_Role;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## vendor

-- COMMAND ----------

--DROP Table if exists vendor;

CREATE OR REFRESH  LIVE TABLE Vendor_v 
as
select 
Ent_Vendor_Id, 
Vendor_Name, 
Bus_Party_Org_Id_,
Bus_Party_Ind_Id_,
Vendor_Type_Code, 
Unfi_Acct_Nbr, 
Incoterms_Type_Code, 
Pmt_Terms_Code, Pmt_Method_Code
from dev.silver_erp.vendor

-- ( 
--   Ent_Vendor_id  BIGINT NOT NULL,
--   Vendor_Name STRING ,
--   Bus_Party_Org_Id_ BIGINT, 
--   Bus_Party_Ind_Id_ BIGINT, 
--   Vendor_Type_Code STRING ,
--   Unfi_Acct_Nbr STRING, 
--   Incoterms_Type_Code STRING,
--   Pmt_Terms_Code STRING,
--   Pmt_Method_Code STRING
-- )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## vendor_bus_sys_relshnship

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE vendor_bus_sys_relnshp_v 
as
select 
Ent_Vendor_Id,
Bus_Src_Sys_Id, 
Vendor_Legacy_Id
from dev.silver_erp.vendor_bus_sys_relnshp

-- ( 
--   Ent_Vendor_id  BIGINT NOT NULL,
--   Bus_Src_Sys_Id BIGINT ,
--   Vendor_Legacy_Id STRING 
-- )

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE vendor_pmt_data_pmt_terms_v 
as
select
Ent_Vendor_Id, 
Pmt_Terms_Code
from dev.silver_erp.vendor_pmt_data_pmt_terms

-- ( 
--   Ent_Vendor_id  BIGINT NOT NULL,
--   Pmt_Terms_Code STRING 
-- )

-- COMMAND ----------


CREATE OR REFRESH  LIVE TABLE vendor_pmt_data_pmt_methods_v 
as
select 
Ent_Vendor_Id, 
Pmt_Method_Code
from dev.silver_erp.vendor_pmt_data_pmt_methods 

-- ( 
--   Ent_Vendor_id  BIGINT NOT NULL,
--   Pmt_Method_Code STRING 
-- )

-- COMMAND ----------


CREATE OR REFRESH  LIVE TABLE bus_party_addr_v 
as
select 
Ent_Addr_Id, 
Addr_Attn_To_Text, 
Addr_Ln_1_Text, 
Addr_Ln_2_Text, 
Apartment_Suite_Addr_Text, 
City_Name,
State_Province_Code,
Postal_Code,
Cntry_Code_,
Latitude_Coord_Nbr,
Longitude_Coord_Nbr,
Formatted_Addr_Text,
Pobox_Military_Unit_Nbr,
Fpo_Apo_Text
from dev.silver_erp.bus_party_addr


-- ( 
--   Ent_Addr_Id  BIGINT NOT NULL,
--   Addr_Attn_To_Text STRING ,
--   Addr_Ln_1_Text STRING, 
--   Addr_Ln_2_Text STRING, 
--   Apartment_Suite_Addr_Text STRING,
--   City_Name STRING, 
--   State_Province_Code STRING,
--   Postal_Code STRING,
--   Cntry_Code_ STRING, 
--   Latitude_Coord_Nbr STRING,
--   Longitude_Coord_Nbr STRING,
--   Formatted_Addr_Text STRING,
--   Pobox_Military_Unit_Nbr STRING,
-- Fpo_Apo_Text STRING)

-- COMMAND ----------


CREATE OR REFRESH  LIVE TABLE bus_party_indvdl_v 
as
select 	
Ent_Bus_Party_Id,
Bus_Party_Name, 
First_Name, 
Mid_Name, 
Last_Name, 
Title_Name, 
Nick_Name, 
Ssn, 
cast (Ent_Addr_Id as bigint ), 
Indvdl_Cntct_Name, 
Indvdl_Addresses_Text, 
Indvdl_Phone_Nbr, 
Indvdl_Email_Addr_Text	
from dev.silver_erp.bus_party_indvdl_archive		

-- ( 
--   Ent_Bus_Party_Id  BIGINT NOT NULL,
--   Bus_Party_Name STRING, 
--   First_Name STRING, 
--   Mid_Name STRING, 
--   Last_Name STRING, 
--   Title_Name STRING, 
--   Nick_Name STRING, 
--   Ssn STRING, 
--   Ent_Addr_Id  BIGINT , 
--   Indvdl_Cntct_Name STRING, 
--   Indvdl_Addresses_Text STRING, 
--   Indvdl_Phone_Nbr STRING, 
--   Indvdl_Email_Addr_Text STRING,	
--   Fpo_Apo_Text STRING
-- )

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE bus_party_org_v 
as
select 
Ent_Bus_Party_Id , 
Org_Name, 
Fed_Id, 
Duns_Nbr, 
Org_Tax_Jurisdiction_Nbr, 
Minority_Bus_Party_Owners_Name_	Website_Addr_Text, 
Is_Fncl_Institution_Flag, 
Ent_Addr_Id, Org_Phone_Nbr, 
Org_Email_Addr_Text, 
Org_Cntct_Name, 
Bank_Cntry_Code_, 
Bank_Acct_Nbr, 
International_Bank_Acct_Nbr, 
Fncl_Institution_Type_Code_, 
Fncl_Institution_Ach_Routing_Nbr, 
Org_Addr
from dev.silver_erp.bus_party_org_archive 	

-- ( 
--   Ent_Bus_Party_Id  BIGINT NOT NULL,
--   Org_Name STRING , 
--   Fed_Id STRING, 
--   Duns_Nbr STRING, 
--   Org_Tax_Jurisdiction_Nbr STRING, 
--   Minority_Bus_Party_Owners_Name_	 STRING,
--   Website_Addr_Text STRING, 
--   Is_Fncl_Institution_Flag BOOLEAN, 
--   Ent_Addr_Id BIGINT,
--   Org_Phone_Nbr STRING, 
--   Org_Email_Addr_Text STRING, 
--   Org_Cntct_Name STRING, 
--   Bank_Cntry_Code_ STRING, 
--   Bank_Acct_Nbr STRING, 
--   International_Bank_Acct_Nbr STRING, 
--   Fncl_Institution_Type_Code_ STRING, 
--   Fncl_Institution_Ach_Routing_Nbr STRING, 
--   Org_Addr STRING)

-- COMMAND ----------

CREATE OR REFRESH  LIVE TABLE ref_vendor_type_v 
as 
select 
Vendor_Type_Code, 
Vendor_Type_Desc_Text
from dev.silver_erp.ref_vendor_type

-- ( 
--   Vendor_Type_Code STRING,
--   Vendor_Type_Desc_Text STRING 
-- )

-- COMMAND ----------


