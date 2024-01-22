-- update row in order to simulate source update

UPDATE `my-prj.transformation_scd2.table_1_partners_input`
SET Canton = 'BS'
WHERE PartnerID = 105;

DELETE FROM `my-prj.transformation_scd2.table_1_partners_input`
WHERE PartnerID = 102;