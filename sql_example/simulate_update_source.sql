-- update row in order to simulate source update

UPDATE `worldline-prj.transformation_scd2.table_1_partners_input`
SET Canton = 'BS'
WHERE PartnerID = 105;

DELETE FROM `worldline-prj.transformation_scd2.table_1_partners_input`
WHERE PartnerID = 102;