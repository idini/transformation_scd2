-- update row in order to simulate source update

UPDATE `worldline-prj.transformation_scd2.table_1_partners_input`
SET Canton = 'BS'
WHERE PartnerID = 105;

delete from `worldline-prj.transformation_scd2.table_1_partners_input`
where PartnerID = 102;