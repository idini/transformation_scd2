-- create dataset transformation_scd2 into project my-prj
CREATE SCHEMA IF NOT EXISTS `my-prj.transformation_scd2`
  OPTIONS (
    description = 'schema for use case transformation scd2',
    location = 'europe-west6'
);

-- create table table_2_partners_output inside dataset transformation_scd2
CREATE TABLE IF NOT EXISTS `my-prj.transformation_scd2.table_1_partners_input` (
    PartnerID INTEGER NOT NULL,
    Name STRING,
    Canton STRING
);

-- populate table with data
INSERT INTO `my-prj.transformation_scd2.table_1_partners_input` (PartnerID, Name, Canton) VALUES
    (101, 'Store A', 'ZH'),
    (102, 'Store B', 'BE'),
    (103, 'Store C', 'BS'),
    (104, 'Salon D', 'GE'),
    (105, 'Bookshop E', 'ZH')
;
