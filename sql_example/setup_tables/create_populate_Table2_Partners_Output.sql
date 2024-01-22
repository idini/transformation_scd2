-- create dataset transformation_scd2 into project my-prj
CREATE SCHEMA IF NOT EXISTS `my-prj.transformation_scd2`
  OPTIONS (
    description = 'schema for use case transformation scd2',
    location = 'europe-west6'
);

-- create table table_2_partners_output inside dataset transformation_scd2
CREATE TABLE IF NOT EXISTS `my-prj.transformation_scd2.table_2_partners_output` (
    TechnicalKey INTEGER NOT NULL,
    PartnerID INTEGER NOT NULL,
    Name STRING,
    Canton STRING,
    Date_From DATETIME NOT NULL,
    Date_To DATETIME NOT NULL,
    Is_valid STRING NOT NULL
);

-- populate table with data
INSERT INTO `my-prj.transformation_scd2.table_2_partners_output` (TechnicalKey, PartnerID, Name, Canton, Date_From, Date_To, Is_valid) VALUES
    (456789, 101, 'Store A', 'ZH', '2000-01-01 00:00:00', '9999-12-31 00:00:00', 'yes'),
    (123456, 102, 'Store B', 'BE', '2001-04-08 00:00:00', '9999-12-31 00:00:00', 'yes'),
    (789012, 103, 'Store C', 'BS', '2011-11-15 00:00:00', '9999-12-31 00:00:00', 'yes'),
    (345678, 104, 'Salon D', 'GE', '2002-02-08 00:00:00', '9999-12-31 00:00:00', 'yes'),
    (901234, 105, 'Bookshop E', 'ZH', '2023-05-01 00:00:00', '9999-12-31 00:00:00', 'yes')
;