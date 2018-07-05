/***
Author    : Cata Gonzales
Create Date : 09Mar1818
Version   : 1.0
Description : WAPP-2206 Covert Cron and Active Users Report to Stored Proc

***/

DELIMITER //

DROP PROCEDURE IF EXISTS `sp_active_users_report`//

CREATE PROCEDURE `sp_active_users_report`(vcountry VARCHAR(5), vstatus VARCHAR(10), vstart_date DATE, vend_date DATE, vtest_account INT, vcolumnsort VARCHAR(25),vsortby VARCHAR(5),vkeyword VARCHAR(30))
BEGIN 
  
  DROP TEMPORARY TABLE  IF EXISTS TEMP1;
  DROP TEMPORARY TABLE  IF EXISTS TEMP2;
  DROP TEMPORARY TABLE  IF EXISTS TEMP3;
  DROP TEMPORARY TABLE  IF EXISTS TEMP4;
  DROP TEMPORARY TABLE  IF EXISTS TEMP5;
  DROP TEMPORARY TABLE  IF EXISTS TEMP6;

  CREATE TEMPORARY TABLE TEMP1
  AS
  SELECT 
  P.id AS ewallet_profile_id,
  P.first_name AS first_name,
  P.last_name AS last_name,
  P.country_code AS country_code,
  P.mobile_phone_number AS mobile_phone_number, 
  P.email_address AS email_address,
  P.username AS username,
  DATE_FORMAT(P.date_created, '%d/%m/%Y') AS registration_date_formatted,
  P.date_created AS registration_date,
  P.no_area_code_mobile_phone_number AS no_area_code_mobile_phone_number,
  P.failed_login_count AS failed_login_count,
  C.name AS send_country,
  E.hashed_card_number AS hashed_card_number,
  IF ((K.is_personal_info_verified = 1 AND K.is_address_verified = 1 AND K.is_salary_range_verified =1) AND (IF(country_code NOT IN ("US","CA") OR K.is_pin_verified = 1, 1, 0)), "verified", "unverified" ) AS user_status,
  IF( VC.date_validated IS NOT NULL, "activated", IF( ( DATEDIFF( CURDATE(), VC.date_generated ) >= 2 ), "expired", "notactivated" ) ) AS status1,
  IF(P.timezone > "", P.timezone, IF(TN.timezone > "", TN.timezone, (SELECT _tn.timezone FROM timezone_name _tn WHERE _tn.countrycode = IF(P.country_code = "UK", "GB", P.country_code) LIMIT 0,1) ) ) AS timezone,
  FROM_UNIXTIME(UNIX_TIMESTAMP(P.date_created) -28800 + IF(TN.gmt_offset_seconds > "", TN.gmt_offset_seconds, TN2.gmt_offset_seconds)) AS converted_date_created,
  IF(P.user_type = 1, "individual", "corporate") AS account_type,
  (SELECT 
    COUNT(tt) 
  FROM
    (SELECT 
    T.ewallet_account_number_id AS tt 
    FROM
    ewallet_transactions T 
    WHERE T.ewallet_transaction_type_id IN (SELECT `ewallet_transaction_type_id` FROM ewallet_transaction_type_id_mapping WHERE ewallet_transaction_type_id != 0 AND user_initiated = 1)
     AND T.type='debit'
     AND T.transaction_date BETWEEN vstart_date AND  vend_date  
    GROUP BY T.ewallet_account_number_id, T.processor_reference_number) P 
  WHERE tt = L.ewallet_account_number_id) AS no_transactions,
  (SELECT 
    transaction_date 
  FROM
    ewallet_transactions T 
  WHERE T.ewallet_account_number_id = L.ewallet_account_number_id       
    AND T.ewallet_transaction_type_id IN (SELECT `ewallet_transaction_type_id` FROM ewallet_transaction_type_id_mapping WHERE ewallet_transaction_type_id != 0 AND user_initiated = 1)
    AND T.type='debit'
    AND T.transaction_date BETWEEN vstart_date AND  vend_date 
    GROUP BY T.ewallet_account_number_id, T.processor_reference_number
  ORDER BY transaction_date DESC 
  LIMIT 1) AS last_transaction_date

  FROM ewallet_profiles P    
  LEFT JOIN ewallet_profiles_ewallet_account_numbers L 
    ON L.ewallet_profile_id = P.id 
  LEFT JOIN ewallet_account_numbers E 
    ON E.id = L.ewallet_account_number_id
  LEFT JOIN kyc_verification_status K 
    ON P.id = K.ewallet_profile_id
  LEFT JOIN countries C 
    ON C.iso = P.country_code
  LEFT JOIN ewallet_verification_codes VC 
    ON VC.ewallet_profile_id = P.id
  LEFT JOIN timezone_name TN 
    ON TN.countrycode = IF(P.country_code = "UK", "GB", P.country_code) 
    AND TN.timezone = P.timezone
  LEFT JOIN timezone_name TN2 
    ON TN2.countrycode = IF(P.country_code = "UK", "GB", P.country_code) 
    AND TN2.timezone = (SELECT _tn2.timezone FROM timezone_name _tn2 WHERE _tn2.countrycode = IF(P.country_code = "UK", "GB", P.country_code) LIMIT 0,1);

  -- apply date range criteria
  CREATE TEMPORARY TABLE TEMP2 LIKE TEMP1;
  INSERT INTO TEMP2
  SELECT *
  FROM TEMP1
  WHERE last_transaction_date BETWEEN vstart_date AND  vend_date;

  
  -- APPLY COUNTRY filter
  CREATE TEMPORARY TABLE TEMP3 LIKE TEMP2;  
  IF (vcountry <> '') THEN
    INSERT INTO TEMP3
    SELECT *
    FROM TEMP2 
    WHERE country_code = vcountry;
  ELSE 
    INSERT INTO TEMP3
    SELECT *
    FROM TEMP2;   
  END IF;
  
  -- APPLY STATUS filter
  CREATE TEMPORARY TABLE TEMP4 LIKE TEMP3;  
  IF (LTRIM(vstatus) <> '') THEN

    IF (vstatus = 'inactive') THEN
      INSERT INTO TEMP4
      SELECT *
      FROM TEMP3 
      WHERE no_transactions = 0;
      
    ELSEIF (vstatus = 'active') THEN
      
      INSERT INTO TEMP4
      SELECT *
      FROM TEMP3 
      WHERE no_transactions >=1;
      
    ELSE
      
      INSERT INTO TEMP4
      SELECT *
      FROM TEMP3 
      WHERE no_transactions >1;
    
    END IF;
    
  ELSE 
  
    INSERT INTO TEMP4
    SELECT *
    FROM TEMP3; 
    
  END IF;
  
  -- apply test account inclusion/exclusion
  CREATE TEMPORARY TABLE TEMP5 LIKE TEMP4;
  IF (vtest_account = 0) THEN
    
    INSERT INTO TEMP5
    SELECT *
    FROM TEMP4;   

  ELSEIF  (vtest_account = 1) THEN
    INSERT INTO TEMP5
    SELECT *
    FROM TEMP4 
    WHERE ewallet_profile_id NOT IN (SELECT ewallet_profile_id FROM ewallet_profiles_test_accounts);

  ELSE
    INSERT INTO TEMP5
    SELECT *
    FROM TEMP4
    WHERE ewallet_profile_id IN 
      (SELECT ewallet_profile_id FROM ewallet_profiles_test_accounts);
      
  END IF;

  -- apply search keyword
  CREATE TEMPORARY TABLE TEMP6 LIKE TEMP5;
  IF (LTRIM(vkeyword) <> '') THEN
    
    INSERT INTO TEMP6
    SELECT *
    FROM TEMP5
    WHERE (
        CONCAT_WS(' ', first_name, last_name) LIKE CONCAT('%',vkeyword,'%') OR 
        mobile_phone_number LIKE CONCAT('%',vkeyword,'%') OR 
        username LIKE CONCAT('%',vkeyword,'%') OR
        send_country LIKE CONCAT('%',vkeyword,'%') OR 
        no_area_code_mobile_phone_number LIKE CONCAT('%',vkeyword,'%') OR         
        email_address LIKE CONCAT('%',vkeyword,'%') OR         
        account_type LIKE CONCAT('%',vkeyword,'%'));

  ELSE
    INSERT INTO TEMP6
    SELECT *
    FROM TEMP5;
      
  END IF;


--  if (UPPER(LTRIM(vsortby)) <> 'ASC' or UPPER(LTRIM(vsortby)) <> 'DESC') then
--    SET vsortby = 'asc';
--  end if;
  

  SELECT ewallet_profile_id, first_name, last_name, country_code, mobile_phone_number, email_address, username, registration_date_formatted,
    registration_date, no_area_code_mobile_phone_number, failed_login_count, send_country, hashed_card_number, user_status, status1, timezone,
    converted_date_created, account_type, no_transactions, last_transaction_date
  FROM TEMP6
  ORDER BY
    CASE  WHEN vcolumnsort = 'registration_date' AND vsortby = 'asc' THEN registration_date END ASC,
    CASE  WHEN vcolumnsort = 'registration_date' AND vsortby = 'desc' THEN registration_date END DESC,
    
    CASE  WHEN vcolumnsort = 'first_name' AND vsortby = 'asc' THEN first_name END ASC,
    CASE  WHEN vcolumnsort = 'first_name' AND vsortby = 'desc' THEN first_name END DESC,
    
    CASE  WHEN vcolumnsort = 'last_name' AND vsortby = 'asc' THEN last_name END ASC,
    CASE  WHEN vcolumnsort = 'last_name' AND vsortby = 'desc' THEN last_name END DESC,
    
    CASE  WHEN vcolumnsort = 'email_address' AND vsortby = 'asc' THEN email_address END ASC,
    CASE  WHEN vcolumnsort = 'email_address' AND vsortby = 'desc' THEN email_address END DESC,
    
    CASE  WHEN vcolumnsort = 'mobile_phone_number' AND vsortby = 'asc' THEN mobile_phone_number END ASC,
    CASE  WHEN vcolumnsort = 'mobile_phone_number' AND vsortby = 'desc' THEN mobile_phone_number END DESC,
    
    CASE  WHEN vcolumnsort = 'send_country' AND vsortby = 'asc' THEN send_country END ASC,
    CASE  WHEN vcolumnsort = 'send_country' AND vsortby = 'desc' THEN send_country END DESC,
    
    CASE  WHEN vcolumnsort = 'no_transactions' AND vsortby = 'asc' THEN no_transactions END ASC,
    CASE  WHEN vcolumnsort = 'no_transactions' AND vsortby = 'desc' THEN no_transactions END DESC,

    CASE  WHEN vcolumnsort = 'last_transaction_date' AND vsortby = 'asc' THEN last_transaction_date END ASC,
    CASE  WHEN vcolumnsort = 'last_transaction_date' AND vsortby = 'desc' THEN last_transaction_date END DESC
    ;
END//

DELIMITER ;