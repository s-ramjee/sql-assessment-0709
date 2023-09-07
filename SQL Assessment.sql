CREATE TABLE Orders (
    ORDER_ID            INT AUTO_INCREMENT PRIMARY KEY,
    ORDER_REF           VARCHAR(2000),
    ORDER_DATE          DATE,
    SUPPLIER_ID         INT,
    ORDER_TOTAL_AMOUNT  DECIMAL(10, 2),
    ORDER_DESCRIPTION   VARCHAR(2000),
    ORDER_STATUS        VARCHAR(2000),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES Suppliers(SUPPLIER_ID)
);


CREATE TABLE Suppliers (
    SUPPLIER_ID         INT AUTO_INCREMENT PRIMARY KEY,
    SUPPLIER_NAME       VARCHAR(2000),
    SUPP_CONTACT_NAME  VARCHAR(2000),
    SUPP_ADDRESS       VARCHAR(2000),
    SUPP_CONTACT_NUMBER VARCHAR(2000),
    SUPP_EMAIL         VARCHAR(2000)
);


CREATE TABLE Invoices (
    INVOICE_ID          INT AUTO_INCREMENT PRIMARY KEY,
    ORDER_ID            INT,
    INVOICE_REFERENCE   VARCHAR(2000),
    INVOICE_DATE        DATE,
    INVOICE_STATUS      VARCHAR(2000),
    INVOICE_HOLD_REASON VARCHAR(2000),
    INVOICE_AMOUNT      DECIMAL(10, 2),
    INVOICE_DESCRIPTION VARCHAR(2000),
    FOREIGN KEY (ORDER_ID) REFERENCES Orders(ORDER_ID)
);



--3
DELIMITER //

CREATE PROCEDURE MigrateDataFromXXBCM_ORDER_MGT()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE order_ref VARCHAR(2000);
    DECLARE order_date VARCHAR(2000);
    DECLARE supplier_name VARCHAR(2000);
    DECLARE supp_contact_name VARCHAR(2000);
    DECLARE supp_address VARCHAR(2000);
    DECLARE supp_contact_number VARCHAR(2000);
    DECLARE supp_email VARCHAR(2000);
    DECLARE order_total_amount DECIMAL(10, 2);
    DECLARE order_description VARCHAR(2000);
    DECLARE order_status VARCHAR(2000);
    DECLARE order_line_amount DECIMAL(10, 2);
    DECLARE invoice_reference VARCHAR(2000);
    DECLARE invoice_date VARCHAR(2000);
    DECLARE invoice_status VARCHAR(2000);
    DECLARE invoice_hold_reason VARCHAR(2000);
    DECLARE invoice_amount DECIMAL(10, 2);
    DECLARE invoice_description VARCHAR(2000);
    
    DECLARE cur CURSOR FOR SELECT * FROM XXBCM_ORDER_MGT;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN cur;
    
    migration_loop: LOOP
        FETCH cur INTO
            order_ref, order_date, supplier_name, supp_contact_name, supp_address, supp_contact_number, supp_email,
            order_total_amount, order_description, order_status, order_line_amount, invoice_reference, invoice_date,
            invoice_status, invoice_hold_reason, invoice_amount, invoice_description;
        
        IF done THEN
            LEAVE migration_loop;
        END IF;
        
        INSERT INTO Suppliers (SUPPLIER_NAME, SUPP_CONTACT_NAME, SUPP_ADDRESS, SUPP_CONTACT_NUMBER, SUPP_EMAIL)
        VALUES (supplier_name, supp_contact_name, supp_address, supp_contact_number, supp_email);
        
        INSERT INTO Orders (ORDER_REF, ORDER_DATE, SUPPLIER_ID, ORDER_TOTAL_AMOUNT, ORDER_DESCRIPTION, ORDER_STATUS)
        VALUES (order_ref, STR_TO_DATE(order_date, '%d-%b-%Y'), LAST_INSERT_ID(), order_total_amount, order_description, order_status);
        
        INSERT INTO Invoices (ORDER_ID, INVOICE_REFERENCE, INVOICE_DATE, INVOICE_STATUS, INVOICE_HOLD_REASON, INVOICE_AMOUNT, INVOICE_DESCRIPTION)
        VALUES (LAST_INSERT_ID(), invoice_reference, STR_TO_DATE(invoice_date, '%d-%b-%Y'), invoice_status, invoice_hold_reason, invoice_amount, invoice_description);
    END LOOP;
    
    CLOSE cur;
    
END //

DELIMITER ;




--4
DELIMITER $$

CREATE PROCEDURE generateDistinctInvoicesAndTheirTotalAmount()
BEGIN
    SELECT 
        SUBSTRING(ORDER_REF, 3) AS 'Order Reference',
        DATE_FORMAT(ORDER_DATE, '%b-%Y') AS 'Order Period',
        CONCAT(UPPER(LEFT(S.SUPPLIER_NAME, 1)), LOWER(SUBSTRING(S.SUPPLIER_NAME, 2))) AS 'Supplier Name',
        FORMAT(ORDER_TOTAL_AMOUNT, 2) AS 'Order Total Amount',
        ORDER_STATUS AS 'Order Status',
        I.INVOICE_REFERENCE AS 'Invoice Reference',
        FORMAT(I.INVOICE_AMOUNT, 2) AS 'Invoice Total Amount',
        CASE
            WHEN SUM(CASE WHEN I.INVOICE_STATUS = 'Paid' THEN 1 ELSE 0 END) = COUNT(*) THEN 'OK'
            WHEN SUM(CASE WHEN I.INVOICE_STATUS = 'Pending' THEN 1 ELSE 0 END) > 0 THEN 'To follow up'
            ELSE 'To verify'
        END AS 'Action'
    FROM 
        Orders O
    JOIN 
        Suppliers S ON O.SUPPLIER_ID = S.SUPPLIER_ID
    LEFT JOIN 
        Invoices I ON O.ORDER_ID = I.ORDER_ID
    GROUP BY 
        O.ORDER_ID
    ORDER BY 
        O.ORDER_DATE DESC;
END$$

DELIMITER ;


--5
DELIMITER $$

CREATE PROCEDURE getSecondHighestOrderDetails()
BEGIN
    SELECT 
        SUBSTRING(ORDER_REF, 3) AS 'Order Reference',
        DATE_FORMAT(ORDER_DATE, '%M %d, %Y') AS 'Order Date',
        UPPER(S.SUPPLIER_NAME) AS 'Supplier Name',
        FORMAT(ORDER_TOTAL_AMOUNT, 2) AS 'Order Total Amount',
        ORDER_STATUS AS 'Order Status',
        GROUP_CONCAT(I.INVOICE_REFERENCE ORDER BY I.INVOICE_REFERENCE ASC SEPARATOR '|') AS 'Invoice References'
    FROM 
        Orders O
    JOIN 
        Suppliers S ON O.SUPPLIER_ID = S.SUPPLIER_ID
    LEFT JOIN 
        Invoices I ON O.ORDER_ID = I.ORDER_ID
    WHERE
        ORDER_TOTAL_AMOUNT = (
            SELECT 
                MAX(ORDER_TOTAL_AMOUNT)
            FROM 
                Orders
            WHERE 
                ORDER_TOTAL_AMOUNT < (
                    SELECT 
                        MAX(ORDER_TOTAL_AMOUNT)
                    FROM 
                        Orders
                )
        )
    GROUP BY 
        O.ORDER_ID
    LIMIT 1;
END$$

DELIMITER ;





--6
DELIMITER $$

CREATE PROCEDURE GetSupplierOrderSummary()
BEGIN
    SELECT 
        S.SUPPLIER_NAME AS 'Supplier Name',
        S.SUPP_CONTACT_NAME AS 'Supplier Contact Name',
        CASE
            WHEN LENGTH(S.SUPP_CONTACT_NUMBER) = 8 THEN CONCAT(LEFT(S.SUPP_CONTACT_NUMBER, 4), '-', RIGHT(S.SUPP_CONTACT_NUMBER, 4))
            ELSE S.SUPP_CONTACT_NUMBER
        END AS 'Supplier Contact No. 1',
        CASE
            WHEN LENGTH(S.SUPP_CONTACT_NUMBER2) = 8 THEN CONCAT(LEFT(S.SUPP_CONTACT_NUMBER2, 4), '-', RIGHT(S.SUPP_CONTACT_NUMBER2, 4))
            ELSE S.SUPP_CONTACT_NUMBER2
        END AS 'Supplier Contact No. 2',
        COUNT(O.ORDER_ID) AS 'Total Orders',
        FORMAT(SUM(O.ORDER_TOTAL_AMOUNT), 2) AS 'Order Total Amount'
    FROM 
        Suppliers S
    LEFT JOIN 
        Orders O ON S.SUPPLIER_ID = O.SUPPLIER_ID
    WHERE
        O.ORDER_DATE BETWEEN '2022-01-01' AND '2022-08-31'
    GROUP BY 
        S.SUPPLIER_ID
    ORDER BY 
        S.SUPPLIER_NAME;
END$$

DELIMITER ;