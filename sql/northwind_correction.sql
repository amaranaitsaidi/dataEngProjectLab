

create table if not exists CustomerFeedback (
FeedbackID serial primary key,
CustomerId character varying(5) NOT NULL,
FeedbackText text,
FeedbackData TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
foreign key (CustomerId) REFERENCES customers(customer_id) 
);


alter table products add column category_id INT2;

alter table products 
add constraint fk_category
foreign key (category_id) references categories(category_id);

alter table customers  
alter column company_name set not null;

alter table orders 
alter column ship_city set not null;

create table if not exists orderItems (
orderid int2 not null,
productid int2 not null,
quantity smallint check (quantity > 0),
unitPrice numeric(10,2),
foreign key (orderid) references orders(order_id),
foreign key (productid) references products(product_id)
);


alter table orderItems 
alter column unitprice set not null;


alter table customers add column phonenumber varchar(15);

create table if not exists employeesales (
employeeid int not null,
orderid int not null,
salesAmount decimal(10,2),
foreign key (employeeid) references employees(employee_id),
foreign key (orderid) references orders(order_id)
);

alter table employeesales 
add constraint check_sales_amount
check (salesAmount > 0);


alter table products drop column discontinued;

alter table customerfeedback 
alter column feedbackdate set not null;

alter table orderitems 
add constraint fk_product_id
foreign key (productid) references products(product_id);

create table if not exists supplierproducts (
suppliersid int not null,
product_id int not null,
price decimal(10,2),
foreign key (product_id) references products(product_id)
);
