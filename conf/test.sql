-- test schema since 2020-03-24
--
-- mysql
--
-- Init some sequences for test
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('COMPANY', 10000, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('GLOBAL', 1, 50);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('EMPLOYEE', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('CUSTOMER', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('CUSTOMER_ADDR', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('goods', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('order', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('order_item', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('track', 1, 100);
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('play_record', 1, 100);
-- illegal config test
-- INSERT INTO mycat_sequence(name, current_value, increment) VALUES ('VIEWSPOT', 1, 0);

create table company (
    id bigint not null primary key,
    name varchar(20) not null,
    address varchar(250),
    create_date date not null,
    unique u_company_name(name)
);
-- insert into company(id, name, create_date)values(1, 'c1', '2020-03-24'), (2, '公司2', '2020-02-25');

create table employee (
    id bigint not null primary key auto_increment,
    company_id bigint not null,
    empno varchar(10) not null,
    name varchar(50) not null,
    salary integer default 5000 not null,
    gender char(1) default 'M' not null,
    entry_date date,
    leave_date date,
    unique u_employee_empno(company_id, empno),
    foreign key(company_id) references company(id)
);
-- insert into employee(id, company_id, empno, name)values(1, 1, '001', 'Peter'), (2, 1, '002', 'Tom');
-- insert into employee(id, company_id, empno, name)values(3, 2, '001', 'Kite'),  (4, 2, '002', 'John');

-- default dataNode table
create table if not exists hotel (
    id bigint not null auto_increment,
    name varchar(20) not null,
    address varchar(250),
    tel varchar(20),
    rooms int default 50 not null,
    primary key(id)
);

-- partition by file map
create table customer (
    id bigint not null auto_increment,
    sharding_id bigint not null,
    username varchar(50) not null,
    contact varchar(20),
    primary key(id),
    unique u_idx_customer_username(username)
);

create table customer_addr (
    id bigint not null auto_increment,
    customer_id bigint not null,
    address varchar(250) not null,
    primary key(id),
    foreign key(customer_id) references customer(id)
);

create table goods (
    id bigint not null auto_increment,
    company_id bigint not null,
    name varchar(50) not null,
    price decimal(7, 2) not null,
    stock int not null,
    primary key(id),
    foreign key(company_id) references company(id)
);

-- ER 2 level table with joinKey not partition column
create table `order` (
    id bigint not null auto_increment,
    customer_id bigint not null,
    quantity int not null,
    amount decimal(12, 2) not null,
    status int not null comment '1-Unpaid, 2-Paid, 3-Delivered, 4-Received, 5-Canceled, 6-Closed, 7-Deleted',
    create_time datetime not null,
    pay_time datetime,
    primary key(id),
    foreign key(customer_id) references customer(id)
);

-- ER 3 level table with joinKey not partition column
create table order_item (
    id bigint not null auto_increment,
    order_id bigint not null,
    goods_id bigint not null,
    goods_name varchar(50) not null,
    quantity int not null,
    price decimal(7, 2) not null,
    primary key(id),
    foreign key(order_id) references `order`(id),
    foreign key(goods_id) references goods(id)
);

create table artist (
    id bigint not null auto_increment,
    name varchar(50) not null,
    primary key(id)
);

-- ER 2 level table with joinKey as partition column
create table track (
    id bigint not null auto_increment,
    artist_id bigint not null,
    name varchar(50) not null,
    primary key(id),
    foreign key(artist_id) references artist(id)
);

-- ER 3 level table with joinKey not partition column
create table play_record (
    id bigint not null auto_increment,
    track_id bigint not null,
    customer_id bigint not null,
    play_time datetime not null,
    duration int not null,
    primary key(id),
    foreign key(track_id) references track(id)
);
