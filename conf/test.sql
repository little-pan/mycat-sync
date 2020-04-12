-- test schema since 2020-03-24
--
-- mysql
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
