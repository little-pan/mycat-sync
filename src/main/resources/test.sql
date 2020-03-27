-- test schema since 2020-03-24
--
-- h2
create table company (
    id bigint not null primary key,
    name varchar(20) not null,
    address varchar(250),
    create_date date not null,
    unique u_company_name(name)
);
-- insert into company(id, name, create_date)values(1, 'c1', '2020-03-24'), (2, '公司2', '2020-02-25');

create table travelrecord (
    id bigint not null primary key,

);
