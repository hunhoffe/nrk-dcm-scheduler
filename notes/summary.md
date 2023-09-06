# DCM Configuration in DiNOS

## Database Tables

The database tables are:
```sql
create table nodes(
    id integer,
    cores integer,
    memslices integer,
    primary key (id));

create table applications(
    id integer,
    primary key (id));

create table pending(
    id long not null auto_increment,
    application integer,
    cores integer,
    memslices integer,
    status varchar(36),
	current_node integer,
	controllable__node integer,
	foreign key (controllable__node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (id));

create table placed(
    application integer,
    node integer,
    cores integer,
    memslices integer,
	foreign key (node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (application, node));
```

## Additional Views

Additional views used to simplify what DCM sees are:
```sql
create view allocated as
  select node, cast(sum(cores) as int) as cores, cast(sum(memslices) as int) as memslices
  from placed
  group by node

create view unallocated as
  select n.id as node, cast(n.cores - coalesce(sum(p.cores), 0) as int) as cores,
    cast(n.memslices - coalesce(sum(p.memslices), 0) as int) as memslices
  from nodes n
    left join placed p
      on n.id = p.node
      group by n.id

create view app_nodes as
  select application, node
  from placed
  group by application, node
```

I think this spare view is performance problem:
```sql
create constraint spare_view as
select unallocated.node, unallocated.cores - sum(pending.cores) as cores,
  unallocated.memslices - sum(pending.memslices) as memslices
from pending
join unallocated
  on unallocated.node = pending.controllable__node
group by unallocated.node, unallocated.cores, unallocated.memslices
```
My indication of that is this error message:
```
2023-09-06 07:44:54 WARN [TableRowGenerator{table=PENDING}, TableRowGenerator{table=UNALLOCATED}] are being iterated using nested for loops
```

## DCMcap (cap = capacity only) Policies

#### Placed Constraint
```sql
create constraint placed_constraint as
select * from pending
where status = 'PLACED'
check current_node = controllable__node
```

#### Capacity Functions

```sql
create constraint core_cap as
select * from pending
join unallocated
  on unallocated.node = pending.controllable__node
check capacity_constraint(pending.controllable__node, unallocated.node, pending.cores,
  unallocated.cores) = true

create constraint mem_cap as
select * from pending
join unallocated
  on unallocated.node = pending.controllable__node
check capacity_constraint(pending.controllable__node, unallocated.node, pending.memslices,
  unallocated.memslices) = true
```

## DCMloc (loc = cap + locality) Policies

#### Locality with placed resources constraint:
```sql
create constraint app_locality_placed_constraint as
select * from pending
  maximize (pending.controllable__node in
    (select node
      from app_nodes
      where app_nodes.application = pending.application
  ))
```

#### Locality with pending resources constraint:
```sql
create constraint app_locality_pending_constraint as
select * from pending
maximize
  (pending.controllable__node in
    (select b.controllable__node
      from pending as b
      where b.application = pending.application
      and not b.id = pending.id
  ))
```

## Unused Policies

#### Hand-written capacity constraint:
```sql
create constraint capacity_constraint as
select * from spare_view
check cores >= 0 and memslices >= 0
```

#### Hand-written load balancing constraint:
```sql
create constraint app_locality_constraint as
select * from pending
maximize
  (pending.controllable__node in
    (select b.controllable__node
      from pending as b
      where b.application = pending.application
      and not b.id = pending.id
    ))
  + (pending.controllable__node in
    (select node
      from app_nodes
      where app_nodes.application = pending.application
    ))
```

#### Symmetry breaking constraint:
```sql
create constraint constraint_symmetry_breaking as
  select *
  from pending
  group by application, cores, memslices
  check increasing(controllable__node) = true
```
