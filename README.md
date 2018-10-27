# Exemplos 

Implementações de operações em SQL em MapReduce.

## Filter

```sql
select * from lineitem where l_shipmode = 'RAIL';
```

## Aggregation

```sql
select count(*) from lineitem;
```

## GroupBy

```sql
select count(*) from lineitem group by l_shipmode;
```

## Join

```sql
select * from lineitem l join orders o on o.o_orderkey = l.l_orderkey;
```

# Compilar

ant build

# Usar

./mrexemplos -h
./mrexemplos -c filter
