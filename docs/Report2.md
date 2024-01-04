# DSM23 Project Phase-2

## Assumptions
<ul>
  <li>All the data being written into the .csv file must be int (no commas). </li>
  <li> Summation of all values not greater than range of int.</li>
  <li> Number of columns in resultant table should not exceed size of block after join.</li>
  <li> All float values will be rounded to floor value before being written back into the file.</li>
  <li> Tables with no rows are not loaded. </li>
</ul>

## External Sort
#### Syntax
```sql
SORT <table_name> BY <column_name1, column_name2,..., column_namek> IN
<ASC|DESC, ASC|DESC,..., ASC|DESC>
```
#### Example
```sql
SORT EMPLOYEE BY GENDER, SALARY IN ASC, DESC
```
<img src="./image.jpeg" width="600" title="hover text">

#### Procedure
<ul>
  <li> We get the file name through the command. We fetch this file one block at a time and sort individual block. </li>
  <li> We have all the individual blocks sorted for now. Now we take 9 blocks at a time. </li>
  <li> Do 9-way merging into a "write-back" buffer. As soon as this buffer fills up we write it back and start afresh with a new empty block. </li>
  <li> As we can see this creates 9-block sorted sub file and then we merge such subfiles using the same 9-way merge. </li>
  <li> When we are done, we get the entire file as one sorted file with original number of blocks </li>
</ul>

## JOIN
#### Syntax
```sql
<new_relation_name> <- JOIN <tablename1>, <tablename2> ON
<column1> <bin_op> <column2>
```
#### Example
```sql
Result <- JOIN Students, Courses ON ID == RollNo 
```

#### Procedure
<ul>
  <li> As depicted from the syntax, we store both tables, their respectictive columns and the binary operator in the program. </li>
  <li> Then we sort both the tables on the respective columns in ascending order using External Sort implemented above </li>
  <li> Now according to the binary operator we traverse the 2 tables and store result into the resultant table.</li>
  <li> For all binary operators we treat one table as "stationary" table and the other as "moving" table. </li>
  <li> For the "stationary" table: we just take one row at a time and then match the column with the respective column of the other table.</li>
  <li> For the "moving" table we match all the rows that are matching in sequence (as they are sorted).</li>
  <li> As soon as we get a value that is not matching, we move to the next row in the stationary table and the first valid entry in the moving table. </li>
  <li> This first valid entry move is where the added optimization for equijoin lies.</li>
  <li> For joins with condition "<" and "<=" we are using the same implementation as ">" and ">=" just that we are switching the tables.</li>
</ul>

## Order By
#### Syntax
```sql
<new_table> <- ORDER BY <attribute> ASC|DESC ON <table_name>
```
#### Example
```sql
Result <- ORDER BY Year_Released ASC ON Movies
```

#### Procedure
<ul>
  <li> This is exactly as SORT functionality. </li>
  <li> The difference is the sort result being stored in the table itself, but here the result is being stored in the table name being provided </li>
  <li> Just that the sort is being applied on the table and column received from the command. </li>
  
</ul>

## Group By
#### Syntax
```sql
<new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING
<aggregate(attribute)> <bin_op> <attribute_value> RETURN
<aggregate_func(attribute)>
```
#### Example
```sql
T1 <- GROUP BY Department_ID FROM EMPLOYEE HAVING
AVG(Salary) > 50,000 RETURN MAX(Salary)
```
#### Procedure
<ul>
  <li> Here we capture the table, its grouping column, the aggregate function, and return value. </li>
  <li> Aggregate functions are SUM, MIN, MAX, COUNT and AVG.</li>
  <li> The table is then sorted (External Sort) on the grouping column. </li>
  <li> Now we traverse the sorted table for each value and while traversing we calculate the 4 aggreagate functions simultaneously- SUM, MIN, MAX, COUNT.</li>
  <li> SUM and COUNT are the only ones that need constant updation.</li>
  <li> As soon as we hit a new value, we check the HAVING clause and if it satisfies the HAVING clause we push the RETURN value asked by the command.</li>
  <li> We then startd with a new distinct value. </li>
</ul>

## Learnings

<ul>
  <li> Implementing External Sort where only a part of file can be accessed once. </li>
  <li> Difference between implementing a 2 way merge and a multiple way merge (>=3).</li>
  <li> We learnt how complexity reduces for Join, Order By and Group By if we are working on sorted tables.</li>
  <li> We learnt why join is an expensive operation compared to just Order By and Group By. </li>
  <li> If Join is performed on only equality condition, it can be further optimized. </li>
  <li> Importance of doing operations in an optimized manner so as to improve query performance. </li>
</ul>

