# DATA SYSTEMS

---
## Assumptions
---
<ul>
<li>There will be only integers in the matrix.
<li>One block can handle 250 elements in it, and since the matrices are always square
matrices, the highest number of rows in one block is 15.
<li>There can be only 225 elements in the block at most and further bigger inputs have
to be broken up into subsequent 15x15 size matrices.
</ul>
<br> 

## Page Design
<ul>
  <li> Since there can se only 15x15 elements in one block, a matrix with higher dimensions will be broken up as shown (32x32 matrix)
    <a target="_blank" href="https://imageupload.io/wi0JiqpAVoHNbeg"><img  src="https://imageupload.io/ib/PogjJEhnYMz36xb_1693936652.jpg" alt="Untitled Diagram.jpg"/></a>
</ul>

## Commands
---
```
1. LOAD MATRIX
```
<ul>
  <li> Logic:<br> 
    - First reads the first row of csv file to understand the length(n) of the matrix (because square matrix).<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    - Then writes it into the blocks that were calculated in the second step.<br> 
</ul>
<ul>
  <li> Block Accesses:<br> 
    - The number of blocks write = The number of blocks the matrix is divided into = (n/15)^2.<br> 
    - We are reading csv and not the blocks in LOAD.<br> 
</ul>
<ul>
  <li> Error Handling:<br> 
    - The usual error checks are made - Semantix, Syntactic.<br> 
    - If no errors are detected by the above then it checks whether such matrix exists or not. <br> 
    - Executor runs if everything is fine from the above checks.<br> 
</ul>




```
2. PRINT MATRIX
```
<ul>
  <li> Logic:<br> 
    - Checks into the Pages for the matrix and prints it line by line..<br> 
    - The matrix is printed line by line and it does the block reads as and when required .<br> 
</ul>
<ul>
  <li> Block Accesses:<br> 
    - If there is a 16x16 matrix, it will do 32 block reads.<br> 
</ul>
<ul>
  <li> Error Handling:<br> 
    - The usual error checks are made - Semantix, Syntactic.<br> 
    - If no errors are detected by the above then it checks whether such matrix exists or not. <br> 
    - Executor runs if everything is fine from the above checks.<br>
</ul>



```
3. TRANSPOSE MATRIX
```
<ul>
  <li> Logic:<br> 
    - If the matrix is broken up as shown in the assumptions, then the matrix is transposed by using either 1 block read min, or 2 block reads at max<br> 
    - The logic is shown in the Block Accesses part.
</ul>
<ul>
  <li> Block Accesses:<br> 
    - The diagonal matrix will be transposed as a normal matrix is done.<br> 
    - The other matrices are transposed using 2 block accesses and swapping them element by element. <br> 
</ul>
<ul>
  <li> Error Handling:<br> 
    - The usual error checks are made - Semantix, Syntactic.<br> 
    - If no errors are detected by the above then it checks whether such matrix exists or not. <br> 
    - Executor runs if everything is fine from the above checks.<br>
</ul>


```
4. EXPORT MATRIX
```
<ul>
  <li> Logic:<br> 
    - If the same file name exists then deletes the old one and creates a new one.<br> 
    - If not, then creates a new file.<br> 
</ul>
<ul>
  <li> Error Handling:<br> 
    - First reads the first row of csv file to understand the length of the matrix (because square matrix).<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    - Then writes it into the blocks that were calculated in the second step.<br> 
</ul>

```
5. RENAME MATRIX
```
<ul>
  <li> Logic:<br> 
    - Searches the block, makes a new block and reads the data from old block and writes it into new blocks.<br> 
</ul>
<ul>
  <li> Block Accesses:<br> 
    - First reads the first row of csv file to understand the length of the matrix (because square matrix).<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    - Then writes it into the blocks that were calculated in the second step.<br> 
</ul>
<ul>
  <li> Error Handling:<br> 
    - The usual error checks are made - Semantix, Syntactic.<br> 
    - If no errors are detected by the above then it checks whether such matrix exists or not. <br> 
    - Executor runs if everything is fine from the above checks.<br> 
</ul>

```
6. CHECKSYMMETRY
```
<ul>
  <li> Logic:<br> 
    - If same i and j, then one block check and else 2 block checks<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    -  An optimization done in this function is that if the symmetry check fails at any point, no further pages are loaded from there on and the code exits with false as output. So we do not need to load and check every page pertaining to the matrix.
</ul>
<ul>
  <li> Error Handling:<br> 
    - First reads the first row of csv file to understand the length of the matrix (because square matrix).<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    - Then writes it into the blocks that were calculated in the second step.<br> 
</ul>

```
7. COMPUTE
```
<ul>
  <li> Logic:<br> 
    - Here, we save the result as <Table_name>_RESULT and save it as another matrix. Basically, this result will be treated as another matrix which was loaded. In order to access this matrix, you will follow the same convention as we follow to print any other matrix, using the PRINT command. <br>
</ul>

<ul>
  <li> Error Handling:<br> 
    - First reads the first row of csv file to understand the length of the matrix (because square matrix).<br> 
    - Likewise it calculates how many blocks the matrix will be broken into.<br> 
    - Then writes it into the blocks that were calculated in the second step.<br> 
</ul>

---
## LEARNINGS
---

This phase got us directly and closely working with the pages. The main challenges were operating on big matrices (more than 1 page), where in most cases the operation was dependent on values which existed in different pages. The constraint was that we cannot load more than 2 pages at a time. We learnt how to obtain the page and store it in a volatile data structure, operate on them efficiently such that there are less page loads, and then flush the data structure while saving the data by overwriting the page or creating a new one. 

We also learnt the code template better this time as we had to work closely with it and understand its complete flow and nitty gritties. The major learning came in the form of the understanding that memory is limited and efficient use of memory is an important task. We also had to think creatively in order to minimize page accesses and perform the mentioned operations efficiently.


---

