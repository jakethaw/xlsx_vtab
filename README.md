# xlsx_vtab
SQLite3 xlsx virtual table

Dependencies:

1. xml_to_json - https://github.com/jakethaw/xml_to_json
2. zipfile     - https://www.sqlite.org/src/artifact/c1ca8f52330b4564

The dependencies must be compiled as run-time loadable extensions, and must
reside in the same location as xlsx_vtab. 

To compile xlsx_vtab  with gcc as a run-time loadable extension:

```bash
UNIX-like : gcc -g -O3 -fPIC -shared xlsx_vtab.c -o xlsx_vtab.so
Mac       : gcc -g -O3 -fPIC -dynamiclib xlsx_vtab.c -o xlsx_vtab.dylib
Windows   : gcc -g -O3 -shared xlsx_vtab.c -o xlsx_vtab.dll
```

xlsx_vtab takes 3 arguments:

1. Filepath
2. Worksheet name
3. Header row number

If a header column name is not available, then the column will be named as
the Excel column letter.

The vtab represents the xlsx contents at the time the vtab is created.

## Usage examples:

```sql
CREATE VIRTUAL TABLE xlsx USING xlsx_vtab('filepath.xlsx', 'Sheet1', 1);

SELECT * FROM xlsx;
```
