/*
** xlsx_vtab.c - 2018-03-02 - jakethaw
**
*************************************************************************
**
** MIT License
** 
** Copyright (c) 2019 jakethaw
** 
** Permission is hereby granted, free of charge, to any person obtaining a copy
** of this software and associated documentation files (the "Software"), to deal
** in the Software without restriction, including without limitation the rights
** to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
** copies of the Software, and to permit persons to whom the Software is
** furnished to do so, subject to the following conditions:
** 
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
** 
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
** 
*************************************************************************
** SQLite3  *************************************************************
*************************************************************************
**
** Dependencies:
**
**. 1. xml_to_json - https://github.com/jakethaw/xml_to_json
**  2. zipfile     - https://www.sqlite.org/src/artifact/c1ca8f52330b4564
**
** The dependencies must be compiled as run-time loadable extensions, and must
** reside in the same location as xlsx_vtab. 
**
** To compile xlsx_vtab  with gcc as a run-time loadable extension:
**
**   UNIX-like : gcc -g -O3 -fPIC -shared xlsx_vtab.c -o xlsx_vtab.so
**   Mac       : gcc -g -O3 -fPIC -dynamiclib xlsx_vtab.c -o xlsx_vtab.dylib
**   Windows   : gcc -g -O3 -shared xlsx_vtab.c -o xlsx_vtab.dll
**
*************************************************************************
**
** xlsx_vtab takes 3 arguments:
**
**  1. Filepath
**. 2. Worksheet name
**  3. Header row number
**
** If a header column name is not available, then the column will be named as
** the Excel column letter.
**
** The vtab represents the xlsx contents at the time the vtab is created.
**
*************************************************************************
**
** Usage examples:
**
** CREATE VIRTUAL TABLE xlsx USING xlsx_vtab('filepath.xlsx', 'Sheet1', 1);
**
** SELECT * FROM xlsx;
**
*************************************************************************
*/


#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdio.h>

// int sqlite3_xml_to_json_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
// int sqlite3_zipfile_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

/* 
** An xlsx_vtab virtual-table object.
*/
typedef struct xlsx_vtab xlsx_vtab;
struct xlsx_vtab {
  sqlite3_vtab base;          /* Base class.  Must be first */
  sqlite3 *vdb;               /* Private in-memory db */
  sqlite3_stmt *pStmt;
  sqlite3_int64 iMaxRowid;    /* The last rowid */
};

/* xlsx_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct xlsx_cursor xlsx_cursor;
struct xlsx_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
};

static int xlsxConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  //printf("xlsxConnect - start\n");
  xlsx_vtab *pNew = 0;

  sqlite3_stmt *pStmt;
  int rc;
  char *sql;
  char *cols;
  sqlite3_int64 worksheet_id;
  sqlite3_int64 iMaxRowid;
  const char *workbook;
  const char *worksheet;
  const char *header_row;
  
  workbook = argv[3];
  worksheet = argv[4];
  header_row = argv[5];
  
  pNew = (xlsx_vtab*)sqlite3_malloc(sizeof(xlsx_vtab));
  if( pNew==0 ) return SQLITE_NOMEM;
  memset(pNew, 0, sizeof(*pNew));
    
  sqlite3_open(":memory:", &pNew->vdb);

  sqlite3_db_config(pNew->vdb, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, (int*)0);
  sqlite3_load_extension(pNew->vdb, "./xml_to_json", "sqlite3_xmltojson_init", 0);
  sqlite3_load_extension(pNew->vdb, "./zipfile", "sqlite3_zipfile_init", 0);
  // sqlite3_xml_to_json_init(pNew->vdb, NULL, NULL);
  // sqlite3_zipfile_init(pNew->vdb, NULL, NULL);
  
  //printf("%s\n", sqlite3_errmsg(pNew->vdb));
  
  sql = "BEGIN;\n"
        "\n"
        "CREATE TABLE workbook(\n"
        "  workbook_id         INTEGER PRIMARY KEY,\n"
        "  path                TEXT,\n"
        "  json                TEXT,\n"
        "  shared_strings_json TEXT\n"
        ");\n"
        "\n"
        "CREATE TABLE worksheet(\n"
        "  worksheet_id  INTEGER PRIMARY KEY,\n"
        "  workbook_id   INT,\n"
        "  name          TEXT,\n"
        "  json          TEXT\n"
        ");\n"
        "CREATE INDEX worksheet_idx ON worksheet(workbook_id);\n"
        "\n"
        "\n"
        "INSERT\n"
        "  INTO workbook(\n"
        "       path\n"
        ")\n"
        "SELECT %s;\n"
        "\n"
        "--\n"
        "-- Extract the workbook and shared string XML content from the Excel spreadsheets\n"
        "--\n"
        "UPDATE workbook\n"
        "   SET (json, shared_strings_json)\n"
        "     = ((SELECT xml_to_json(data) FROM zipfile(workbook.path) WHERE name LIKE '%%workbook.xml'),\n"
        "        (SELECT xml_to_json(data) FROM zipfile(workbook.path) WHERE name LIKE '%%sharedStrings.xml'));\n"
        "\n"
        "--\n"
        "-- Extract the worksheet XML content from the Excel spreadsheets\n"
        "--\n"
        "INSERT\n"
        "  INTO worksheet(\n"
        "       workbook_id,\n"
        "       name,\n"
        "       json\n"
        ")\n"
        "SELECT wb.workbook_id,\n"
        "       j2.value,\n"
        "       xml_to_json(z.data)\n"
        "  FROM workbook           wb\n"
        "  JOIN zipfile(wb.path)   z\n"
        "  JOIN json_tree(wb.json) j\n"
        "  JOIN json_tree(j.value) j2\n"
        " WHERE z.name LIKE '%%/worksheets/%%.xml'\n"
        "   AND j.path = '$.workbook.sheets'\n"
        "   AND j2.key = '@name'\n"
        "   AND Trim(z.name, 'xl/worksheets/sheet.xml')+0 = Trim(j2.path, '$[]')+1\n"
        " ORDER BY\n"
        "       wb.workbook_id,\n"
        "       CAST(Trim(j2.path, '$[]') AS INT);\n"
        " \n"
        "--\n"
        "-- Tables to hold extracted Excel content\n"
        "--\n"
        "CREATE TABLE shared_string(\n"
        "  id          INT,\n"
        "  workbook_id INT,\n"
        "  val         TEXT\n"
        ");\n"
        "\n"
        "CREATE TABLE value(\n"
        "  worksheet_id INT,\n"
        "  col          TEXT,\n"
        "  row          INT,\n"
        "  val          TEXT,\n"
        "  PRIMARY KEY (worksheet_id, row, col)\n"
        ") WITHOUT ROWID;\n"
        "\n"
        "CREATE TABLE worksheet_tree(\n"
        "  worksheet_id INT,\n"
        "  [key],\n"
        "  value,\n"
        "  path\n"
        ");\n"
        "\n"
        "-- Extract shared strings\n"
        "INSERT\n"
        "  INTO shared_string\n"
        "SELECT CAST(substr(j.path, instr(j.path, '[')+1) AS INT) id,\n"
        "       wb.workbook_id,\n"
        "       group_concat(atom, '') val\n"
        "  FROM workbook                       wb\n"
        "  JOIN json_tree(shared_strings_json) j\n"
        " WHERE j.key IN ('t', '#text')\n"
        "GROUP BY \n"
        "      wb.workbook_id,\n"
        "      substr(j.path, 1, instr(j.path, ']'));\n"
        "      \n"
        "CREATE INDEX shared_string_idx ON shared_string(workbook_id, id);\n"
        "      \n"
        "-- Extract worksheet values\n"
        "INSERT\n"
        "  INTO worksheet_tree\n"
        "SELECT ws.worksheet_id,\n"
        "       j.[key],\n"
        "       j.value,\n"
        "       j.path\n"
        "  FROM worksheet          ws\n"
        "  JOIN json_tree(ws.json) j\n"
        " WHERE key LIKE '[0-9]%%'\n"
        "    OR key IN ('v', '@t', '@r');\n"
        "\n"
        "CREATE INDEX worksheet_tree_idx1 ON worksheet_tree (worksheet_id, path, key, value);\n"
        "\n"
        "INSERT\n"
        "  INTO value\n"
        "SELECT ws.worksheet_id,\n"
        "       Trim(r.value, '0123456789') col,\n"
        "       Trim(r.value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') row,\n"
        "       CASE WHEN t.key IS NULL THEN v.value ELSE ss.val END val\n"
        "  FROM worksheet      ws\n"
        "  JOIN worksheet_tree r  ON ws.worksheet_id = r.worksheet_id\n"
        "  JOIN worksheet_tree v  ON ws.worksheet_id = v.worksheet_id\n"
        "                        AND r.path = v.path\n"
        "                        AND v.key = 'v'\n"
        "  LEFT JOIN\n"
        "       worksheet_tree t  ON ws.worksheet_id = t.worksheet_id\n"
        "                        AND r.path = t.path\n"
        "                        AND t.key = '@t'\n"
        "                        AND t.value = 's'\n"
        "  LEFT JOIN\n"
        "       shared_string  ss ON ws.workbook_id = ss.workbook_id\n"
        "                        AND v.value = ss.id\n"
        " WHERE r.key = '@r';\n"
        "\n"
        "COMMIT;";
        
  sql = sqlite3_mprintf(sql, workbook);
  //printf("%s\n", sql);
  sqlite3_exec(pNew->vdb, sql, NULL, NULL, NULL);
  
  //printf("%s\n", sqlite3_errmsg(pNew->vdb));
  
  // get column names (or column letters if blank)
  sql = "WITH i(i, col) AS (\n"
        "SELECT 1, 'A'\n"
        "UNION ALL\n"
        "SELECT i+1,\n"
        "       CASE 0\n"
        "         WHEN i/26 THEN Char(i%%26+64+1)\n"
        "         WHEN i/676 THEN Char(i%%676/26+64, i%%26+64+1)\n"
        "         ELSE Char(i%%17576/676+64, i%%676/26+64+1, i%%26+64+1)\n"
        "       END\n"
        "  FROM i\n"
        " WHERE col <> (SELECT col\n"
        "                 FROM workbook  w\n"
        "                 JOIN worksheet ws USING (workbook_id)\n"
        "                 JOIN value     v  USING (worksheet_id)\n"
        "                WHERE w.path = %s\n"
        "                  AND ws.name = %s\n"
        "                ORDER BY CASE Length(col)\n"
        "                            WHEN 1 THEN '@'\n"
        "                            WHEN 2 THEN '@@'\n"
        "                            ELSE '@@@'\n"
        "                         END DESC, col DESC\n"
        "                LIMIT 1)\n"
        ")\n"
        "SELECT group_concat('[' || IfNull(v.val, i.col) || ']'),\n"
        "               ws.worksheet_id,\n"
        "               (SELECT Max(row) FROM value WHERE worksheet_id = ws.worksheet_id)\n"
        "          FROM i\n"
        "          JOIN workbook  w\n"
        "          JOIN worksheet ws USING (workbook_id)\n"
        "          LEFT JOIN\n"
        "               value     v  ON ws.worksheet_id = v.worksheet_id\n"
        "                           AND i.col = v.col\n"
        "                           AND v.row = %s\n"
        "         WHERE w.path = %s\n"
        "           AND ws.name = %s";
  
  sql = sqlite3_mprintf(sql, workbook, worksheet, header_row, workbook, worksheet);
  //printf("%s\n", sql);
  
  sqlite3_prepare_v2(pNew->vdb, sql, -1, &pStmt, 0);
  //printf("%s\n", sqlite3_errmsg(pNew->vdb));
  sqlite3_free(sql);
  //sqlite3_bind_text(pStmt, 1, workbook, -1, SQLITE_STATIC);
  //sqlite3_bind_text(pStmt, 2, worksheet, -1, SQLITE_STATIC);
  //if( argc == 6 )
  //  sqlite3_bind_text(pStmt, 3, header_row, -1, SQLITE_STATIC);
  sqlite3_step(pStmt);
  cols = (char *)sqlite3_column_text(pStmt,0);
  worksheet_id = sqlite3_column_int64(pStmt,1);
  iMaxRowid = sqlite3_column_int64(pStmt,2);
  
  sql = sqlite3_mprintf("CREATE TABLE x(row,%s /*, workbook HIDDEN, worksheet HIDDEN, header_row HIDDEN*/)", cols);
  //printf("%s\n", sql);
  
  rc = sqlite3_declare_vtab(db, sql);
  sqlite3_free(sql);
  sqlite3_finalize(pStmt);
  
  sql = "SELECT val\n"
        "  FROM value\n"
        " WHERE worksheet_id = ?1\n"
        "   AND row = ?2\n"
        "   AND col = ?3";
        
  sqlite3_prepare_v2(pNew->vdb, sql, -1, &pStmt, 0);
  sqlite3_bind_int64(pStmt, 1, worksheet_id);
  pNew->pStmt = pStmt;
  pNew->iMaxRowid = iMaxRowid;
  
  *ppVtab = (sqlite3_vtab*)pNew;
  
  return rc;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int xlsxCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return xlsxConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** This method is the destructor for xlsx_cursor objects.
*/
static int xlsxDisconnect(sqlite3_vtab *pVtab){
  xlsx_vtab *pTab = (xlsx_vtab*)pVtab;
  sqlite3_finalize(pTab->pStmt);
  sqlite3_close(pTab->vdb);
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/*
** Constructor for a new xlsx_cursor object.
*/
static int xlsxOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  xlsx_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a xlsx_cursor.
*/
static int xlsxClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance a xlsx_cursor to its next row of output.
*/
static int xlsxNext(sqlite3_vtab_cursor *cur){
  xlsx_cursor *pCur = (xlsx_cursor*)cur;
  xlsx_vtab *pTab = (xlsx_vtab*)(pCur->base.pVtab);
  
  pCur->iRowid++;
  //printf("row_id = %d\n", (int)pCur->iRowid);
  sqlite3_bind_int64(pTab->pStmt, 2, pCur->iRowid);
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the xlsx_cursor
** is currently pointing.
*/
static int xlsxColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  xlsx_cursor *pCur = (xlsx_cursor*)cur;
  xlsx_vtab *pTab = (xlsx_vtab*)(pCur->base.pVtab);
  sqlite3_stmt *pStmt = pTab->pStmt;
  char zCol[4];
  
  if( i == 0 ){
    sqlite3_result_int64(ctx, pCur->iRowid);
    return SQLITE_OK;
  }
  i--;
  
  //printf("xlsxColumn = %d\n", i);

  if( i/26 == 0 ){
    zCol[0] = i%26+64+1;
    zCol[1] = 0;
  }else if( i/676 == 0 ){
    zCol[0] = i%676/26+64;
    zCol[1] = i%26+64+1;
    zCol[2] = 0;
  }else{
    zCol[0] = i%17576/676+64;
    zCol[1] = i%676/26+64+1;
    zCol[2] = i%26+64+1;
    zCol[3] = 0;
  }
  
  //printf("%s\n", zCol);
  
  sqlite3_bind_text(pStmt, 3, zCol, -1, SQLITE_STATIC);
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    sqlite3_result_value(ctx, sqlite3_column_value(pStmt, 0));
  }else{
    sqlite3_result_null(ctx);
  }
  sqlite3_reset(pStmt);
  
  return SQLITE_OK;
}

/*
** Return the rowid for the current row. In this implementation, the
** first row returned is assigned rowid value 1, and each subsequent
** row a value 1 more than that of the previous.
*/
static int xlsxRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  xlsx_cursor *pCur = (xlsx_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int xlsxEof(sqlite3_vtab_cursor *cur){
  xlsx_cursor *pCur = (xlsx_cursor*)cur;
  xlsx_vtab *pTab = (xlsx_vtab*)(pCur->base.pVtab);
  return pCur->iRowid > pTab->iMaxRowid;
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int xlsxFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  xlsx_cursor *pCur = (xlsx_cursor*)pVtabCursor;
  pCur->iRowid = 0;
  return xlsxNext(pVtabCursor);
}

/*
** Only a forward full table scan is supported. xBestIndex is mostly a no-op.
*/
static int xlsxBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = 1000000;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** generate_xlsx virtual table.
*/
static sqlite3_module xlsxModule = {
  0,                       /* iVersion */
  xlsxCreate,              /* xCreate */
  xlsxConnect,             /* xConnect */
  xlsxBestIndex,           /* xBestIndex */
  xlsxDisconnect,          /* xDisconnect */
  xlsxDisconnect,          /* xDestroy */
  xlsxOpen,                /* xOpen - open a cursor */
  xlsxClose,               /* xClose - close a cursor */
  xlsxFilter,              /* xFilter - configure scan constraints */
  xlsxNext,                /* xNext - advance a cursor */
  xlsxEof,                 /* xEof - check for end of scan */
  xlsxColumn,              /* xColumn - read data */
  xlsxRowid,               /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_xlsxvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "xlsx_vtab", &xlsxModule, 0);
  return rc;
}
