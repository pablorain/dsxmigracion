# ADF — Migración J990_Jen (desde DataStage)

Estructura estándar de repositorio Git de Azure Data Factory. Importar conectando la Data Factory a este repo, o copiar cada carpeta a la raíz del repo ADF existente.

## Contenido

```
adf/
├── linkedService/
│   ├── LS_Sybase_Journal_ODBC.json   # Origen Sybase ASE vía ODBC (requiere SHIR)
│   ├── LS_KeyVault.json              # Secreto de password Sybase
│   └── LS_ADLS_Landing.json          # Zona de aterrizaje (archivos planos = $PathTemp)
├── dataset/
│   ├── DS_Sybase_Jen.json            # journal.dbo.jen (26 columnas, esquema mapeado)
│   └── DS_Jen_File.json              # Archivo plano pipe-delimited, sin cabecera, CP1252
└── pipeline/
    ├── PL_J990_Jen_Extraccion_IB.json     # IB: jen_ope_evt LIKE 'IB%', fecha-7 días
    ├── PL_J990_Jen_Extraccion_No_IB.json  # No_IB: NOT LIKE 'IB%', mismo día
    └── PL_S990_Jen_Seq_Extraccion.json    # Orquestador (entry point): IB -> (OK) -> No_IB
```

## Antes de publicar — reemplazar placeholders

- `LS_KeyVault.json` → `baseUrl` del Key Vault real.
- `LS_ADLS_Landing.json` → `url` de la cuenta ADLS Gen2 real.
- `LS_Sybase_Journal_ODBC.json` → referencia `SHIR-OnPrem`: crear/asignar el Self-hosted Integration Runtime con el driver ODBC de Sybase.
- Key Vault → crear el secreto `Sybase-Journal-PWD` (la password `{iisenc}` del DSX no es reutilizable).
- `DS_Jen_File.json` → ajustar `Container`/`fileSystem` (default `landing`) a tu contenedor.

## Equivalencias DataStage → ADF

| DataStage | ADF |
|---|---|
| Job Sequence S990 (BASIC, checkpoint) | Pipeline orquestador + Execute Pipeline + retry por actividad |
| Server Job (ODBC → Transformer → SeqFile) | Pipeline con 1 Copy activity |
| CODBCStage (Sybase) | OdbcSource + LS Odbc sobre SHIR |
| Transformer `EREPLACE(ICONV(col,"MCP"),"|"," ")` | `REPLACE(col,'|',' ')` en el SELECT del source |
| CSeqFileStage (pipe, sin header) | DelimitedTextSink (`\|`, firstRowAsHeader=false, CP1252) |
| Parámetros `$Param` | Parámetros de pipeline |

## Pendientes / validación (ver gaps en J990_Jen_migration_plan.json)

1. **ICONV('MCP')**: el `REPLACE('|',' ')` cubre la protección del delimitador, pero NO la
   eliminación de caracteres no imprimibles. Si el journal contiene bytes de control,
   añadir limpieza adicional (regex en Mapping Data Flow) y comparar byte a byte contra el
   archivo legacy.
2. **Formato de fechas/monto**: `jen_fec_evt_neg`, `jen_fec_ctb_evt` (TIMESTAMP) y `jen_mto`
   (NUMERIC 18) se serializan según el sink; validar que el formato coincide con lo que espera
   la carga downstream.
3. **Carga downstream (MultiLoad/MLoad a Teradata)**: NO está en este DSX (sólo extracción).
   Solicitar esos jobs para completar el pipeline end-to-end.
4. **Jobs `CopyOf*` / `CopyOfCopyOf*`**: copias de desarrollo, excluidas (no se migran).
