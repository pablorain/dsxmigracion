# Databricks notebook: /Notebooks/J390/fncSev390MovComMonex_{NombreJob}
# ============================================================
# Equivalente DataStage: J390_Movimiento_Compra_Monex
#   - Trf_Traductor  (CTransformerStage): join con Traductor, filtra NOTFOUND
#   - Trf_Valida     (CTransformerStage): fncSev390MovComMonex, fncValidaNuloBlanco,
#                                         MaxEventId acumulador, derivaciones de salida
#   - Hsh_Datos      (CHashedFileStage) : lookups → Event_Activity_Type, Event_Last,
#                                         Event_Payment_Type, Ext_Iden_Hist, BEPB,
#                                         Currency, Bci_Transaction_Type_390
#   - Hsh_EventLast  (CHashedFileStage) : escribe Event_Id actualizado al finalizar
#   - Hsh_Consultas  (CHashedFileStage) : lookup Traductor → filtro NOT NOTFOUND
#
# Tablas Teradata origen (vía JDBC):
#   EDW_Event.EVENT_ACTIVITY_TYPE         WHERE BCI_Host_Cd='46'  → Event_Activity_Type_Cd
#   EDW_Event_Bel.EVENT_LAST             → Event_Id (COALESCE 0)
#   EDW_Event.EVENT_PAYMENT_TYPE          keyed by bci_host_cd
#   EDW_Party.EXTERNAL_IDENTIFICATION_HIST keyed by Ext_Identification_Num, Ext_Identification_Type_Cd=3
#   EDW_Agreement.Bci_Electronic_Payment_Button keyed by Identifier_Id
#   EDW_Finance.Currency                  keyed by Currency_Cd
#   EDW_Event.Bci_Transaction_Type_390    keyed by TRIM(Bci_Host_Cd)
#   EDW_Param_and_Others.Traductor        keyed by Bci_Host_Cd
#
# Archivo origen (ADLS):
#   {PathExtract}{NombreArchExtraccion}.{FechaArchExtraccion}
#   23 columnas pipe-delimitadas: cnv_idn, mmx_srl, mmx_trx, mmx_idc, mmx_dig_vrf,
#   mmx_cta_car, mmx_mto, mmx_fec_pag, mmx_fec_est_cua, mmx_mnd_ori, mmx_tip_cam,
#   mmx_cod_err, mmx_fec_ctb, mmx_idn_ser, mmx_fec_pro_rec, cmx_idn_prd, cmx_cnt,
#   cmx_prc, cmx_fec_cmp, cmx_frm_prd, cmx_num_fol, cmx_fec_vct, cmx_dsc_prd
#
# Salidas:
#   EVENT                       → Teradata EDW_Event_Bel.EVENT      (RuleSeveridad[1] <> 3)
#   BCI_EVENT_PAYMENT_ELECTRONIC → Teradata EDW_Event_Bel            (RuleSeveridad[1] <> 3)
#   No_Procesados               → ADLS {PathOutput}No_Procesados_{NombreJob}_{FechaArchExtraccion}
#   Severidad2                  → ADLS {PathOutput}Sev2_{NombreJob}_{FechaArchExtraccion}
#   Severidad3                  → ADLS {PathOutput}Sev3_{NombreJob}_{FechaArchExtraccion}
#   EVENT_LAST actualizado      → Teradata EDW_Event_Bel.EVENT_LAST
# ============================================================

# NOTA IMPORTANTE: La función BASIC fncSev390MovComMonex no tiene su código fuente
# disponible en el DSX. La implementación siguiente es una aproximación funcional
# basada en los parámetros de entrada y la lógica observada de severidades.
# DEBE VALIDARSE contra el código BASIC original antes de pasar a producción.

# ============================================================
# PARÁMETROS
# ============================================================
dbutils.widgets.text("NombreJob",              "J390_Movimiento_Compra_Monex")
dbutils.widgets.text("FechaProceso",           "")
dbutils.widgets.text("FechaArchExtraccion",    "")
dbutils.widgets.text("NombreArchExtraccion",   "J390_Movimiento_Compra_Monex.TXT")
dbutils.widgets.text("StorageAccountUrl",      "")
dbutils.widgets.text("PathExtract",            "")
dbutils.widgets.text("PathOutput",             "")
dbutils.widgets.text("Teradata_Server",        "")
dbutils.widgets.text("Teradata_User_Grupo1",   "")
dbutils.widgets.text("Teradata_PWD_Grupo1",    "")  # Pasar desde Key Vault via ADF secret
dbutils.widgets.text("EDW_Wrk",               "")
dbutils.widgets.text("EDW_Event_Bel",          "")
dbutils.widgets.text("EDW_Event",             "")
dbutils.widgets.text("EDW_Party",             "")
dbutils.widgets.text("EDW_Agreement",         "")
dbutils.widgets.text("EDW_Finance",           "")
dbutils.widgets.text("EDW_Param_and_Others",  "")

NombreJob             = dbutils.widgets.get("NombreJob")
FechaProceso          = dbutils.widgets.get("FechaProceso")
FechaArchExtraccion   = dbutils.widgets.get("FechaArchExtraccion")
NombreArchExtraccion  = dbutils.widgets.get("NombreArchExtraccion")
StorageAccountUrl     = dbutils.widgets.get("StorageAccountUrl")
PathExtract           = dbutils.widgets.get("PathExtract")
PathOutput            = dbutils.widgets.get("PathOutput")
td_server             = dbutils.widgets.get("Teradata_Server")
td_user               = dbutils.widgets.get("Teradata_User_Grupo1")
td_pwd                = dbutils.widgets.get("Teradata_PWD_Grupo1")
EDW_Wrk               = dbutils.widgets.get("EDW_Wrk")
EDW_Event_Bel         = dbutils.widgets.get("EDW_Event_Bel")
EDW_Event             = dbutils.widgets.get("EDW_Event")
EDW_Party             = dbutils.widgets.get("EDW_Party")
EDW_Agreement         = dbutils.widgets.get("EDW_Agreement")
EDW_Finance           = dbutils.widgets.get("EDW_Finance")
EDW_Param_and_Others  = dbutils.widgets.get("EDW_Param_and_Others")

# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType
)
from pyspark.sql.window import Window

# ============================================================
# TERADATA JDBC
# ============================================================
td_url = f"jdbc:teradata://{td_server}/CHARSET=UTF8,TMODE=TERA,CLIENT_CHARSET=UTF8"
td_opts = {
    "driver":   "com.teradata.jdbc.TeraDriver",
    "url":      td_url,
    "user":     td_user,
    "password": td_pwd,
}

def td_read(query):
    return (spark.read.format("jdbc")
            .options(**td_opts)
            .option("query", query)
            .load())

# ============================================================
# STEP 1: LEER ARCHIVO Mov_Com_Monex DESDE ADLS
# Equiv: CSeqFileStage Seq_Mov_Com_Monex → Hsh_Consultas
# ============================================================
src_cols = [
    "cnv_idn", "mmx_srl", "mmx_trx", "mmx_idc", "mmx_dig_vrf",
    "mmx_cta_car", "mmx_mto", "mmx_fec_pag", "mmx_fec_est_cua",
    "mmx_mnd_ori", "mmx_tip_cam", "mmx_cod_err", "mmx_fec_ctb",
    "mmx_idn_ser", "mmx_fec_pro_rec", "cmx_idn_prd", "cmx_cnt",
    "cmx_prc", "cmx_fec_cmp", "cmx_frm_prd", "cmx_num_fol",
    "cmx_fec_vct", "cmx_dsc_prd"
]

src_path = f"abfss://{PathExtract.lstrip('/')}@{StorageAccountUrl.replace('https://','')}/{NombreArchExtraccion}.{FechaArchExtraccion}"

df_src = (spark.read
          .format("csv")
          .option("delimiter", "|")
          .option("header", "false")
          .option("inferSchema", "false")
          .load(src_path)
          .toDF(*src_cols)
          .withColumn("RegId", F.monotonically_increasing_id()))

# ============================================================
# STEP 2: LEER TABLAS LOOKUP DESDE TERADATA
# Equiv: CCustomStage Datos_EDW → Hsh_Datos
#        V0S10P11 → Hsh_Consultas → Trf_Traductor
# ============================================================

# Singleton: EVENT_ACTIVITY_TYPE WHERE BCI_Host_Cd='46'
df_eat = td_read(
    f"SELECT 1 AS Llave, Event_Activity_Type_Cd "
    f"FROM {EDW_Event}.EVENT_ACTIVITY_TYPE WHERE BCI_Host_Cd = '46'"
)

# Singleton: EVENT_LAST (max Event_Id acumulado)
df_el = td_read(
    f"SELECT 1 AS Llave, COALESCE(Event_Id, 0) AS Event_Id "
    f"FROM {EDW_Event_Bel}.EVENT_LAST"
)

# EVENT_PAYMENT_TYPE (key: bci_host_cd → join mmx_trx)
df_ept = td_read(
    f"SELECT bci_host_cd, event_payment_type_cd "
    f"FROM {EDW_Event}.EVENT_PAYMENT_TYPE"
)

# EXTERNAL_IDENTIFICATION_HIST (key: Ext_Identification_Num → join mmx_idc)
df_eih = td_read(
    f"SELECT Ext_Identification_Num, Party_Id "
    f"FROM {EDW_Party}.EXTERNAL_IDENTIFICATION_HIST "
    f"WHERE Ext_Identification_Type_Cd = 3 AND Ext_Identification_End_Dt IS NULL"
)

# Bci_Electronic_Payment_Button (key: Identifier_Id → join mmx_srl)
df_bepb = td_read(
    f"SELECT TRIM(Identifier_Id) AS Identifier_Id "
    f"FROM {EDW_Agreement}.Bci_Electronic_Payment_Button"
)

# Currency (key: Currency_Cd → join mmx_mnd_ori)
df_cur = td_read(
    f"SELECT Currency_Cd FROM {EDW_Finance}.Currency"
)

# Bci_Transaction_Type_390 (key: TRIM(Bci_Host_Cd) → join mmx_trx)
df_btt = td_read(
    f"SELECT TRIM(Bci_Host_Cd) AS Bci_Host_Cd, Trx_Code_Cd "
    f"FROM {EDW_Event}.Bci_Transaction_Type_390"
)

# Traductor (key: Bci_Host_Cd → join mmx_trx, filter NOT NOTFOUND)
df_trad = td_read(
    f"SELECT Bci_Host_Cd, Homologado "
    f"FROM {EDW_Param_and_Others}.Traductor"
)

# ============================================================
# STEP 3: TRF_TRADUCTOR
# Join Mov_Com_Monex con Traductor (LEFT JOIN por mmx_trx)
# - NOT NOTFOUND → Lnk_Trad_Mov_Com_Monex (pasa a Trf_Valida)
# - NOTFOUND     → Lnk_No_Procesados (archivo rechazados)
# Equiv: CTransformerStage Trf_Traductor
# ============================================================
df_joined_trad = df_src.join(
    df_trad.alias("trad"),
    F.col("mmx_trx") == F.col("trad.Bci_Host_Cd"),
    "left"
)

# Registros sin match → No_Procesados
df_no_proc = df_joined_trad.filter(F.col("trad.Bci_Host_Cd").isNull())

# Registros con match → flujo principal (Lnk_Trad_Mov_Com_Monex = passthrough columnas fuente)
df_trad_ok = df_joined_trad.filter(F.col("trad.Bci_Host_Cd").isNotNull())

# ============================================================
# STEP 4: LOOKUPS SOBRE EL DATASET PRINCIPAL
# Equiv: Hsh_Datos lookups en Trf_Valida
# ============================================================

# Singleton: join constante Llave=1
base_event_id = df_el.first()["Event_Id"] if df_el.count() > 0 else 0
event_activity_type_cd = df_eat.first()["Event_Activity_Type_Cd"] if df_eat.count() > 0 else None

# Join EVENT_PAYMENT_TYPE (mmx_trx → bci_host_cd)
df_main = df_trad_ok.join(
    df_ept.alias("ept"),
    F.col("mmx_trx") == F.col("ept.bci_host_cd"),
    "left"
)

# Join EXTERNAL_IDENTIFICATION_HIST (mmx_idc → Ext_Identification_Num)
df_main = df_main.join(
    df_eih.alias("eih"),
    F.col("mmx_idc") == F.col("eih.Ext_Identification_Num"),
    "left"
)

# Join Bci_Electronic_Payment_Button (mmx_srl → Identifier_Id)
# NOTA: validar columna de join contra código original DataStage
df_main = df_main.join(
    df_bepb.alias("bepb"),
    F.col("mmx_srl") == F.col("bepb.Identifier_Id"),
    "left"
)

# Join Currency (mmx_mnd_ori → Currency_Cd)
df_main = df_main.join(
    df_cur.alias("cur"),
    F.col("mmx_mnd_ori") == F.col("cur.Currency_Cd"),
    "left"
)

# Join Bci_Transaction_Type_390 (mmx_trx → Bci_Host_Cd)
df_main = df_main.join(
    df_btt.alias("btt"),
    F.col("mmx_trx") == F.col("btt.Bci_Host_Cd"),
    "left"
)

# Agregar constantes de lookups singleton
df_main = df_main.withColumn("Event_Activity_Type_Cd", F.lit(event_activity_type_cd))

# ============================================================
# STEP 5: FUNCIONES BÁSICAS (equivalentes BASIC → Python UDF)
# ============================================================

def fnc_valida_nulo_blanco(val):
    """
    Equiv DataStage BASIC: fncValidaNuloBlanco(val)
    Retorna True si val es nulo o solo blancos.
    """
    if val is None:
        return True
    return val.strip() == ""

fnc_valida_nulo_blanco_udf = F.udf(fnc_valida_nulo_blanco)


def fnc_sev_390(identifier_id, party_id, currency_cd, trx_code_cd):
    """
    Equiv DataStage BASIC: fncSev390MovComMonex(Identifier_Id, Party_Id, Currency_Cd, Trx_Code_Cd)

    NOTA: Esta es una implementación aproximada. El código BASIC original no está
    disponible en el DSX. La función original usa RuleSeveridad[2,1] (posición 2,
    longitud 1, base-1) para determinar:
      - '3': Error crítico → solo archivo rechazo, NO escribe en EDW
      - '2': Advertencia   → escribe en EDW + archivo rechazo
      - '1': OK            → solo escribe en EDW

    Regla aproximada implementada:
      - Si Identifier_Id Y Party_Id son NULL → severidad 3
      - Si solo uno de los lookups críticos es NULL → severidad 2
      - Si todos los lookups encontrados → severidad 1

    DEBE VALIDARSE contra el fuente BASIC original antes de producción.
    """
    missing_critical = (identifier_id is None or party_id is None)
    missing_secondary = (currency_cd is None or trx_code_cd is None)

    if missing_critical and missing_secondary:
        severity = "3"
    elif missing_critical or missing_secondary:
        severity = "2"
    else:
        severity = "1"

    # Retorna string de formato "X{severity}..." compatible con RuleSeveridad[2,1]
    return f"R{severity}00"

fnc_sev_390_udf = F.udf(fnc_sev_390, StringType())

# ============================================================
# STEP 6: APLICAR TRAVA DE SEVERIDADES (Trf_Valida stage variables)
# Stage vars: RegId=@INROWNUM, RuleSeveridad=fncSev390(...), MaxEventId=acumulador
# ============================================================
df_sev = df_main.withColumn(
    "RuleSeveridad",
    fnc_sev_390_udf(
        F.col("bepb.Identifier_Id"),
        F.col("eih.Party_Id"),
        F.col("cur.Currency_Cd"),
        F.col("btt.Trx_Code_Cd")
    )
)

# Posición 2 (1-based) = índice 1 (0-based)
df_sev = df_sev.withColumn("Sev_Code", F.substring(F.col("RuleSeveridad"), 2, 1))

# MaxEventId: contador incremental sobre filas donde Sev_Code <> '3'
# Equiv: IF RuleSeveridad[2,1]<>3 THEN MaxEventId+1 ELSE MaxEventId
w_order = Window.orderBy("RegId")
df_sev = df_sev.withColumn(
    "MaxEventId",
    F.when(
        F.col("Sev_Code") != "3",
        F.count(F.when(F.col("Sev_Code") != "3", F.lit(1))).over(
            Window.orderBy("RegId").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    ).otherwise(F.lit(0))
)

# Event_Id calculado: Lnk_Event_Last_Hsh.Event_Id + MaxEventId
df_sev = df_sev.withColumn("Computed_Event_Id", F.lit(base_event_id) + F.col("MaxEventId"))

# ============================================================
# STEP 7: FUNCIÓN fncValidaNuloBlanco APLICADA EN DERIVACIONES
# Conv_Bca_Electr_Code:
#   IF fncValidaNuloBlanco(mmx_idn_ser) THEN NULL
#   ELSE 'BE':RIGHT('0000000000':TRIM(mmx_idn_ser),10)
# ============================================================
df_sev = df_sev.withColumn(
    "Conv_Bca_Electr_Code",
    F.when(
        F.col("mmx_idn_ser").isNull() | (F.trim(F.col("mmx_idn_ser")) == ""),
        F.lit(None).cast(StringType())
    ).otherwise(
        F.concat(
            F.lit("BE"),
            F.substring(
                F.concat(F.lit("0000000000"), F.trim(F.col("mmx_idn_ser"))),
                -10, 10
            )
        )
    )
)

# Charge_Account:
#   IF mmx_cta_car[4,1]=9 THEN mmx_cta_car[1,3]:'0':mmx_cta_car[5,8]
#   ELSE mmx_cta_car
# BASIC substring [pos,len] → Python [pos-1:pos-1+len]
df_sev = df_sev.withColumn(
    "Charge_Account",
    F.when(
        F.substring(F.col("mmx_cta_car"), 4, 1) == "9",
        F.concat(
            F.substring(F.col("mmx_cta_car"), 1, 3),
            F.lit("0"),
            F.substring(F.col("mmx_cta_car"), 5, 8)
        )
    ).otherwise(F.col("mmx_cta_car"))
)

# ============================================================
# STEP 8: SEPARAR FLUJOS DE SALIDA
# ============================================================
df_sev3   = df_sev.filter(F.col("Sev_Code") == "3")
df_sev2   = df_sev.filter(F.col("Sev_Code") == "2")
df_sev_ok = df_sev.filter(F.col("Sev_Code") != "3")   # Sev 1 + Sev 2

# ============================================================
# STEP 9: CONSTRUIR Lnk_Event (44 columnas → EVENT table)
# Equiv: CTrxOutput V0S1P3 (Constraint: RuleSeveridad[2,1]<>3)
# ============================================================
df_event = df_sev_ok.select(
    F.col("RegId").alias("Reg_Id"),
    F.col("Computed_Event_Id").alias("Event_Id"),
    F.col("Event_Activity_Type_Cd"),
    F.lit(0).alias("Event_Status_Cd"),
    F.lit(0).alias("Event_Status_Reason_Cd"),
    F.lit(0).alias("Event_Reason_Cd"),
    F.lit(None).cast(StringType()).alias("BCI_JNL_COD_TRN"),
    F.substring(F.col("mmx_fec_pag"), 1, 10).alias("Event_Start_Dt"),
    F.substring(F.col("mmx_fec_pag"), 12, 8).alias("Event_Start_Tm"),
    F.substring(F.col("mmx_fec_pag"), 1, 10).alias("Event_End_Dt"),
    F.substring(F.col("mmx_fec_pag"), 12, 8).alias("Event_End_Tm"),
    F.lit(None).cast(StringType()).alias("Account_Ind"),
    F.lit(None).cast(StringType()).alias("Financial_Ind"),
    F.lit(None).cast(StringType()).alias("Contact_Ind"),
    F.lit(None).cast(StringType()).alias("Incident_Ind"),
    F.lit(None).cast(StringType()).alias("Account_Group_Ind"),
    F.lit(None).cast(StringType()).alias("Consent_Ind"),
    F.lit(None).cast(StringType()).alias("Drug_Use_Review_Ind"),
    F.lit(None).cast(StringType()).alias("Financial_Account_Event_Ind"),
    F.lit(None).cast(StringType()).alias("Internal_Investment_Event_Ind"),
    F.lit(None).cast(StringType()).alias("External_Investment_Event_Ind"),
    F.col("Sev_Code").alias("Quality_Type_Cd"),
    F.lit(None).cast(StringType()).alias("BCI_AMT1"),
    F.lit(None).cast(StringType()).alias("BCI_AMT2"),
    F.lit(None).cast(StringType()).alias("BCI_AMT3"),
    F.lit(None).cast(StringType()).alias("BCI_AMT4"),
    F.lit(None).cast(StringType()).alias("BCI_AMT5"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Cod_Sis_Trn"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Cod_Amb"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Cod_Mod"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Opr"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Fec_Ctb"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Vcb_Ope"),
    F.lit(None).cast(StringType()).alias("BCI_Jnl_Mnm"),
    F.lit(None).cast(StringType()).alias("BCI_Doc_Num"),
    F.lit(None).cast(StringType()).alias("BCI_Evt_Source"),
    F.lit(None).cast(StringType()).alias("Acct_Num_Relates"),
    F.lit(None).cast(StringType()).alias("Acct_Modifier_Num_Relates"),
    F.lit(None).cast(StringType()).alias("Invoice_Payment_Acct_Ind"),
    F.lit(0).alias("Account_Event_Type_Cd"),
    F.lit(None).cast(StringType()).alias("BCI_Process_Date"),
    F.lit(None).cast(StringType()).alias("BCI_Tx_Data"),
    F.lit(None).cast(StringType()).alias("BCI_Acct_Modifier_Num_Related"),
    F.lit(None).cast(StringType()).alias("BCI_Acct_Num_Related")
)

# ============================================================
# STEP 10: CONSTRUIR Lnk_Bci_Event_Payment_Electronic (29 columnas)
# Equiv: CTrxOutput V0S1P4 (Constraint: RuleSeveridad[2,1]<>3)
# ============================================================
df_bepe = df_sev_ok.select(
    F.col("RegId").alias("Reg_Id"),
    F.col("Computed_Event_Id").alias("Event_Id"),
    F.lit(FechaProceso).alias("Process_Dt"),
    F.col("cmx_idn_prd").alias("Second_Identifier"),
    F.col("mmx_srl").alias("Serial_Num"),
    F.lit(None).cast(StringType()).alias("Tax_Code_Cd"),
    F.col("cnv_idn").alias("Conv_Code_Cd"),
    F.col("Conv_Bca_Electr_Code"),
    F.col("ept.event_payment_type_cd").alias("Event_Payment_Type_Cd"),
    F.col("Charge_Account"),
    F.col("eih.Party_Id"),
    F.col("cur.Currency_Cd"),
    F.col("mmx_mto").alias("Payment_Amount"),
    F.col("cmx_frm_prd").alias("Form_Num_Cd"),
    F.col("cmx_prc").alias("Price_Detail"),
    F.col("cmx_cnt").alias("Quantity_Detail"),
    F.substring(F.col("mmx_fec_pag"), 1, 10).alias("Reception_Dt"),
    F.substring(F.col("mmx_fec_ctb"), 1, 10).alias("Accounting_Dt"),
    F.col("btt.Trx_Code_Cd"),
    F.lit(0).alias("Facturador_Code"),
    F.lit(0).alias("Services_Code"),
    F.col("Sev_Code").alias("Quality_Type_Cd"),
    F.col("mmx_cod_err").alias("error_code"),
    F.substring(F.col("mmx_fec_est_cua"), 1, 10).alias("Company_Accounting_Dt"),
    F.col("mmx_tip_cam").alias("Exchange_Rate"),
    F.substring(F.col("mmx_fec_pro_rec"), 1, 10).alias("Company_Process_Dt"),
    F.col("cmx_num_fol").alias("Debt_Number"),
    F.substring(F.col("cmx_fec_vct"), 1, 10).alias("Debt_Maturity_Dt"),
    F.col("cmx_dsc_prd").alias("Product_Desc")
)

# ============================================================
# STEP 11: CONSTRUIR ARCHIVOS RECHAZO (Sev2 + Sev3 + No_Procesados)
# Equiv: CTrxOutput Lnk_a_Severidad2, Lnk_a_Severidad3, Lnk_No_Procesados
# Columnas de rechazo: passthrough de Lnk_Trad_Mov_Com_Monex + Error_Regla
# ============================================================
reject_cols = [
    "cnv_idn","mmx_srl","mmx_trx","mmx_idc","mmx_dig_vrf",
    "mmx_cta_car","mmx_mto","mmx_fec_pag","mmx_fec_est_cua",
    "mmx_mnd_ori","mmx_tip_cam","mmx_cod_err","mmx_fec_ctb",
    "mmx_idn_ser","mmx_fec_pro_rec","cmx_idn_prd","cmx_cnt",
    "cmx_prc","cmx_fec_cmp","cmx_frm_prd","cmx_num_fol",
    "cmx_fec_vct","cmx_dsc_prd"
]

# Error_Regla: FIELD(RuleSeveridad, "\", 1) → primer campo separado por "\"
df_sev2_rej = df_sev2.select(
    *[F.col(c) for c in reject_cols if c in df_sev2.columns],
    F.split(F.col("RuleSeveridad"), "\\\\").getItem(0).alias("Error_Regla")
)

df_sev3_rej = df_sev3.select(
    *[F.col(c) for c in reject_cols if c in df_sev3.columns],
    F.split(F.col("RuleSeveridad"), "\\\\").getItem(0).alias("Error_Regla")
)

df_no_proc_out = df_no_proc.select(
    *[F.col(c) for c in reject_cols if c in df_no_proc.columns]
)

# ============================================================
# STEP 12: ESCRIBIR EN TERADATA (EVENT y BCI_EVENT_PAYMENT_ELECTRONIC)
# Equiv: CCustomStage TeradataConnector (insert mode)
# ============================================================
td_write_opts = {
    **td_opts,
    "batchsize": "10000",
}

(df_event.write
 .format("jdbc")
 .options(**td_write_opts)
 .option("dbtable", f"{EDW_Event_Bel}.EVENT")
 .mode("append")
 .save())

(df_bepe.write
 .format("jdbc")
 .options(**td_write_opts)
 .option("dbtable", f"{EDW_Event_Bel}.BCI_EVENT_PAYMENT_ELECTRONIC")
 .mode("append")
 .save())

# ============================================================
# STEP 13: ACTUALIZAR EVENT_LAST (Equiv: Hsh_EventLast write-back)
# Lnk_a_hEventLast: Llave=1, Event_Id=Lnk_Event_Last_Hsh.Event_Id+MaxEventId
# ============================================================
max_event_count = df_sev_ok.agg(F.max("MaxEventId")).first()[0] or 0
new_event_id = base_event_id + max_event_count

spark.createDataFrame(
    [(1, new_event_id)],
    schema=["Llave", "Event_Id"]
).write.format("jdbc").options(**td_write_opts) \
 .option("dbtable", f"{EDW_Event_Bel}.EVENT_LAST") \
 .mode("overwrite") \
 .save()

# ============================================================
# STEP 14: ESCRIBIR ARCHIVOS RECHAZO EN ADLS
# ============================================================
def adls_path(sub):
    base = StorageAccountUrl.rstrip("/") + "/" + PathOutput.strip("/")
    return f"{base}/{sub}"

df_sev2_rej.coalesce(1).write.format("csv") \
    .option("delimiter", "|") \
    .option("header", "false") \
    .mode("overwrite") \
    .save(adls_path(f"Sev2_{NombreJob}_{FechaArchExtraccion}"))

df_sev3_rej.coalesce(1).write.format("csv") \
    .option("delimiter", "|") \
    .option("header", "false") \
    .mode("overwrite") \
    .save(adls_path(f"Sev3_{NombreJob}_{FechaArchExtraccion}"))

df_no_proc_out.coalesce(1).write.format("csv") \
    .option("delimiter", "|") \
    .option("header", "false") \
    .mode("overwrite") \
    .save(adls_path(f"No_Procesados_{NombreJob}_{FechaArchExtraccion}"))

# ============================================================
# STEP 15: RESUMEN DE EJECUCIÓN
# ============================================================
print(f"[{NombreJob}] Fecha: {FechaProceso} | Arch: {NombreArchExtraccion}.{FechaArchExtraccion}")
print(f"  Total registros fuente        : {df_src.count()}")
print(f"  No procesados (sin Traductor) : {df_no_proc.count()}")
print(f"  Severidad 3 (rechazados EDW)  : {df_sev3.count()}")
print(f"  Severidad 2 (aviso)           : {df_sev2.count()}")
print(f"  Severidad 1 (OK)              : {df_sev_ok.filter(F.col('Sev_Code')=='1').count()}")
print(f"  EVENT escritos                : {df_event.count()}")
print(f"  BCI_EVENT_PAYMENT_ELECTRONIC  : {df_bepe.count()}")
print(f"  EVENT_LAST base_id            : {base_event_id}")
print(f"  EVENT_LAST nuevo_max          : {new_event_id}")
