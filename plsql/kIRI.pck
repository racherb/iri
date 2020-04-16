CREATE OR REPLACE PACKAGE kIRI_v4 IS

   TYPE tString IS TABLE OF VARCHAR2(4000);

   /*##############################################################################
   %desc Launcher of the IRI process of automatic image consolidation.
   %parm fpdAnalysisDay day to be analyzed, 
                       if NULL is reported, the system recovers the 
                       value of the CNF parameter "delayConsolidationIRI" and subtract it up to date
   %autr RHE / Raciel Hernandez
   %vers 1A / 31mar'09 / RHE
   */
   PROCEDURE pRunIRI(fpdAnalysisDay IN DATE DEFAULT NULL);
   FUNCTION pListIRI RETURN BOOLEAN;
   FUNCTION fbApplyRNC RETURN BOOLEAN;
   FUNCTION fbApplyIRC RETURN BOOLEAN;
   FUNCTION fbListIRC RETURN BOOLEAN;

END kIRI_v4;
/
CREATE OR REPLACE PACKAGE BODY kIRI_v4 IS
   /**
   Consolidacion automatica de imagenes en base al modelo IRI
   Aplicacion de las matrices de rutas a la verificacion de imagenes en AVI.
   Se basa en el modelo de informacion de ruta inicial (IRI) y consolida para rutas entre N porticos.
   */

   gvcPkg CONSTANT VARCHAR2(31) := 'kIRI_v4_QA.';

   geDBIsDown EXCEPTION;
   PRAGMA EXCEPTION_INIT(geDBIsDown, -12541);

   geDBIsGoingDown EXCEPTION;
   PRAGMA EXCEPTION_INIT(geDBIsGoingDown, -01089);

   -- #############################################################################
   /**
   Colector de basura IRI
   Elimina la tabla especificada en pvTabla
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvTabla Nombre de la tabla que se desea eliminar.
   */
   PROCEDURE pDropPvd(pvTabla VARCHAR2) IS
      lvDropPVD VARCHAR2(1) := 'F';
   BEGIN
      SELECT 'V'
        INTO lvDropPVD
        FROM user_tables
       WHERE table_name = upper(pvTabla);
      IF lvDropPVD = 'V' THEN
         EXECUTE IMMEDIATE 'DROP TABLE ' || pvTabla;
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END pDropPvd;


   /*##############################################################################
   %desc Obtiene todos los pasos de vehiculos para el dia especificado como parámetro
         de entrada o en su defecto, calculado a partir de delayConsolidacionIRI.
   %autr RHE / Raciel Hernandez
   %parm fpdAnalysisDay día que se desea analizar, si se informa NULL, el sistema recupera
                       el valor del parámetro CNF "delayConsolidacionIRI" y lo resta
                       al día actual
   %rtrn TRUE = proceso terminó satisfactoriamente
   %rtrn FALSE = proceso se ejecutó con error
   %vers 1A / 31mar'09/ RHE
   FUNCTION fbGetPvd(fpdAnalysisDay IN DATE) RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fbGetPvd';
      ldResume DATE;
      lvIni    VARCHAR2(50);
      lvFin    VARCHAR2(50);
      lvSQL    VARCHAR2(2000);
   BEGIN
      -- borra la tabla temporal de PV del dia
      pDropPVD('PVD_PVDIA');
   
      -- calcula el dia de ejecucion, por defecto es el dia anterior
      IF fpdAnalysisDay IS NULL THEN
         ldResume := TRUNC(SYSDATE, 'dd') -
                     kCNF.fnRetrieve('delayConsolidacionIRI', 1);
      ELSE
         ldResume := TRUNC(fpdAnalysisDay, 'dd');
      END IF;
   
      -- registra la fecha que se esta procesando
      kBTC.avErrMsg := TO_CHAR(ldResume, '"IRI#"dd/mm/yyyy');
      kBTC.pSaveMessage(200, lvcPrg, TRUE);
   
      -- genera el filtro de dia para los PV
      lvIni := TO_CHAR(ldResume, '"TO_DATE(''"dd/mm/yyyy"'', ''dd/mm/yyyy'')"');
      lvFin := TO_CHAR(ldResume + 1,
                       '"TO_DATE(''"dd/mm/yyyy"'', ''dd/mm/yyyy'')"');
   
      -- constuye la sentencia de creacion
      lvSQL := 'CREATE TABLE pvd_pvdia TABLESPACE tempssc_data PCTFREE 0 NOLOGGING AS ' ||
               'SELECT /*+ index(ct_pasajes_vehiculo ct_idx_pasajes_veh_fec_sep_fec) */ ' ||
               'fec_sep_fecha_hora      pvd_fecha, ' ||
               'txt_sop_matricula       pvd_matricula, ' ||
               'txt_sep_ocr_matricula_1 pvd_matriculaocr, ' ||
               'cod_pasaje_vehiculo     pvd_pv, ' ||
               'cod_sop_auto_categoria  pvd_categoria, ' ||
               'cod_sep_dcv_categoria_1 pvd_categoriaocr, ' ||
               'cod_portico             pvd_portico, ' ||
               'num_velocidad           pvd_velocidad, ' ||
               'num_sep_alto            pvd_alto, ' ||
               'num_sep_ancho           pvd_ancho, ' ||
               'num_sep_largo           pvd_largo, ' ||
               'num_sep_largo_total     pvd_largototal, ' ||
               'bol_remolque_detectado  pvd_remolque, ' ||
               'num_carril              pvd_carril, ' ||
               'txt_sep_ocr_matricula_1_rear  pvd_matriculaocrtras, ' ||
               'cod_grupo_imagenes      pvd_gimagenes, ' ||
               'fec_sop_envio_man       pvd_enviocm ' ||
               'FROM ct_pasajes_vehiculo ' || 'WHERE cod_pasaje_vehiculo >= ' ||
               TO_CHAR(ldResume, 'yyyymm"000000000000 "') ||
               'AND cod_pasaje_vehiculo  < ' ||
               TO_CHAR(ADD_MONTHS(ldResume, 1), 'yyyymm"000000000000 "') ||
               'AND fec_sep_fecha_hora  >= ' || lvIni ||
               ' AND fec_sep_fecha_hora   < ' || lvFin;
   
      -- crea la tabla pvd_pvdia
      EXECUTE IMMEDIATE lvSQL;
   
      -- registra el fin de creacion
      kBTC.avErrMsg := 'PVD_PVDIA';
      kBTC.pSaveMessage(85, lvcPrg, TRUE);
   
      -- cierra el DBLink para evitar ser eliminado
      EXECUTE IMMEDIATE 'ALTER SESSION CLOSE DATABASE LINK sop';
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         KBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbGetPvd;


   -- #############################################################################
   /**
   Momento cero PVs de
   Ajusta la informacion de patente y categoria a la lectura original del OCR.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvTabla Nombre de la tabla que se desea eliminar.
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbPvdMCero RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fbPvdMCero';
      lvCommand VARCHAR2(250);
   BEGIN
      -- elimina la tabla actualmente existente
      pDropPVD('PVD_PVDMCERO');
   
      -- crea la tabla
      lvCommand := 'CREATE TABLE pvd_pvdmcero TABLESPACE tempssc_data PCTFREE 0 NOLOGGING AS SELECT * FROM pvd_pvdia';
      EXECUTE IMMEDIATE lvCommand;
   
      -- actualiza los registros que no van a CM
      lvCommand := 'UPDATE pvd_pvdmcero SET pvd_matricula = pvd_matriculaocr, pvd_categoria = pvd_categoriaocr WHERE pvd_enviocm IS NOT NULL';
      EXECUTE IMMEDIATE lvCommand;
      COMMIT;
   
      -- crea los indices
      EXECUTE IMMEDIATE 'CREATE INDEX pdv_idx_matricula ON pvd_pvdmcero(pvd_matricula)';
      EXECUTE IMMEDIATE 'CREATE INDEX pdv_idx_pcmatricula ON pvd_pvdmcero(SUBSTR(pvd_matricula, 1, 1))';
   
      -- registra el fin
      kBTC.avErrMsg := 'PVD_PDVMCERO';
      kBTC.pSaveMessage(85, lvcPrg, TRUE);
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         kBTC.avErrMsg := SQLERRM;
         KBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbPvdMCero;


   -- #############################################################################
   /**
   Resume UDV.
   Resume el uso de via diario por vehiculo.
   %author RHE / Raciel Hernandez
   %return TRUE Si el proceso termina satisfactoriamente
   %return FALSE Si el proceso se ejecuta con error
   %version 1A / 31mar'09 / RHE
   %version 1B / 16jun'09 / RHE > incorporación de matricula "NOT PROC" a la lista de
                 matriculas no válidas
   */
   FUNCTION fbResumeUDV RETURN BOOLEAN IS
      CURSOR lcDSPs IS
         SELECT column_value AS Item, 0 AS Done
           FROM TABLE(faConvert2Array('A B C D E F G H I J K L M N O P Q R S T U V W X Y Z 0 1 2 3 4 5 6 7 8 9',
                                      ' '));
      lvOrderSQL VARCHAR2(2500);
   
      laScan     tString;
      lnInd      PLS_INTEGER;
      lvProgram VARCHAR2(30) := gvcPkg || 'fbResumeUDV';
   
   BEGIN
      -- registro de inicio ResumeUDV
      kBTC.avErrMsg := 'resume_UDV';
      kBTC.pSaveMessage(142, lvProgram, TRUE);
   
      -- elimina el indice
      EXECUTE IMMEDIATE 'DROP INDEX udv_idx_pcpatente';
   
      -- trunca la tabla
      EXECUTE IMMEDIATE 'TRUNCATE TABLE udv_udvia';
   
      -- inicializacion del arreglo indicador de scaneo de PVD
      laScan := tString();
      lnInd  := 0;
      FOR lrDPS IN lcDSPs LOOP
         lnInd := lnInd + 1;
         laScan.EXTEND(1);
         laScan(lnInd) := lrDPS.Item;
      END LOOP;
   
      --
      FOR lnInd IN laScan.FIRST .. laScan.LAST LOOP
      
         -- elimina la tabla existente
         pDropPVD('tmp_pvscan' || laScan(lnInd));
      
         -- crea la tabla
         lvOrderSQL := 'CREATE TABLE tmp_pvscan' || laScan(lnInd) ||
                       ' PCTFREE 10 NOLOGGING AS SELECT * FROM pvd_pvdmcero WHERE substr(pvd_matricula, 1, 1) = ''' ||
                       laScan(lnInd) || '''';
      
         EXECUTE IMMEDIATE lvOrderSQL;
      
         -- agrega campo volumen
         EXECUTE IMMEDIATE 'ALTER TABLE tmp_pvscan' || laScan(lnInd) ||
                           ' ADD (pvd_nm_volumen NUMBER)';
      
         -- actualiza el volumen
         EXECUTE IMMEDIATE 'UPDATE tmp_pvscan' || laScan(lnInd) ||
                           ' SET pvd_nm_volumen = pvd_largo * pvd_alto * pvd_ancho';
         COMMIT;
      
         -- crea indice
         EXECUTE IMMEDIATE 'CREATE INDEX tmp_pvscan_inx' || laScan(lnInd) ||
                           ' ON tmp_pvscan' || laScan(lnInd) ||
                           '(pvd_matricula, pvd_fecha)';
      
         -- elimina la tabla existente
         pDropPVD('drop_pvresmatdate');
      
         -- crea la tabla
         EXECUTE IMMEDIATE 'CREATE TABLE drop_pvresmatdate PCTFREE 0 NOLOGGING AS
                            SELECT pvd_matricula, TRUNC(pvd_fecha, ''DDD'') AS pvd_fecha
                              FROM tmp_pvscan' ||
                           laScan(lnInd) ||
                           ' GROUP BY pvd_matricula, TRUNC(pvd_fecha, ''DDD'')';
      
         -- agrega campo de control
         EXECUTE IMMEDIATE 'ALTER TABLE drop_pvresmatdate ADD(resumepath INTEGER)';
      
         -- crea indice
         EXECUTE IMMEDIATE 'CREATE INDEX drop_pvresmatdate_idx ON drop_pvresmatdate(pvd_matricula, pvd_fecha)';
      
         -- resume UDV
         DECLARE
            lvComand CLOB;
         BEGIN
            lvComand := 'DECLARE
            CURSOR lcPVs IS
               SELECT pvd_matricula, pvd_fecha, ROWID
                 FROM drop_pvresmatdate p
                WHERE resumepath IS NULL
                  AND pvd_matricula NOT IN (''NOT VALID'', ''NO READ'', ''NOT PROC'')
                ORDER BY pvd_matricula, pvd_fecha;

            ldFecha     TIMESTAMP(4);
            lvMrk_Pat   VARCHAR2(12);
            ldMrk_Date  DATE;
            ldFechatrnc DATE;
            lvMark      VARCHAR2(25);

            Vinterval INTERVAL DAY TO SECOND;

            lvRecorrido  VARCHAR2(3950);
            lvInterval   VARCHAR2(3950);
            lvPV         VARCHAR2(3950);
            lvVelocidad  VARCHAR2(3950);
            lvCategoria  VARCHAR2(3950);
            lvVolumen    VARCHAR2(3950);
            lvGImagen    VARCHAR2(3950);
            lvEnvioCM    VARCHAR2(3950);
            lvLargo      VARCHAR2(3950);
            lvLargoTotal VARCHAR2(3950);
            lvRemolque   VARCHAR2(3950);
            lvCarril     VARCHAR2(3950);
            lvMatriculaOCRTras VARCHAR2(3950);

         BEGIN
            lvMrk_Pat  := NULL;
            ldMrk_Date := NULL;
            lvMark     := NULL;
            kbtc.pInitCommit(55000, ''' || lvProgram ||
                        ''');
            FOR lrPV IN lcPVs LOOP
               DECLARE
                  CURSOR lcInsidePVs IS
                     SELECT pvd_fecha,
                            pvd_matricula,
                            pvd_pv,
                            pvd_portico,
                            pvd_categoria,
                            pvd_velocidad,
                            pvd_largo,
                            pvd_largototal,
                            pvd_nm_volumen,
                            pvd_remolque,
                            pvd_carril,
                            pvd_matriculaocrtras,
                            pvd_gimagenes,
                            pvd_enviocm
                       FROM tmp_pvscan' || laScan(lnInd) || '
                      WHERE (pvd_matricula = lrPV.pvd_matricula AND
                            Trunc(pvd_fecha, ''DDD'') = ldFechatrnc)
                      ORDER BY pvd_matricula, pvd_fecha
                      WHERE ROWNUM <= 210;
               BEGIN
                  ldFechatrnc := Trunc(lrPV.pvd_fecha, ''DDD'');
                  IF lvMrk_Pat IS NULL OR
                     lvMark <> (lrPV.pvd_matricula || Trunc(lrPV.pvd_fecha, ''DDD'')) THEN
                     FOR lrInsidePV IN lcInsidePVs LOOP
                        BEGIN
                           IF lvRecorrido IS NULL THEN
                              lvRecorrido := To_Char(lrInsidePV.pvd_portico);
                              ldFecha     := lrInsidePV.pvd_fecha; --Esta es la primera fecha con formato de hoa (Momento en que entra a la autopista)
                              Vinterval   := ldFecha - ldFecha;
                              lvInterval  := substr(To_Char(Vinterval), 5, 8);
                              lvCategoria := To_Char(lrInsidePV.pvd_categoria);
                              lvVelocidad := To_Char(lrInsidePV.pvd_velocidad);
                              lvVolumen   := To_Char(lrInsidePV.pvd_nm_volumen);
                              lvPV        := To_Char(lrInsidePV.pvd_pv);
                              lvLargo     := To_Char(lrInsidePV.pvd_largo);
                              lvLargoTotal:= To_Char(lrInsidePV.pvd_largototal);
                              lvRemolque  := To_Char(lrInsidePV.pvd_remolque);
                              lvCarril    := To_Char(lrInsidePV.pvd_carril);
                              lvMatriculaOCRTras:= To_Char(lrInsidePV.pvd_matriculaocrtras);
                              
                              IF lrInsidePV.pvd_gimagenes IS NULL THEN
                                 lvGImagen   := ''0'';
                              ELSE
                                  lvGImagen   := ''1'';
                              END IF;
                              IF lrInsidePV.pvd_enviocm IS NULL THEN
                                 lvEnvioCM := ''0'';
                              ELSE
                                 lvEnvioCM := ''1'';
                                 END IF;
                           ELSE
                              lvRecorrido := lvRecorrido || '';'' ||
                                             To_Char(lrInsidePV.pvd_portico);
                              Vinterval   := lrInsidePV.pvd_fecha - ldFecha;
                              lvInterval  := lvInterval || '';'' ||
                                             substr(To_Char(Vinterval), 5, 8);
                              lvCategoria := lvCategoria || '';'' ||
                                             To_Char(lrInsidePV.pvd_categoria);
                              lvVelocidad := lvVelocidad || '';'' ||
                                             To_Char(lrInsidePV.pvd_velocidad);
                              lvVolumen   := lvVolumen || '';'' ||
                                             To_Char(lrInsidePV.pvd_nm_volumen);
                              lvPV        := lvPV || '';'' || 
                                             To_Char(lrInsidePV.pvd_pv);
                              lvLargo     := lvLargo || '';'' || 
                                             To_Char(lrInsidePV.pvd_largo);
                              lvLargoTotal:= lvLargoTotal || '';'' || 
                                             To_Char(lrInsidePV.pvd_largototal);
                              lvRemolque  := lvRemolque || '';'' || 
                                             To_Char(lrInsidePV.pvd_remolque);
                              lvCarril    := lvCarril || '';'' || 
                                             To_Char(lrInsidePV.pvd_carril);
                              lvMatriculaOCRTras:= lvMatriculaOCRTras || '';'' || 
                                                   To_Char(lrInsidePV.pvd_matriculaocrtras);

                              IF lrInsidePV.pvd_gimagenes IS NULL THEN
                                 lvGImagen   := lvGImagen || '';'' || ''0'';
                              ELSE
                                  lvGImagen   := lvGImagen || '';'' || ''1'';
                                  END IF;

                              IF lrInsidePV.pvd_enviocm IS NULL THEN
                                 lvEnvioCM := lvEnvioCM || '';'' || ''0'';
                              ELSE
                                 lvEnvioCM := lvEnvioCM || '';'' || ''1'';
                                 END IF;
                             END IF;
                        EXCEPTION
                           WHEN no_data_found THEN
                              dbms_output.put_line(''Out'');
                              EXIT;
                        END;
                     END LOOP;

                     -- inserta registro UDV
                     INSERT INTO udv_udvia
                        (udv_tx_patente,
                         udv_dt_fecha,
                         udv_tx_vpv,
                         udv_tx_vrecorrido,
                         udv_tx_vinterval,
                         udv_tx_vcategoria,
                         udv_tx_vvelocidad,
                         udv_tx_vvolumen,
                         udv_tx_largo,
                         udv_tx_largototal,
                         udv_tx_remolque,
                         udv_tx_carril,
                         udv_tx_matriculaocrtras,
                         udv_tx_vgimg,
                         udv_tx_venviocm)
                     VALUES
                        (lrPV.pvd_matricula,
                         ldFecha,
                         lvPv,
                         lvRecorrido,
                         lvInterval,
                         lvCategoria,
                         lvVelocidad,
                         lvVolumen,
                         lvLargo,
                         lvLargoTotal,
                         lvRemolque,
                         lvCarril,
                         lvMatriculaOCRTras,
                         lvGImagen,
                         lvEnvioCM);

                     lvMrk_Pat  := lrPV.pvd_matricula;
                     ldMrk_Date := lrPV.pvd_fecha;
                     lvMark     := lvMrk_Pat || Trunc(ldMrk_Date, ''DDD'');

                     lvRecorrido := NULL;
                     Vinterval   := NULL;
                     lvInterval  := NULL;
                     lvCategoria := NULL;
                     lvVelocidad := NULL;
                     lvVolumen   := NULL;
                     lvPV        := NULL;
                     lvGImagen   := NULL;
                     lvEnvioCM   := NULL;
                     lvLargo     := NULL;
                     lvLargoTotal:= NULL;
                     lvRemolque  := NULL;
                     lvCarril    := NULL;
                     lvMatriculaOCRTras := NULL;

                     UPDATE drop_pvresmatdate SET resumepath = 1 WHERE ROWID = lrPV.ROWID;
                     kBTC.pRealizarCommit;

                  END IF;
               EXCEPTION
                  WHEN no_data_found THEN
                     kBTC.avErrMsg := SQLERRM;
                     kBTC.pSaveMessage(1, ''' ||
                        lvProgram || ''', TRUE);
               END;
            END LOOP;
            kBTC.pFinalizarCommit;
         END;';
            EXECUTE IMMEDIATE TO_CHAR(lvComand);
            COMMIT;
         END;
      
         -- elimina las tablas TEMPORALES
         EXECUTE IMMEDIATE 'DROP TABLE tmp_pvscan' || laScan(lnInd);
         EXECUTE IMMEDIATE 'DROP TABLE drop_pvresmatdate';
      
      END LOOP;
   
      -- forzamos a oracle a que tenga en cuenta el indice de funcion
      EXECUTE IMMEDIATE 'ALTER SESSION SET QUERY_REWRITE_ENABLED = TRUE';
      EXECUTE IMMEDIATE 'ALTER SESSION SET QUERY_REWRITE_INTEGRITY = TRUSTED';
   
      -- construye el indice udv_idx_pcpatente
      EXECUTE IMMEDIATE 'CREATE INDEX udv_idx_pcpatente ON udv_udvia(substr(udv_tx_patente, 1, 1))';
   
      -- registro de fin
      kBTC.avErrMsg := 'resume_UDV';
      kBTC.pSaveMessage(85, lvProgram, TRUE);
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbResumeUDV;


   -- #############################################################################
   /**
   Distancia entre porticos.
   Funcion que determina la distancia entre porticos.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pnRef1 Portico de referencia 1.
   %param pnRef2 Portico de referencia 2.
   %return NUMBER Distancia entre los porticos pnRef1 y pnRef2.
   */
   FUNCTION fnDistanceEP(pnRef1 PLS_INTEGER, pnRef2 PLS_INTEGER) RETURN NUMBER IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fnDistanceEP';
      lnDistance NUMBER(12);
   BEGIN
      -- obtiene la distancia entre porticos
      SELECT dtn_nm_distancia
        INTO lnDistance
        FROM dtn
       WHERE dtn_fk_prt = pnRef1
         AND dtn_fk_srf = pnRef2;
      RETURN(lnDistance);
   EXCEPTION
      WHEN no_data_found THEN
         RETURN NULL;
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1,
                             lvcPrg,
                             TRUE,
                             TO_CHAR(pnRef1) || '/' || TO_CHAR(pnRef2));
         RETURN NULL;
   END fnDistanceEP;


   -- #############################################################################
   /**
   Calculo de IRI.
   Procedimiento que calcula el valor de IRI.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvSecuence Vector secuencia de porticos recorrido por el vehiculo.
   %param pvInterval Vector intervalo de tiempo cuando un vehiculo recorre una secuencia de porticos.
   %param pvVelocidad Vector velocidad cuando un vehiculo recorre una secuencia de porticos.
   %param pvCategoria Vector categoria registrada cuando un vehiculo recorre una secuencia de porticos.
   %param pvVolumen Vector volumen calculado cuando un vehiculo recorre una secuencia de porticos.
   %param pvPV Vector con el registro del paso de vehiculo cuando un vehiculo recorre una secuencia de porticos.
   %param pvDelimitador Caracter delimitador de los elementos del vector.
   %return lvIRI Vector que contiene el valor de IRI calculado.
   */
   PROCEDURE lnpCalculateIRI(pvSecuence    VARCHAR2,
                          pvInterval    VARCHAR2,
                          pvVelocidad   VARCHAR2,
                          pvCategoria   VARCHAR2 := '',
                          pvVolumen     VARCHAR2 := '',
                          pvPV          VARCHAR2 := '',
                          pvDelimitador VARCHAR2 := ';',
                          lvIRI         OUT VARCHAR2) IS
   
      lvlistS  VARCHAR2(32767) := pvSecuence;
      lvlistI  VARCHAR2(32767) := pvInterval;
      lvlistVe VARCHAR2(32767) := pvVelocidad;
      lvlistVo VARCHAR2(32767) := pvVolumen;
      lvlistCa VARCHAR2(32767) := pvCategoria;
      lvlistPV VARCHAR2(32767) := pvPV;
   
      lnInxP  PLS_INTEGER := 1;
      lnInxI  PLS_INTEGER := 1;
      lnInxVe PLS_INTEGER := 1;
      lnInxVo PLS_INTEGER := 1;
      lnInxCa PLS_INTEGER := 1;
      lnInxPV PLS_INTEGER := 1;
   
      lvPortico VARCHAR2(2);
      lvTime    VARCHAR2(45);
      lvVel     NUMBER(6);
      lvVol     VARCHAR2(18);
      lvCat     VARCHAR2(1);
      lvPVh     VARCHAR2(18);
      lnOrden   NUMBER(4);
   
      TYPE arrayInd IS VARRAY(250) OF NUMBER(3);
      TYPE arrayPV IS VARRAY(250) OF NUMBER(18);
      TYPE arraySec IS VARRAY(250) OF NUMBER(3);
      TYPE arrayInt IS VARRAY(250) OF NUMBER(8, 2);
      TYPE arrayVel IS VARRAY(250) OF NUMBER(5);
      TYPE arrayVol IS VARRAY(250) OF NUMBER(12);
      TYPE arrayCat IS VARRAY(259) OF NUMBER(1);
   
      TYPE arrayDpc IS ARRAY(259) OF NUMBER(8, 2); --distancia entre porticos consecutivos
      TYPE arrayVmc IS ARRAY(259) OF NUMBER(6, 4); --velocidad media de calculo ente porticos
      TYPE arrayTmc IS ARRAY(259) OF NUMBER(8, 2); --tiempo de recorrido medio calculado entre porticos
      TYPE arrayTac IS ARRAY(259) OF NUMBER(8, 2); --tiempo de recorrido acumulado en cada portico desde su comienzo de uso de via
      lnMark NUMBER(8, 2); --marca para el calculo del tiempo acumulado en la ruta
   
      TYPE arrayIRI IS ARRAY(259) OF INTEGER; --identificador de ruta inicial
   
      --lvProgram VARCHAR2(25) := gvcPrograma || 'lnpCalculateIRI';
   
   BEGIN
      DECLARE
         laInd arrayInd;
         laPV  arrayPV;
         laSec arraySec;
         laInt arrayInt;
         laVel arrayVel;
         laVol arrayVol;
         laCat arrayCat;
      
      BEGIN
         laInd := arrayInd(NULL);
         laPV  := arrayPV(NULL);
         laSec := arraySec(NULL);
         laInt := arrayInt(NULL);
         laVel := arrayVel(NULL);
         laVol := arrayVol(NULL);
         laCat := arrayCat(NULL);
      
         lnOrden := 0;
      
         LOOP
            lnOrden := lnOrden + 1;
            lnInxP  := INSTR(lvlistS, pvDelimitador);
            lnInxI  := INSTR(lvlistI, pvDelimitador);
            lnInxVe := INSTR(lvlistVe, pvDelimitador);
            lnInxVo := INSTR(lvlistVo, pvDelimitador);
            lnInxCa := INSTR(lvlistCa, pvDelimitador);
            lnInxPV := INSTR(lvlistPV, pvDelimitador);
         
            laInd.EXTEND(1);
            laSec.EXTEND(1);
            laInt.EXTEND(1);
            laVel.EXTEND(1);
            laVol.EXTEND(1);
            laCat.EXTEND(1);
            laPV.EXTEND(1);
         
            IF lnInxP = 0 OR lnInxI = 0 THEN
               lvPortico := lvlistS;
               lvTime    := lvlistI;
               lvVel     := lvlistVe;
               lvVol     := lvlistVo;
               lvCat     := lvlistCa;
               lvPVh     := lvlistPV;
            
               IF instr(lvTime, '+', 1) = 1 THEN
                  lvTime := substr(lvTime, instr(lvTime, ':', 1) - 2, 8);
               END IF;
            
               laInd(lnOrden) := lnOrden;
               laSec(lnOrden) := to_number(lvPortico);
               laInt(lnOrden) := to_number(substr(lvTime, 1, 2)) * 3600 +
                                 to_number(substr(lvTime, 4, 2)) * 60 +
                                 to_number(substr(lvTime, 7, 2));
               laVel(lnOrden) := to_number(lvVel);
               laVol(lnOrden) := to_number(lvVol);
               laCat(lnOrden) := to_number(lvCat);
               laPV(lnOrden) := to_number(lvPVh);
            
               EXIT;
            ELSE
               lvPortico := substr(lvListS, 1, lnInxP - 1);
               lvTime    := substr(lvListI, 1, lnInxI - 1);
               lvVel     := substr(lvListVe, 1, lnInxVe - 1);
               lvVol     := substr(lvListVo, 1, lnInxVo - 1);
               lvCat     := substr(lvListCa, 1, lnInxCa - 1);
               lvPVh     := substr(lvListPV, 1, lnInxPV - 1);
            
               IF instr(lvTime, '+', 1) = 1 THEN
                  lvTime := substr(lvTime, instr(lvTime, ':', 1) - 2, 8);
               END IF;
            
               laInd(lnOrden) := lnOrden;
               laSec(lnOrden) := to_number(lvPortico);
               laInt(lnOrden) := to_number(substr(lvTime, 1, 2)) * 3600 +
                                 to_number(substr(lvTime, 4, 2)) * 60 +
                                 to_number(substr(lvTime, 7, 2));
               laVel(lnOrden) := to_number(lvVel);
               laVol(lnOrden) := to_number(lvVol);
               laCat(lnOrden) := to_number(lvCat);
               laPV(lnOrden) := to_number(lvPVh);
            
               lvlistS  := LTRIM(SUBSTR(lvlistS, lnInxP + LENGTH(pvDelimitador)));
               lvlistI  := LTRIM(SUBSTR(lvlistI, lnInxI + LENGTH(pvDelimitador)));
               lvlistVe := LTRIM(SUBSTR(lvlistVe,
                                        lnInxVe + LENGTH(pvDelimitador)));
               lvlistVo := LTRIM(SUBSTR(lvlistVo,
                                        lnInxVo + LENGTH(pvDelimitador)));
               lvlistCa := LTRIM(SUBSTR(lvlistCa,
                                        lnInxCa + LENGTH(pvDelimitador)));
               lvlistPV := LTRIM(SUBSTR(lvlistPV,
                                        lnInxPV + LENGTH(pvDelimitador)));
            
            END IF;
         END LOOP;
      
         DECLARE
            lnIdx NUMBER(4);
            laDpc arrayDpc := arrayDpc(NULL);
            laVmc arrayVmc := arrayVmc(NULL);
            laTmc arrayTmc := arrayTmc(NULL); --tiempo calculado de recorrido entre porticos
            laTrr arrayTmc := arrayTmc(NULL); --tiempo real recorrido entre dos porticos
            laTac arrayTac := arrayTac(NULL); --tiempo acumulado calculado de recorrido
            laTar arrayTac := arrayTac(NULL); --tiempo acumulado real de recorrido
         
            laIRI arrayIRI := arrayIRI(NULL);
         
            lnVmin1 NUMBER(6, 2);
            lnVprm1 NUMBER(6, 2);
            lnVmin2 NUMBER(6, 2);
            lnVprm2 NUMBER(6, 2);
         
            lnTteo     NUMBER(8, 2); --tiempo teorico o calculado de recorrido entre porticos
            lnTteo_min NUMBER(8, 2);
            lnTteo_avg NUMBER(8, 2);
            lnIRI      NUMBER(8, 4);
            lnIRI_min  NUMBER(8, 4);
            lnIRI_avg  NUMBER(8, 4);
         
         BEGIN
            lnMark := 0;
            FOR lnIdx IN laInd.FIRST + 1 .. laInd.LAST - 1 LOOP
               BEGIN
                  laDpc.EXTEND(1);
                  laIRI.EXTEND(1);
                  laVmc.EXTEND(1);
                  laTmc.EXTEND(1);
                  laTac.EXTEND(1);
                  laTrr.EXTEND(1);
                  laTar.EXTEND(1);
               
               
                  laDpc(lnIdx) := fnDistanceEP(laSec(lnIdx), laSec(lnIdx - 1));
               
                  IF laDpc(lnIdx) IS NOT NULL THEN
                     laIRI(lnIdx) := 1;
                  
                     IF laVel(lnIdx) = 0 AND laVel(lnIdx - 1) <> 0 THEN
                        laVmc(lnIdx) := laVel(lnIdx - 1) / 100;
                     ELSIF laVel(lnIdx) <> 0 AND laVel(lnIdx - 1) = 0 THEN
                        laVmc(lnIdx) := laVel(lnIdx) / 100;
                     ELSE
                        laVmc(lnIdx) := (laVel(lnIdx) + laVel(lnIdx - 1)) / 2 / 100; --/100 para convertir a unidad de m/s
                     END IF;
                  
                     IF laVmc(lnIdx) <> 0 THEN
                        laTmc(lnIdx) := laDpc(lnIdx) / laVmc(lnIdx);
                        laTrr(lnIdx) := laInt(lnIdx) - laInt(lnIdx - 1);
                        laTac(lnIdx) := nvl(laTac(lnIdx - 1), 0) + laTmc(lnIdx);
                        laTar(lnIdx) := laInt(lnIdx) - lnMark;
                     
                        BEGIN
                           SELECT evl_nm_vavg, evl_nm_vmin
                             INTO lnVprm1, lnVmin1
                             FROM evl
                            WHERE evl_nm_categoria = laCat(lnIdx)
                              AND evl_nm_portico = laSec(lnIdx);
                        EXCEPTION
                           WHEN no_data_found THEN
                              lnVprm1 := 0;
                        END;
                     
                        BEGIN
                           SELECT evl_nm_vavg, evl_nm_vmin
                             INTO lnVprm2, lnVmin2
                             FROM evl
                            WHERE evl_nm_categoria = laCat(lnIdx - 1)
                              AND evl_nm_portico = laSec(lnIdx - 1);
                        EXCEPTION
                           WHEN no_data_found THEN
                              lnVprm2 := 0;
                        END;
                     
                        lnTteo     := laDpc(lnIdx) /
                                      ((laVel(lnIdx) + laVel(lnIdx - 1)) / 2 / 100);
                        lnTteo_avg := laDpc(lnIdx) /
                                      ((lnVprm1 + lnVprm2) / 2 / 100);
                        lnTteo_min := laDpc(lnIdx) /
                                      ((lnVmin1 + lnVmin2) / 2 / 100);
                     
                        IF lnTteo IS NULL THEN
                           lnTteo := 0;
                        END IF;
                     
                        IF lnTteo_min IS NULL THEN
                           lnTteo_min := 0;
                        END IF;
                     
                        IF lnTteo_avg IS NULL THEN
                           lnTteo_avg := 0;
                        END IF;
                     
                        IF laTrr(lnIdx) <> 0 THEN
                           lnIRI := round(lnTteo / laTrr(lnIdx), 4);
                           lnIRI_min := lnTteo_min / laTrr(lnIdx);
                           lnIRI_avg := lnTteo_avg / laTrr(lnIdx);
                           laIRI(lnIdx) := round(lnIRI);
                        END IF;
                     
                        IF lnIRI_avg > lnIRI THEN
                           laIRI(lnIdx) := round(lnIRI_avg);
                        END IF;
                     
                        IF lnIRI_min > lnIRI THEN
                           laIRI(lnIdx) := round(lnIRI_min);
                        END IF;
                     
                     END IF;
                  ELSE
                     laIRI(lnIdx) := 0;
                     lnMark := laInt(lnIdx);
                     laVmc(lnIdx) := NULL;
                     laTmc(lnIdx) := NULL;
                  END IF;
               
                  lvIRI := lvIRI || ';' || laIRI(lnIdx);
               
               END;
            
            END LOOP;
         
            lvIRI := LTRIM(SUBSTR(lvIRI, 2));
         
         END;
      END;
   END lnpCalculateIRI;


   -- #############################################################################
   /**
   Matriz de rutas IRI
   Conforma la matriz de rutas IRI de todos los vehiculos resumidos en UDV.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbConformingIRI RETURN BOOLEAN IS
      lvProgram VARCHAR2(25) := gvcPkg || 'fbConformingIRI';
      -- cursor scan
      CURSOR lcDSPs IS
         SELECT COLUMN_VALUE AS Item, 0 AS Done
           FROM TABLE(faConvert2Array('A B C D E F G H I J K L M N O P Q R S T U V W X Y Z 0 1 2 3 4 5 6 7 8 9',
                                      ' '));
   
      CURSOR lcRMs(vLetra VARCHAR2) IS
         SELECT udv_tx_vrecorrido vR,
                udv_tx_vinterval vI,
                udv_tx_vcategoria vC,
                udv_tx_vvelocidad vVe,
                udv_tx_vvolumen vVo,
                udv_tx_vpv vPv,
                ROWID
           FROM udv_udvia
          WHERE SUBSTR(udv_tx_patente, 1, 1) = vLetra
            AND udv_tx_viri IS NULL
            AND INSTR(udv_tx_vrecorrido, ';') <> 0;
      lvIRI VARCHAR2(4000);
   BEGIN
      --registro de inicio
      kBTC.avErrMsg := 'conformar_IRI';
      kBTC.pSaveMessage(142, lvProgram, TRUE);
   
      FOR lrDSP IN lcDSPs LOOP
         kBTC.pInitCommit(50000, lvProgram);
         FOR lrRM IN lcRMs(lrDSP.item) LOOP
            lnpCalculateIRI(lrRM.vR,
                         lrRM.vI,
                         lrRM.vVe,
                         lrRM.vC,
                         lrRM.vVo,
                         lrRM.vPv,
                         ';',
                         lvIRI);
         
            UPDATE udv_udvia SET udv_tx_viri = lvIRI WHERE ROWID = lrRM.ROWID;
         
            kBTC.pRealizarCommit;
         END LOOP;
         kBTC.pFinalizarCommit;
      END LOOP;
   
      --registro de fin
      kBTC.avErrMsg := 'conformar_IRI';
      kBTC.pSaveMessage(85, lvProgram, FALSE);
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbConformingIRI;


   -- #############################################################################
   /**
   Vector de n elementos iguales entre si.
   Construye un vector de n elementos iguales entre si.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvX Valor de la secuencia de elementos del vector.
   %param pnSize Tamaño del vector a construir.
   %param pvCharSplit Caracter delimitador entre los elementos del vector.
   %return Vector
   */
   FUNCTION fvVectorX(pvX         IN VARCHAR2,
                      pnSize      IN PLS_INTEGER,
                      pvCharSplit VARCHAR2 := ';') RETURN VARCHAR2 IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fvVectorX';
      lvVector VARCHAR2(5000) := pvX;
   BEGIN
      -- devolver null
      IF pnSize <= 0 THEN
         RETURN NULL;
      END IF;
   
      FOR i IN 1 .. pnSize - 1 LOOP
         lvVector := lvVector || pvCharSplit || pvX;
      END LOOP;
   
      RETURN lvVector;
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN NULL;
   END fvVectorX;


   -- #############################################################################
   /**
   Tamaño de un vector.
   Calcula el tamaño de un vector dado.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvVector Vector.
   %param pnSize Tamaño del vector a construir.
   %param pvCharSplit Caracter delimitador entre los elementos del vector.
   %return INTEGER Tamaño del vector.
   */
   FUNCTION fnVectorSize(pvVector IN VARCHAR2, pvCharSplit IN VARCHAR2 := ';')
      RETURN PLS_INTEGER IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fnVectorSize';
      lnSize PLS_INTEGER;
      lnPos  PLS_INTEGER;
   BEGIN
      -- inicializacion de variables
      lnPos  := 1;
      lnSize := 0;
   
      LOOP
         lnPos := instr(pvVector, pvCharSplit, lnPos);
         IF lnPos = 0 THEN
            EXIT;
         ELSE
            lnSize := lnSize + 1;
            lnPos  := lnPos + 1;
         END IF;
      
      END LOOP;
   
      -- cantidad de elementos
      lnSize := lnSize + 1;
   
      RETURN lnSize;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN lnSize;
      
   END fnVectorSize;


   -- #############################################################################
   /**
   Elemento del vector en una posicion dada.
   Devuelve el valor del elemento del vector en una posicion dada.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvVector Vector.
   %param pnItem Posicion del elemento del vector que se desea recuperar.
   %return string Valor con el elemento del vector.
   */
   FUNCTION fvVectorItem(pvVector    IN VARCHAR2,
                         pnItem      IN PLS_INTEGER,
                         pvCharSplit IN VARCHAR2 := ';') RETURN VARCHAR2 IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fvVectorItem';
      lvVector VARCHAR2(4000) := pvVector;
      lvItem   VARCHAR2(4000);
      lnPos    PLS_INTEGER;
      lnItem   PLS_INTEGER;
      lnLChar  PLS_INTEGER;
   
   BEGIN
   
      -- inicializacion de variables
      lnItem  := 0;
      lnPos   := 0;
      lnLChar := length(lvVector);
   
      LOOP
         lnItem := lnItem + 1;
      
         IF lvVector IS NULL THEN
            RETURN NULL;
         END IF;
      
         lnPos := instr(lvVector, pvCharSplit, 1);
         IF lnItem = pnItem AND lnPos <> 0 THEN
            lvItem := substr(lvVector, 1, lnPos - 1);
            EXIT;
         ELSIF lnItem = pnItem AND lnPos = 0 THEN
            lvItem := lvVector;
         ELSE
            lvVector := substr(lvVector, lnPos + 1, lnLChar);
         END IF;
      
         IF lnPos = 0 THEN
            EXIT;
         END IF;
      
      END LOOP;
   
      RETURN lvItem;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN NULL;
      
   END fvVectorItem;


   -- #############################################################################
   /**
   Reemplaza todos los elementos coincidentes dentro de un vector.
   Funcion que reemplaza todos los elementos de un vector que son coincidentes con uno dado por otro nuevo.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvVector Vector.
   %param pvReplace Valor que se desea reemplazar.
   %param pvNewValue Valor nuevo de reemplazo.
   %return STRING Nuevo vector.
   */
   FUNCTION fvReplaceAll(pvVector   IN VARCHAR2,
                         pvReplace  IN VARCHAR2,
                         pvNewValue VARCHAR2) RETURN VARCHAR2 IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fvReplaceAll';
      lvVector     VARCHAR2(5000) := pvVector;
      lvItemValue  VARCHAR2(5000);
      lnVectorSize PLS_INTEGER;
      lvVectorRepl VARCHAR2(5000);
   
   BEGIN
      lnVectorSize := fnVectorSize(lvVector, ';');
   
      -- recorrer cada elemento
      FOR i IN 1 .. lnVectorSize LOOP
         lvItemValue := fvVectorItem(lvVector, i, ';');
      
         IF lvItemValue = pvReplace THEN
            -- realiza el reemplazo
            lvVectorRepl := lvVectorRepl || ';' || pvNewValue;
         ELSE
            -- continua
            lvVectorRepl := lvVectorRepl || ';' || lvItemValue;
         END IF;
      
      END LOOP;
   
      IF lvVectorRepl IS NOT NULL THEN
         lvVectorRepl := ltrim(lvVectorRepl, ';');
         RETURN lvVectorRepl;
      ELSE
         RETURN NULL;
      END IF;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN NULL;
   END fvReplaceAll;


   -- #############################################################################
   /**
   Busca un elemento dentro de un vector.
   Busca un elemento dentro de un vector y devuelve la primera posicion a partir de una posicion de busqueda, si es que lo encuentra.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvVector Vector.
   %param pnSize Tamaño del vector a construir.
   %param pvSearchX Elemento de busqueda.
   %param pnFromItem Posicion de inicio de la busqueda.
   %return INTEGER Posicion donde se encuentra el elemento.
   */
   FUNCTION fnSearhItem(pvVector   IN VARCHAR2,
                         pvSearchX  IN VARCHAR2,
                         pnFromItem IN PLS_INTEGER := 1) RETURN PLS_INTEGER IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fnSearhItem';
      lvVector     VARCHAR2(5000) := pvVector;
      lvItemValue  VARCHAR2(5000);
      lnVectorSize PLS_INTEGER;

   BEGIN
      lnVectorSize := fnVectorSize(lvVector, ';');
   
      -- si el inicio de busqueda esta fuera de rango devuelve nulo
      IF pnFromItem > lnVectorSize THEN
         RETURN NULL;
      END IF;
   
      -- recorre cada elemento a partir del elemento de busqueda
      FOR i IN pnFromItem .. lnVectorSize LOOP
         lvItemValue := fvVectorItem(lvVector, i, ';');
      
         IF lvItemValue = pvSearchX THEN
            RETURN i; --devuelve la posicion encontrada
         END IF;
      
      END LOOP;
   
      -- si no lo encuentra devuelve nulo
      RETURN NULL;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN NULL;
   END fnSearhItem;


   -- #############################################################################
   /**
   Busca un vector dentro de otro vector.
   Busca un vector dentro de otro vector y devuelve la primera posicion a partir de una posicion de busqueda, si es que lo encuentra.
   %author RHernandez
   %version 1A/ 31mar'09/ RHernandez
   %param pvVector Vector.
   %param pnSize Tamaño del vector a construir.
   %param pvSearchXS Segmento o vector de busqueda.
   %param pnFromItem Posicion de inicio de la busqueda.
   %return INTEGER Posicion donde se encuentra el segmento.
   */
   FUNCTION fnBuscarSeg(pvVector   IN VARCHAR2,
                        pvSearchXS IN VARCHAR2,
                        pnFromItem IN PLS_INTEGER := 1) RETURN PLS_INTEGER IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fnBuscarSeg';
      lvVector     VARCHAR2(5000) := pvVector;
      lvItemValue  VARCHAR2(5000);
      lvItemSearch VARCHAR2(5000);
      lnVectorSize PLS_INTEGER;
      lnSegmenSize PLS_INTEGER;
      lbMatch      BOOLEAN;
      lnDespl      PLS_INTEGER;
      lvItemV      VARCHAR2(5000);
      lvItemS      VARCHAR2(5000);
      lnFromItem   PLS_INTEGER := pnFromItem;
   
   BEGIN
   
      -- si no hay parametros de entrada devuelve nulo
      IF pvVector IS NULL OR pvSearchXS IS NULL THEN
         RETURN NULL;
      END IF;
   
      -- si son iguales esta devolver 1
      IF pvVector = pvSearchXS AND lnFromItem = 1 THEN
         RETURN 1;
      END IF;
   
      lnVectorSize := fnVectorSize(lvVector, ';');
      lnSegmenSize := fnVectorSize(pvSearchXS, ';');
   
      -- si esta fuera de rango devolver nulo
      IF lnFromItem > lnVectorSize OR
         lnSegmenSize > lnVectorSize - lnFromItem + 1 THEN
         RETURN NULL;
      END IF;
   
      -- recorrer cada elemento a partir de la posicion de busqueda
      FOR i IN lnFromItem .. lnVectorSize LOOP
         lnDespl     := 0;
         lvItemValue := fvVectorItem(lvVector, i, ';');
         IF lvItemSearch IS NULL THEN
            lvItemSearch := fvVectorItem(pvSearchXS, 1, ';');
         END IF;
         IF lvItemValue = lvItemSearch THEN
            --analiszar el resto de la secuencia
            lbMatch := TRUE;
            FOR j IN 2 .. lnSegmenSize LOOP
               lnDespl := lnDespl + 1;
               lvItemV := fvVectorItem(lvVector, i + lnDespl, ';');
               lvItemS := fvVectorItem(pvSearchXS, j, ';');
               IF lvItemV IS NULL OR lvItemV <> lvItemS THEN
                  lbMatch := FALSE;
                  EXIT;
               END IF;
            
            END LOOP;
         
            -- el elemento se encontro
            IF lbMatch THEN
               RETURN i;
            END IF;
         
         END IF;
      
      END LOOP;
   
      -- si no se encuentra devuelve nulo
      RETURN NULL;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
         RETURN NULL;
   END fnBuscarSeg;


   /*##############################################################################
   %desc Prediccion de patentes segun IRI a partir de la informacion de ruta inicial
         conformada entre n (CNF=cantidadPorticosIRI) pórticos.
   %rtrn TRUE = el proceso termina satisfactoriamente
   %rtrn FALSE = si el proceso se ejecuta con error
   %autr RHE / Raciel Hernandez
   %vers 1A / 31mar'09 / RHE
   %vers 1B / 13oct'09 / S&S > se agrega parámetro CNF para la cantidad de pórticos
   */
   FUNCTION fbIRIConsPat RETURN BOOLEAN IS
      lvProgram VARCHAR2(25) := gvcPkg || 'fbIRIConsPat';
      CURSOR lcIRIs IS
         SELECT udv_tx_vpv,
                udv_tx_vcategoria,
                udv_tx_venviocm,
                udv_tx_viri,
                ROWID
           FROM udv_udvia
          WHERE udv_tx_viri IS NOT NULL
            AND INSTR(udv_tx_venviocm, '1', 1) <> 0;
   
      lvPvNoAvi_IRI VARCHAR2(4000);
      lvCatVal_IRI  VARCHAR2(4000);
      lvIRI         VARCHAR2(4000);
      lvIRIMatch    VARCHAR2(4000);
      lnPv          PLS_INTEGER;
      lnPorts       PLS_INTEGER := kCNF.fnRetrieve('cantidadPorticosIRI', 2);
   BEGIN
      -- registro de inicio
      kBTC.avErrMsg := 'predictor_PAT';
      kBTC.pSaveMessage(142, lvProgram, TRUE);
   
      -- fija la frecuencia de COMMIT
      kBTC.pInitCommit(2000, lvProgram);
   
      --construye el match a buscar
      lvIRIMatch := fvVectorX('1', lnPorts - 1, ';');
   
      FOR lrIRI IN lcIRIs LOOP
         -- inicializa las variables
         lvPvNoAvi_IRI := NULL;
         lvCatVal_IRI  := NULL;
      
         -- cantidad de PV resumidos en IRI
         lnPv := fnVectorSize(lrIRI.udv_tx_viri, ';') + 1;
      
         IF lnPv >= lnPorts THEN
            -- recorre cada pv
            FOR lnItem IN 1 .. lnPv LOOP
               -- si el pv es enviado a cm
               IF fvVectorItem(lrIRI.udv_tx_venviocm, lnItem, ';') = '1' THEN
                  --iri 2 y 3 son considerados como 1
                  lvIRI := fvReplaceAll(lrIRI.udv_tx_viri, '2', '1');
                  lvIRI := fvReplaceAll(lvIRI, '3', '1');
                  -- por cada lnPorts
                  FOR p IN 0 .. lnPorts - 1 LOOP
                     -- busca match lnPorts
                     IF lnItem - p = fnBuscarSeg(lvIRI, lvIRIMatch, lnItem - p) THEN
                        -- se encontro un pv que es confirmado en una ruta de lnPorts
                        lvPvNoAvi_IRI := lvPvNoAvi_IRI || ';' ||
                                         fvVectorItem(lrIRI.udv_tx_vpv,
                                                      lnItem,
                                                      ';');
                        EXIT;
                     END IF;
                  
                  END LOOP;
               
               END IF;
            
            END LOOP;
         
            -- elimina ; al inicio
            lvPvNoAvi_IRI := LTRIM(lvPvNoAvi_IRI, ';');
         
            -- registra el PV
            UPDATE udv_udvia
               SET udv_tx_pvnoavi = lvPvNoAvi_IRI
             WHERE ROWID = lrIRI.ROWID;
         
            kBTC.pRealizarCommit;
         
         END IF;
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      -- registro de fin
      kBTC.avErrMsg := 'predictor_PAT';
      kBTC.pSaveMessage(85, lvProgram, FALSE);
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbIRIConsPat;


   -- #############################################################################
   /**
   Marca de unidad incoherente de categorias.
   Funcion que marca las diferencias de categoria mientras un vehiculo hace uso de via.
   %author RHE / RHernandez
   %version 1A/ 31mar'09/ RHE
   %version 2A/ 22oct'09/ RHE > Se incluyen iri los valores 2 y 3 en el cursor lcVEHs
                                Se incorpora la clasificación de incoherencia de categorías.
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbMarcaUICat RETURN BOOLEAN IS
      CURSOR lcVEHs IS
         SELECT udv_tx_vcategoria, udv_tx_viri, ROWID
           FROM udv_udvia a
          WHERE instr(udv_tx_venviocm, '1', 1) <> 0
            AND (instr(udv_tx_viri, '1', 1) <> 0 OR
                 instr(udv_tx_viri, '2', 1) <> 0 OR
                 instr(udv_tx_viri, '3', 1) <> 0);
   
      lvProgram  VARCHAR2(30) := gvcPkg || 'fbMarcaUICat';
      lnPv        PLS_INTEGER;
      lnItem      PLS_INTEGER;
      lvUICat     VARCHAR2(4000);
      lvUICatClas VARCHAR2(4000); --clasificación de la incoherencia de categorías
      lvCatPv     VARCHAR2(1);
      lvCatPvProx VARCHAR2(1);
   
   BEGIN
      kBTC.pInitCommit(2100, lvProgram);
      FOR lrVEH IN lcVEHs LOOP
         -- inicializacion de variable
         lvUICat     := NULL;
         lvUICatClas := NULL;
         lvCatPv     := NULL;
         lvCatPvProx := NULL;
      
         -- cantidad de PV
         lnPv := fnVectorSize(lrVEH.udv_tx_viri, ';');
      
         FOR lnItem IN 1 .. lnPv LOOP
            lvCatPv     := fvVectorItem(lrVEH.udv_tx_vcategoria, lnItem, ';');
            lvCatPvProx := fvVectorItem(lrVEH.udv_tx_vcategoria,
                                        lnItem + 1,
                                        ';');
            IF lvCatPv = lvCatPvProx THEN
               lvUICat := lvUICat || ';1'; --coherencia
            ELSE
               lvUICat := lvUICat || ';0'; --incoherencia
            END IF;
            --clasificación de la coherencia/incoherencia
            IF (lvCatPv = '1' AND lvCatPvProx = '1') THEN
               lvUICatClas := lvUICatClas || ';1';
            ELSIF (lvCatPv = '2' AND lvCatPvProx = '2') THEN
               lvUICatClas := lvUICatClas || ';2';
            ELSIF (lvCatPv = '3' AND lvCatPvProx = '3') THEN
               lvUICatClas := lvUICatClas || ';3';
            ELSIF (lvCatPv = '4' AND lvCatPvProx = '4') THEN
               lvUICatClas := lvUICatClas || ';4';
            ELSIF (lvCatPv = '1' AND lvCatPvProx = '2') OR
                  (lvCatPv = '2' AND lvCatPvProx = '1') THEN
               lvUICatClas := lvUICatClas || ';5';
            ELSIF (lvCatPv = '2' AND lvCatPvProx = '3') OR
                  (lvCatPv = '3' AND lvCatPvProx = '2') THEN
               lvUICatClas := lvUICatClas || ';6';
            ELSIF (lvCatPv = '1' AND lvCatPvProx = '4') OR
                  (lvCatPv = '4' AND lvCatPvProx = '1') THEN
               lvUICatClas := lvUICatClas || ';7';
            ELSE
               lvUICatClas := lvUICatClas || ';0';
            END IF;
         
         END LOOP;
      
         -- elimina ; al inicio
         lvUICat     := LTRIM(RTRIM(lvUICat, ';'), ';');
         lvUICatClas := LTRIM(RTRIM(lvUICatClas, ';'), ';');
      
         -- registro de la inconherencia de categoria
         UPDATE udv_udvia
            SET udv_tx_vuicategoria     = lvUICat,
                udv_tx_vuiclascategoria = lvUICatClas
          WHERE ROWID = lrVEH.ROWID;
      
         kBTC.pRealizarCommit;
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbMarcaUICat;

   -- #############################################################################
   /**
   Marca de unidad incoherente de longitud total.
   Funcion que marca las diferencias de longitud total mientras un vehiculo hace uso de via.
   %author RHernandez
   %version 1A/ 20oct'09/ RHernandez
   %return TRUE  Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbMarcaUILon RETURN BOOLEAN IS
      CURSOR lcVEHs IS
         SELECT udv_tx_patente, udv_tx_largototal, udv_tx_viri, ROWID
           FROM udv_udvia
          WHERE instr(udv_tx_venviocm, '1', 1) <> 0
            AND (instr(udv_tx_viri, '1', 1) <> 0 OR
                 instr(udv_tx_viri, '2', 1) <> 0 OR
                 instr(udv_tx_viri, '3', 1) <> 0);
   
      lvProgram   VARCHAR2(30) := gvcPkg || 'fbMarcaUILon';
      lnPv         PLS_INTEGER;
      lnItem       PLS_INTEGER;
      lvUILon      VARCHAR2(4000);
      lvMeanLon    VARCHAR2(4000);
      lnDisper     NUMBER;
      lnMedia      NUMBER;
      lnUmbralDesv NUMBER := kCNF.fnRetrieve('maximaDispersionLargos', 30);
   
   BEGIN
      kBTC.pInitCommit(2100, lvProgram);
      FOR lrVEH IN lcVEHs LOOP
         -- inicializacion de variable
         lvUILon   := NULL;
         lvMeanLon := NULL;
         lnMedia   := NULL;
         lnDisper  := NULL;
      
         -- cantidad de PV
         lnPv := fnVectorSize(lrVEH.udv_tx_viri, ';');
      
         FOR lnItem IN 1 .. lnPv LOOP
            lnMedia := (to_number(fvVectorItem(lrVEH.udv_tx_largototal,
                                               lnItem,
                                               ';')) +
                       to_number(fvVectorItem(lrVEH.udv_tx_largototal,
                                               lnItem + 1,
                                               ';'))) / 2;
            IF lnMedia IS NULL OR lnMedia = 0 THEN
               dbms_output.put_line(lrVEH.udv_tx_patente);
            END IF;
         
            IF lnMedia <> 0 THEN
               lnDisper := ABS((fvVectorItem(lrVEH.udv_tx_largototal,
                                             lnItem,
                                             ';') - lnMedia) * 100 / lnMedia);
            ELSE
               -- fallan dos mediciones consecutivas de longitud
               lnDisper := 100; -- asigna el máximo valor de dispersión
            END IF;
         
            IF lnDisper <= lnUmbralDesv THEN
               lvUILon := lvUILon || ';1'; --coherencia
            ELSE
               lvUILon := lvUILon || ';0'; --incoherencia
            END IF;
         
            lvMeanLon := lvMeanLon || ';' || to_char(round(lnMedia));
         
         END LOOP;
      
         -- elimina ; al inicio
         lvUILon   := LTRIM(RTRIM(lvUILon, ';'), ';');
         lvMeanLon := LTRIM(RTRIM(lvMeanLon, ';'), ';');
      
         -- registro de la inconherencia de categoria
         UPDATE udv_udvia
            SET udv_tx_vuilongitudt = lvUILon, udv_tx_vlongpromedio = lvMeanLon
          WHERE ROWID = lrVEH.ROWID;
      
         kBTC.pRealizarCommit;
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbMarcaUILon;

   -- #############################################################################
   /**
   Marca de unidad incoherente de remolque.
   Funcion que marca las diferencias de uso de remolque mientras un vehiculo hace uso de via.
   %author RHE / RHernandez
   %version 1A/ 29oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbMarcaUIRem RETURN BOOLEAN IS
      CURSOR lcVEHs IS
         SELECT udv_tx_remolque, udv_tx_viri, ROWID
           FROM udv_udvia
          WHERE instr(udv_tx_venviocm, '1', 1) <> 0
            AND (instr(udv_tx_viri, '1', 1) <> 0 OR
                 instr(udv_tx_viri, '2', 1) <> 0 OR
                 instr(udv_tx_viri, '3', 1) <> 0);
   
      lvProgram VARCHAR2(30) := gvcPkg || 'fbMarcaUICat';
      lnPv       PLS_INTEGER;
      lnItem     PLS_INTEGER;
      lvUIRem    VARCHAR2(4000);
   
   BEGIN
      kBTC.pInitCommit(2100, lvProgram);
      FOR lrVEH IN lcVEHs LOOP
         -- inicializacion de variable
         lvUIRem := NULL;
      
         -- cantidad de PV
         lnPv := fnVectorSize(lrVEH.udv_tx_viri, ';');
      
         FOR lnItem IN 1 .. lnPv LOOP
            IF fvVectorItem(lrVEH.udv_tx_remolque, lnItem, ';') =
               fvVectorItem(lrVEH.udv_tx_remolque, lnItem + 1, ';') THEN
               lvUIRem := lvUIRem || ';' || '1'; --coherencia
            ELSE
               lvUIRem := lvUIRem || ';' || '0'; --incoherencia
            END IF;
         END LOOP;
      
         -- elimina ; al inicio
         lvUIRem := LTRIM(RTRIM(lvUIRem, ';'), ';');
      
         -- registro de la inconherencia de categoria
         UPDATE udv_udvia
            SET udv_tx_vuiremolque = lvUIRem
          WHERE ROWID = lrVEH.ROWID;
      
         kBTC.pRealizarCommit;
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbMarcaUIRem;

   /*##############################################################################
   %desc Prediccion de categorias segun IRI a partir de la informacion de ruta inicial
         conformada entre n (CNF=cantidadPorticosIRI) porticos.
   %rtrn TRUE si el proceso termina satisfactoriamente
   %rtrn FALSE si el proceso se ejecuta con error
   %autr RHE / Raciel Hernandez
   %vers 1A / 31mar'09 / RHE
   */
   FUNCTION fbIRIConsCat RETURN BOOLEAN IS
      CURSOR lcPVs IS
         SELECT udv_tx_pvnoavi,
                udv_tx_vpv,
                udv_tx_vcategoria,
                udv_tx_vuiclascategoria,
                udv_tx_viri,
                udv_tx_vuicategoria,
                ROWID
           FROM udv_udvia u
          WHERE udv_tx_pvnoavi IS NOT NULL
            AND udv_tx_vuicategoria IS NOT NULL;
      lvProgram VARCHAR2(30) := gvcPkg || 'fbIRIConsCat';
      lnCantPvs  PLS_INTEGER;
      lnTotalPvs PLS_INTEGER;
      lnPosPv    PLS_INTEGER;
      lnPorts    PLS_INTEGER := kCNF.fnRetrieve('cantidadPorticosIRI', 2);
      lvCATPv    VARCHAR2(4);
      lvPvnoAvi  VARCHAR2(18);
      lvIRICat   VARCHAR2(4000);
      lvUICMatch VARCHAR2(50) := fvVectorX('1', lnPorts - 1, ';');
      lbMatch    BOOLEAN;
   BEGIN
      kBTC.pInitCommit(1700, lvProgram);
   
      FOR lrPV IN lcPVs LOOP
         --construye el match a buscar
         lvPvnoAvi := NULL;
         lvCATPv   := NULL;
         lvIRICat  := NULL;
      
         -- cantidad de pv a consolidar
         lnCantPVs  := fnVectorSize(lrPV.udv_tx_pvnoavi);
         lnTotalPvs := fnVectorSize(lrPV.udv_tx_vpv);
      
         IF INSTR(lrPV.udv_tx_vuicategoria, '0', 1) = 0 THEN
            -- no hay incoherencia de categoria en todo el uso de via
            -- categoria asociada al pv
            lvCATPv := fvVectorItem(lrPv.udv_tx_vcategoria, 1, ';');
            -- categoria segun IRI
            lvIRICat := fvVectorX(lvCATPv, lnCantPVs);
         ELSE
            -- hay incoherencia de categoria en el uso de via
            -- recorremos cada pv
            FOR i IN 1 .. lnCantPVs LOOP
               -- obtiene el pv para el cual IRI tiene prediccion de patente
               lvPvnoAvi := fvVectorItem(lrPV.udv_tx_pvnoavi, i);
               -- busca la posicion del pv
               lnPosPV := fnSearhItem(lrPV.udv_tx_vpv, lvPvnoAvi);
               -- inicializa variable match
               lbMatch := FALSE;
               -- por cada lnPorts
               FOR p IN 0 .. lnPorts - 1 LOOP
                  IF lnPosPV - p = fnBuscarSeg(lrPV.udv_tx_vuicategoria,
                                               lvUICMatch,
                                               lnPosPV - p) THEN
                     -- la categoria es la misma en lnPorts porticos ruta
                     lvCATPv := fvVectorItem(lrPv.udv_tx_vcategoria,
                                             lnPosPV,
                                             ';');
                     -- marca de encontrado
                     lbMatch := TRUE;
                     EXIT;
                  END IF;
               
               END LOOP;
            
               IF NOT lbMatch THEN
                  -- si la categoria no es la misma en lnPorts porticos IRI 
                  -- tomar la clasificación de la incoherencia
                  IF lnPosPV = lnTotalPvs THEN
                     lvCATPv := fvVectorItem(lrPv.udv_tx_vuiclascategoria,
                                             lnPosPV - 1,
                                             ';');
                  
                  ELSE
                     lvCATPv := fvVectorItem(lrPv.udv_tx_vuiclascategoria,
                                             lnPosPV,
                                             ';');
                  END IF;
               END IF;
            
               lvIRICat := lvIRICat || ';' || lvCATPv;
            
            END LOOP;
         
            -- elimina ; al inicio
            lvIRICat := LTRIM(lvIRICat, ';');
         END IF;
      
         -- guarda la categoria en ruta
         UPDATE udv_udvia
            SET udv_tx_pvnoavicat = lvIRICat
          WHERE ROWID = lrPV.ROWID;
         kBTC.pRealizarCommit;
      
      END LOOP;
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbIRIConsCat;

   -- #############################################################################
   /**
   Obtiene las características comerciales del vehículo.
   Funcion que actualiza los datos comerciales del vehículo a partir de cu_habilitaciones.
   %author RHE / Raciel Hernandez
   %version 1A/ 25oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente
   %return FALSE Si el proceso se ejecuta con error
   */
   FUNCTION fbObtenerDCVeh RETURN BOOLEAN IS
   
      lvcPrg CONSTANT VARCHAR2(30) := gvcPkg || 'fbObtenerDCVeh';
   
      lbSOP BOOLEAN := FALSE; --Indica si se conectó a cu_habilitaciones de SOP
   
   BEGIN
      --construye la tabla local de cu_matriculas de SOP
      BEGIN
         --baja la tabla cu_matriculas de sop (tabla suministro)
         EXECUTE IMMEDIATE 'CREATE TABLE tl_cu_matriculas2 PCTFREE 0 NOLOGGING AS SELECT txt_matricula, nvl(cod_categoria, 0) cod_categoria, nvl(cod_categoria_excepcional, 0) cod_categoria_excepcional FROM cu_matriculas';
         --si ejecutó el paso anterior
         lbSOP := TRUE;
         --elimina la tabla cu_matriculas local
         EXECUTE IMMEDIATE 'DROP TABLE tl_cu_matriculas';
         --reconstruye la tabla local cu_matriculas a partir de la tabla suministro
         EXECUTE IMMEDIATE 'CREATE TABLE tl_cu_matriculas PCTFREE 0 NOLOGGING AS SELECT * FROM tl_cu_matriculas2';
         --reconstruye el índice
         EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX tlcu_idx_matricula ON tl_cu_matriculas(txt_matricula)';
         --elimina la tabla suministro
         EXECUTE IMMEDIATE 'DROP TABLE tl_cu_matriculas2';
      
      EXCEPTION
         WHEN geDBIsDown OR geDBIsGoingDown THEN
            --> BD CRM no disponible
            kBTC.avErrMsg := 'tl_cu_matriculas';
            kBTC.pSaveMessage(199, lvcPrg, TRUE); --199?
      END;
   
      --cierra el dblink con el SOP
      IF lbSOP THEN
         EXECUTE IMMEDIATE 'ALTER SESSION CLOSE DATABASE LINK sop';
      END IF;
   
      --Actualiza los datos de categoría del vehículo en udv_udvia
      EXECUTE IMMEDIATE 'UPDATE udv_udvia SET (udv_tx_categoria, udv_tx_categoria2) = (SELECT cod_categoria, cod_categoria_excepcional FROM tl_cu_matriculas WHERE txt_matricula = udv_tx_patente)';
      COMMIT;
   
      --Asignar categoría cero a los casos que no aparecen en cu_matriculas
      EXECUTE IMMEDIATE 'UPDATE udv_udvia SET udv_tx_categoria = 0, udv_tx_categoria2 = 0 WHERE udv_tx_categoria IS NULL';
      COMMIT;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbObtenerDCVeh;

   -- #############################################################################
   /**
   Aplica las reglas de negocios de categorías.
   Funcion que obtiene la categoría a consolidar según la categoria IRI y las reglas de negocios definidas en RNC.
   %author RHE / Raciel Hernandez
   %version 1A/ 25oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente
   %return FALSE Si el proceso se ejecuta con error
   */

   FUNCTION fbApplyRNC RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(30) := gvcPkg || 'fbApplyRNC';
      CURSOR lcUDVs IS
         SELECT udv_tx_vpv,
                udv_tx_pvnoavi,
                udv_tx_pvnoavicat,
                udv_tx_categoria,
                udv_tx_categoria2,
                udv_tx_remolque,
                udv_tx_vlongpromedio,
                udv_tx_vuiremolque,
                udv_tx_vuilongitudt,
                ROWID
           FROM udv_udvia u
          WHERE udv_tx_pvnoavicat IS NOT NULL;
   
      lnPorts          PLS_INTEGER := kCNF.fnRetrieve('cantidadPorticosIRI', 2);
      lvCatRNC         VARCHAR2(5);
      lvAplRNC         VARCHAR2(5);
      lvIDRNC          VARCHAR2(5);
      lvCatRNCudv      VARCHAR2(3950);
      lvAplRNCudv      VARCHAR2(3950);
      lvIDRNCudv       VARCHAR2(3950);
      lvCantPV         PLS_INTEGER;
      lvTotalPV        PLS_INTEGER;
      lvCatIRI         VARCHAR2(1);
      lvPv             VARCHAR2(25);
      lnItemPv         PLS_INTEGER;
      lvRemolque       VARCHAR2(1);
      lnUmbralLong     PLS_INTEGER := kCNF.fnRetrieve('umbralLargoTotal', 1000);
      lnLongitudProm   NUMBER;
      lvUIRemolque     VARCHAR2(1);
      lvUILong         VARCHAR2(1);
      lvLargoSupUmbral VARCHAR2(1);
      lvUIMatch        VARCHAR2(50) := fvVectorX('1', lnPorts - 1, ';');
   
   BEGIN
      kBTC.pInitCommit(1700, lvcPrg);
   
      FOR lrUDV IN lcUDVs LOOP
         lvCatRNCudv := NULL;
         lvIDRNCudv  := NULL;
         lvAplRNCudv := NULL;
      
         lvCantPV  := fnVectorSize(lrUDV.udv_tx_pvnoavi); --pv a consolidar
         lvTotalPV := fnVectorSize(lrUDV.udv_tx_vpv); --total de pv
      
         FOR i IN 1 .. lvCantPV LOOP
            lvCatRNC         := NULL;
            lvUIRemolque     := NULL;
            lvUILong         := NULL;
            lvRemolque       := NULL;
            lvLargoSupUmbral := NULL;
         
            lvPv       := fvVectorItem(lrUDV.udv_tx_pvnoavi, i);
            lnItemPv   := fnSearhItem(lrUDV.udv_tx_vpv, lvPv);
            lvCatIRI   := fvVectorItem(lrUDV.udv_tx_pvnoavicat, i);
            lvRemolque := fvVectorItem(lrUDV.udv_tx_remolque, lnItemPv);
         
            --búsqueda de coherencia de remolque en ruta de lnPorts
            IF instr(lrUDV.udv_tx_vuiremolque, '0', 1) = 0 THEN
               --no hay incoherencia en el uso de remolque
               lvUIRemolque := '1';
            ELSE
               FOR p IN 0 .. lnPorts - 1 LOOP
                  IF lnItemPv - p = fnBuscarSeg(lrUDV.udv_tx_vuiremolque,
                                                lvUIMatch,
                                                lnItemPv - p) THEN
                     -- el uso de remolque es el mismo en lnPorts porticos ruta
                     lvUIRemolque := '1';
                     EXIT;
                  END IF;
               END LOOP;
            END IF;
         
            --búsqueda de coherencia de longitud en ruta de lnPorts
            IF instr(lrUDV.udv_tx_vuilongitudt, '0', 1) = 0 THEN
               --no hay incoherencia en el uso de remolque
               lvUILong := '1';
            ELSE
               FOR p IN 0 .. lnPorts - 1 LOOP
                  IF lnItemPv - p = fnBuscarSeg(lrUDV.udv_tx_vuilongitudt,
                                                lvUIMatch,
                                                lnItemPv - p) THEN
                     -- el uso de remolque es el mismo en lnPorts porticos ruta
                     lvUILong := '1';
                     EXIT;
                  END IF;
               END LOOP;
            END IF;
         
            --valor de longigud total promedio dos porticos consecutivos
            IF lnItemPv <> lvTotalPV THEN
               lnLongitudProm := to_number(fvVectorItem(lrUDV.udv_tx_vlongpromedio,
                                                        lnItemPv));
            ELSE
               lnLongitudProm := to_number(fvVectorItem(lrUDV.udv_tx_vlongpromedio,
                                                        lnItemPv - 1));
            END IF;
         
            --traducción remolque
            IF lvRemolque = '1' AND lvUIRemolque = '1' THEN
               lvRemolque := 'V';
            ELSIF lvRemolque = '0' AND lvUIRemolque = '1' THEN
               lvRemolque := 'F';
            ELSE
               lvRemolque := NULL;
            END IF;
            --traducción largototal
            IF lnLongitudProm > lnUmbralLong AND lvUILong = '1' THEN
               lvLargoSupUmbral := 'V';
            ELSIF lnLongitudProm < lnUmbralLong AND lvUILong = '1' THEN
               lvLargoSupUmbral := 'F';
            ELSE
               lvLargoSupUmbral := NULL;
            END IF;
         
            BEGIN
               SELECT to_char(rnc_id_regla),
                      NVL(rnc_nm_categoria_consolidar, 0),
                      rnc_ck_activa
                 INTO lvIDRNC, lvCatRNC, lvAplRNC
                 FROM rnc
                WHERE rnc_lv_categoria_iri = lvCatIRI
                  AND rnc_lv_categoria1 = lrUDV.udv_tx_categoria
                  AND rnc_lv_categoria2 = lrUDV.udv_tx_categoria2
                  AND rnc_ck_remolque = lvRemolque
                  AND rnc_ck_supera_umbral_largo = lvLargoSupUmbral;
            
               IF lvCatRNC = '0' THEN
                  lvCatRNC := 'null';
               END IF;
            
            EXCEPTION
               WHEN no_data_found THEN
                  lvCatRNC := 'null';
                  lvIDRNC  := 'null';
                  lvAplRNC := 'null';
            END;
         
            lvCatRNCudv := lvCatRNCudv || ';' || lvCatRNC;
            lvIDRNCudv  := lvIDRNCudv || ';' || lvIDRNC;
            lvAplRNCudv := lvAplRNCudv || ';' || lvAplRNC;
         
         
         END LOOP;
      
         --actualiza el vector con la categoría a aplicar
         lvCatRNCudv := LTRIM(lvCatRNCudv, ';');
         lvIDRNCudv  := LTRIM(lvIDRNCudv, ';');
         lvAplRNCudv := LTRIM(lvAplRNCudv, ';');
      
         UPDATE udv_udvia
            SET udv_tx_categoria_rnc = lvCatRNCudv,
                udv_tx_regla_rnc     = lvIDRNCudv,
                udv_tx_aplicar_rnc   = lvAplRNCudv
          WHERE ROWID = lrUDV.ROWID;
      
         kBTC.pRealizarCommit;
      
      END LOOP;
   
      kbtc.pFinalizarCommit;
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbApplyRNC;

   -- #############################################################################
   /**
   Actualiza la lista local de fraudes segun proceso de infractores.
   Funcion que actualiza la lista de matriculas detectadas como fraudes en el proceso de infractores.
   %author RHE / Raciel Hernandez
   %version 1A/ 27may'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente
   %return FALSE Si el proceso se ejecuta con error
   */
   FUNCTION fbActualizarEXM RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(30) := gvcPkg || 'fbActualizarEXM';
   
      CURSOR lcINFs IS
         SELECT matricula FROM zin_lista_imp;
   
      CURSOR lcAVIs IS
         SELECT txt_sop_matricula FROM dl_cm_msj_matriculas;
   
      lvPatente VARCHAR2(12);
      lbCRM     BOOLEAN := FALSE;
      lbSOP     BOOLEAN := FALSE;
   
   BEGIN
      --carga excepciones de infractores
      BEGIN
         --abre el cursor, con eso se verifica si la base remota esta OK
         OPEN lcINFs;
         lbCRM := TRUE;
      
         --elimina los registros actualmente cargados
         DELETE exm WHERE exm_lv_origen = 'INF';
      
         --carga los registros nuevos
         LOOP
            FETCH lcINFs
               INTO lvPatente;
            EXIT WHEN lcINFs%NOTFOUND;
         
            BEGIN
               INSERT INTO exm
                  (exm_tx_patente, exm_lv_origen)
               VALUES
                  (lvPatente, 'INF');
            EXCEPTION
               WHEN dup_val_on_index THEN
                  --> la patente ya existe en EXM
                  NULL;
            END;
         
         END LOOP;
      
         -- cierra el cursor
         CLOSE lcINFs;
      
      EXCEPTION
         WHEN geDBIsDown OR geDBIsGoingDown THEN
            --> BD CRM no disponible
            kBTC.avErrMsg := 'lzin_lista_imp';
            kBTC.pSaveMessage(199, lvcPrg, TRUE);
      END;
   
      --carga excepciones de AVI
      BEGIN
         --abre el cursor, con eso se verifica si la base remota esta OK
         OPEN lcAVIs;
         lbSOP := TRUE;
      
         --elimina los registros actualmente cargados
         DELETE exm WHERE exm_lv_origen = 'AVI';
      
         --carga los registros neuvos
         LOOP
            FETCH lcAVIs
               INTO lvPatente;
            EXIT WHEN lcAVIs%NOTFOUND;
         
            BEGIN
               INSERT INTO exm
                  (exm_tx_patente, exm_lv_origen)
               VALUES
                  (lvPatente, 'AVI');
            EXCEPTION
               WHEN dup_val_on_index THEN
                  --> la patente ya existe en EXM
                  NULL;
            END;
         
         END LOOP;
      
         -- cierra el cursor
         CLOSE lcAVIs;
      
      EXCEPTION
         WHEN geDBIsDown OR geDBIsGoingDown THEN
            --> BD CRM no disponible
            kBTC.avErrMsg := 'lzin_lista_imp';
            kBTC.pSaveMessage(199, lvcPrg, TRUE);
      END;
   
      -- graba
      COMMIT;
   
      -- cierra los DBL
      IF lbSOP THEN
         EXECUTE IMMEDIATE 'ALTER SESSION CLOSE DATABASE LINK sop';
      END IF;
      IF lbCRM THEN
         EXECUTE IMMEDIATE 'ALTER SESSION CLOSE DATABASE LINK crm';
      END IF;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbActualizarEXM;


   -- #############################################################################
   /**
   Identificador de excepciones IRI.
   Funcion que identifica las excepciones del modelo de prediccion IRI para la consolidacion en AVI.
   %author RHE / Raciel Hernandez
   %version 1A / 31mar'09 / RHE
   %version 1B / 29may'09 / RHE > cambio para que considere la nueva tabla EXM en vez
                 de las dos tablas temporales LDL_CM_MSJ_MATRICULAS y LZIN_LISTA_IMP,
                 cambio en la consulta a EXI, buscando registros EXI vigente para la
                 fecha de transito en vez de vigentes a la fecha de ejecucion, cambio
                 de condicion de exclusion de patentes invalidas, desde codigo en duro
                 a tabla estandar CORE TRD.
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbGestionExi RETURN BOOLEAN IS
      CURSOR lcVLDs IS
         SELECT udv_tx_patente, udv_dt_fecha, ROWID
           FROM udv_udvia
          WHERE udv_nm_excepcion IS NULL;
      lnRlg      PLS_INTEGER;
      lnValid    PLS_INTEGER;
      lvProgram VARCHAR2(30) := gvcPkg || 'fbGestionExcep';
   BEGIN
      -- registra inicio
      kBTC.avErrMsg := 'excepciones_IRI';
      kBTC.pSaveMessage(142, lvProgram, TRUE);
   
      -- actualiza la tabla de excepciones AVI e INF
      IF NOT fbActualizarEXM THEN
         kBTC.pSaveMessage(1, lvProgram, FALSE);
      END IF;
   
      -- fija la frecuencia de COMMIT
      kBTC.pInitCommit(12500, lvProgram);
   
      -- verifica excepcionalidad para cada patente
      FOR lrVLD IN lcVLDs LOOP
         lnValid := 0; --supone que no es una excepcion IRI
         lnRlg   := NULL;
         IF LENGTH(lrVLD.udv_tx_patente) <> 6 THEN
            lnValid := 2; --marca de excepcion generica (concentra patentes chilenas)
         ELSE
            BEGIN
               SELECT DECODE(exm_lv_origen,
                             'INF',
                             3, --> excepcion por tener antecedentes de fraude
                             'AVI',
                             4, --> excepcion por lista de gestion de matriculas de la AVI
                             99) --> excepcion generica no controlada
                 INTO lnValid
                 FROM exm
                WHERE exm_tx_patente = lrVLD.udv_tx_patente;
            EXCEPTION
               WHEN no_data_found THEN
                  BEGIN
                     SELECT 7 --excepcion especial IRI
                       INTO lnValid
                       FROM exi
                      WHERE exi_tx_ocrpatente = lrVLD.udv_tx_patente
                        AND exi_dt_registro <= lrVLD.udv_dt_fecha
                        AND exi_dt_deshabilitacion >= lrVLD.udv_dt_fecha
                        AND ROWNUM = 1;
                  EXCEPTION
                     WHEN no_data_found THEN
                        IF NOT
                            kRGL.fbValidarPatente(lrVLD.udv_tx_patente, lnRlg) THEN
                           lnValid := 5; --excepcion de patente no valida
                        ELSIF NVL(kTRD.fvTraducirValor(lnRlg, 506), 'NO') <> 'SI' THEN
                           lnValid := 6; --> excepciones patente con formato no chileno
                        END IF;
                  END;
            END;
         END IF;
      
         -- graba el motivo de excepcion
         UPDATE udv_udvia
            SET udv_nm_excepcion = lnValid
          WHERE ROWID = lrVLD.ROWID;
      
         kBTC.pRealizarCommit;
      
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      -- registro de fin
      kBTC.avErrMsg := 'excepciones_IRI';
      kBTC.pSaveMessage(85, lvProgram, TRUE);
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbGestionExi;

   -- #############################################################################
   /**
   Determina si dos pórticos son consecutivos entre sí.
   %author RHE / Raciel Hernandez
   %version 1A / 27oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbIsConsecutivePorticos(pvPort01 VARCHAR2, pvPort02 VARCHAR2) RETURN BOOLEAN IS
   
      lvPrg VARCHAR2(30) := gvcPkg || 'fbIsConsecutivePorticos';
   
      CURSOR lcSCNs IS
         SELECT '<' || a.trm_tx_vrecsn || ';><' || a.trm_tx_vrecns || ';>' AS Secuencia
           FROM trm_tramo a
          WHERE a.trm_tx_nombre IN ('R5C', 'GVC');
   
      lvPort01  VARCHAR2(4) := pvPort01;
      lvPort02  VARCHAR2(4) := pvPort02;
      lvIRIPort VARCHAR2(10);
   
   BEGIN
   
      lvIRIPort := lvPort01 || ';' || lvPort02 || ';';
   
      FOR lrSCN IN lcSCNs LOOP
         IF instr(lrSCN.Secuencia, lvIRIPort, 1) <> 0 THEN
            RETURN TRUE;
         END IF;
      END LOOP;
   
      RETURN FALSE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbIsConsecutivePorticos;

   -- #############################################################################
   /**
   Secuencia IRI faltante entre pórticos.
   Obtiene la secuencia faltante entre pórticos que hacen IRI y no son consecutivos.
   (Aplicable sólo a pórticos no consecutivos)
   %author RHE / Raciel Hernandez
   %version 1A / 27oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbSecuenciaIRI_1nX1(pvPort01   IN VARCHAR2,
                                pvPort02   IN VARCHAR2,
                                pvIRI_1nX1 OUT VARCHAR2) RETURN BOOLEAN IS
   
      lvPrg VARCHAR2(30) := gvcPkg || 'fbSecuenciaIRI_1nX1';
   
      CURSOR lcSCNs IS
         SELECT '<;' || trm_tx_vrecsn || ';><;' || trm_tx_vrecns || ';>' AS Secuencia
           FROM trm_tramo
          WHERE trm_tx_nombre IN ('R5C', 'GVC');
   
      lvPort01 VARCHAR2(4) := ';' || pvPort01 || ';';
      lvPort02 VARCHAR2(4) := ';' || pvPort02 || ';';
      lnPos01  PLS_INTEGER;
      lnPos02  PLS_INTEGER;
   
      lvIRI_1nX1 VARCHAR2(300);
   
   BEGIN
      FOR lrSCN IN lcSCNs LOOP
         lnPos01 := 0;
         lnPos02 := 0;
         lnPos01 := instr(lrSCN.Secuencia, lvPort01, 1);
         lnPos02 := instr(lrSCN.Secuencia, lvPort02, lnPos01);
      
         IF lnPos01 <> 0 AND lnPos02 <> 0 AND lnPos01 <> lnPos02 THEN
            lnPos01    := lnPos01 + length(lvPort01) - 2 + 1;
            lvIRI_1nX1 := substr(lrSCN.Secuencia,
                                 lnPos01,
                                 lnPos02 - lnPos01 + 1);
            lvIRI_1nX1 := ltrim(lvIRI_1nX1, ';');
            lvIRI_1nX1 := rtrim(lvIRI_1nX1, ';');
            pvIRI_1nX1 := lvIRI_1nX1;
         
            RETURN TRUE;
         
         END IF;
      
      END LOOP;
   
      RETURN FALSE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvPrg, 'STD', TRUE);
         RETURN FALSE;
      
   END fbSecuenciaIRI_1nX1;

   -- #############################################################################
   /**
   Marca la región de búsqueda IRC.
   Busca y marca la región de búsqueda para el modelo IRC.
   %author RHE / Raciel Hernandez
   %version 1A / 27oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbMarcarRegionIRC RETURN BOOLEAN IS
   
      lvPrg VARCHAR2(30) := gvcPkg || 'fbRegionIRC';
   
      CURSOR lcIRIs IS
         SELECT udv_tx_viri, udv_tx_vrecorrido, ROWID
           FROM udv_udvia a
          WHERE udv_tx_viri IS NOT NULL
            AND instr(udv_tx_viri, '1', 1) <> 0;
   
      lvIRI_1nX1       VARCHAR2(5000);
      lvIRI            VARCHAR2(20);
      lvRecorrido_1nX1 VARCHAR2(5000);
      lvSecXPort       VARCHAR2(5000);
   
      lnSize    PLS_INTEGER := 0;
      lnAdd     PLS_INTEGER := 0;
      lvPort01  VARCHAR2(5);
      lvPort02  VARCHAR2(5);
      lvAdXMark VARCHAR2(500);
   
   BEGIN
      kbtc.pInitCommit(5000, lvPrg);
      FOR lrIRI IN lcIRIs LOOP
         lvPort01         := NULL;
         lvPort02         := NULL;
         lvIRI_1nX1       := NULL;
         lvRecorrido_1nX1 := NULL;
         lnSize           := fnVectorSize(lrIRI.udv_tx_viri) + 1;
      
         FOR i IN 1 .. lnSize LOOP
            lvSecXPort       := NULL;
            lnAdd            := NULL;
            lvPort01         := fvVectorItem(lrIRI.udv_tx_vrecorrido, i);
            lvPort02         := fvVectorItem(lrIRI.udv_tx_vrecorrido, i + 1);
            lvRecorrido_1nX1 := lvRecorrido_1nX1 || ';' || lvPort01;
            lvIRI            := fvVectorItem(lrIRI.udv_tx_viri, i);
            lvIRI_1nX1       := lvIRI_1nX1 || ';' || lvIRI;
         
            IF lvIRI IN ('1', 2, 3) THEN
               --// evaluar a futuro todos los IRI <> 0
               IF NOT fbIsConsecutivePorticos(lvPort01, lvPort02) THEN
                  IF fbSecuenciaIRI_1nX1(lvPort01, lvPort02, lvSecXPort) THEN
                     lvRecorrido_1nX1 := lvRecorrido_1nX1 || ';' || lvSecXPort;
                     lnAdd            := fnVectorSize(lvSecXPort, ';');
                     lvAdXMark        := fvVectorX('x', lnAdd);
                     lvIRI_1nX1       := lvIRI_1nX1 || ';' || lvAdXMark;
                  END IF;
               ELSE
                  NULL;
               END IF;
            END IF;
         END LOOP;
      
         lvIRI_1nX1       := ltrim(lvIRI_1nX1, ';');
         lvIRI_1nX1       := rtrim(lvIRI_1nX1, ';');
         lvRecorrido_1nX1 := ltrim(lvRecorrido_1nX1, ';');
      
         UPDATE udv_udvia a
            SET udv_tx_viri_1nx1       = lvIRI_1nX1,
                udv_tx_vrecorrido_1nx1 = lvRecorrido_1nX1
          WHERE ROWID = lrIRI.ROWID;
      
         kBTC.pRealizarCommit;
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvPrg, 'STD', TRUE);
         RETURN FALSE;
      
   END fbMarcarRegionIRC;

   -- #############################################################################
   /**
   Conformar IRC.
   Función que conforma la información de rutas corregida.
   %author RHE / Raciel Hernandez
   %version 1A / 27oct'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbConformIRC RETURN BOOLEAN IS
   
      lvPrg VARCHAR2(30) := gvcPkg || 'fbConformIRC';
   
      CURSOR lcIRIs IS
         SELECT udv_tx_patente,
                udv_dt_fecha,
                udv_tx_vpv,
                udv_tx_vrecorrido,
                udv_tx_vinterval,
                udv_tx_vcategoria,
                udv_tx_vvolumen,
                udv_tx_vvelocidad,
                udv_tx_viri,
                udv_tx_viri_1nx1,
                udv_tx_vrecorrido_1nx1,
                udv_tx_matriculaocrtras,
                ROWID
           FROM udv_udvia
          WHERE instr(udv_tx_viri_1nx1, 'x', 1) <> 0;
   
      lvIRI_1nX1       VARCHAR2(5000);
      lvIRI_XItem      VARCHAR2(20);
      lvRecorrido_1nX1 VARCHAR2(5000);
      lvSecXPort       VARCHAR2(5000);
      lnItem           PLS_INTEGER;
      lnItemIRI        PLS_INTEGER;
      lnPortico        PLS_INTEGER;
      lvPatOCRtras     VARCHAR2(12);
      lvSimilaridadRVR VARCHAR2(6);
   
      ldIntvInf TIMESTAMP(4);
      ldIntvSup TIMESTAMP(4);
      lvHhmmss  VARCHAR2(8);
      lvSeconds PLS_INTEGER;
   
      lvIRC         VARCHAR2(5000);
      lvComposer    VARCHAR2(2);
      lvPv          VARCHAR2(18);
      lvMat         VARCHAR2(20);
      lvMatComposer VARCHAR2(5000);
      lvPvComposer  VARCHAR2(5000);
   
      lvPatSimilar01 VARCHAR2(20);
      lvPatSimilar02 VARCHAR2(20);
      lvPatSimilar03 VARCHAR2(20);
      lvPatSimilar04 VARCHAR2(20);
      lvPatSimilar05 VARCHAR2(20);
      lvPatSimilar06 VARCHAR2(20);
   
      i PLS_INTEGER;
      j PLS_INTEGER;
   
   BEGIN
   
      --EXECUTE IMMEDIATE 'CREATE INDEX drop_idx_portfechamat ON pvd_pvdmcero(pvd_portico,pvd_fecha, pvd_matricula)';
      EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''dd/mm/yyyy hh24:mi:ss''';
   
      kbtc.pInitCommit(200, lvPrg);
   
      FOR lrIRI IN lcIRIs LOOP
         lvIRI_XItem   := NULL;
         lnItemIRI     := 0;
         lvMatComposer := NULL;
         lvPvComposer  := NULL;
         lvIRC         := NULL;
         j             := 0;
      
         lnItem := ksck_mtrx.fnVectorSize(lrIRI.udv_tx_viri_1nx1, ';');
      
         lvPatSimilar01 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 1);
         lvPatSimilar02 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 2);
         lvPatSimilar03 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 3);
         lvPatSimilar04 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 4);
         lvPatSimilar05 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 5);
         lvPatSimilar06 := kstr.fvReeplaceXChar(lrIRI.udv_tx_patente, '%', 6);
      
         FOR i IN 1 .. lnItem LOOP
            lvComposer  := NULL;
            lvIRI_XItem := fvVectorItem(lrIRI.udv_tx_viri_1nx1, i);
            lvPv        := fvVectorItem(lrIRI.udv_tx_vpv, i - j);
            --lvSimilaridadRVR := fvVectorItem(lrIRI.udv_tx_vsimilaridadrvr, i);
         
            IF lvIRI_XItem = 'x' THEN
               j := j + 1;
               /*
               AND (lvSimilaridadRVR <> 'null') THEN
                  --existe información de la patente trasera
               
                  lvPatOCRtras := fvVectorItem(lrIRI.udv_tx_matriculaocrtras, i);
                  
                  IF lvSimilaridadRVR IN ('0', '1', '2') THEN
                     lvComposer := 'k'; --compone la ruta
                  ELSE
                     lvComposer := 'f'; --posible fraude (missmatch rvr)
                  END IF;
                  
                  lvIRC         := lvIRC || ';' || lvComposer;
                  lvMatComposer := lvMatComposer || ';' || lvPatOCRtras;
                  lvPvComposer  := lvPvComposer || ';' || lvPv;
               
                  --no hay información de patente trasera
               ELSIF lvIRI_XItem = 'x' AND lvSimilaridadRVR = 'null'
               */
            
               lnPortico := to_number(fvVectorItem(lrIRI.udv_tx_vrecorrido_1nx1,
                                                   i));
            
               --buscado una región de tiempo para explorar
               --se busca la región 1nX1 en los límites de tiempo indicados por los extremos 1_nX_1
               lvHhmmss  := fvVectorItem(lrIRI.udv_tx_vinterval, lnItemIRI, ';');
               lvSeconds := to_number(fvVectorItem(lvHhmmss, 1, ':')) * 3600 +
                            to_number(fvVectorItem(lvHhmmss, 2, ':')) * 60 +
                            to_number(fvVectorItem(lvHhmmss, 3, ':'));
            
               ldIntvInf := lrIRI.udv_dt_fecha + lvSeconds / 86400;
            
               lvHhmmss  := fvVectorItem(lrIRI.udv_tx_vinterval,
                                         lnItemIRI + 1,
                                         ';');
               lvSeconds := to_number(fvVectorItem(lvHhmmss, 1, ':')) * 3600 +
                            to_number(fvVectorItem(lvHhmmss, 2, ':')) * 60 +
                            to_number(fvVectorItem(lvHhmmss, 3, ':'));
            
               ldIntvSup := lrIRI.udv_dt_fecha + lvSeconds / 86400;
            
               --Busca información para completar la ruta: compose, trasera, abdución, evasión, etc.            
               BEGIN
                  lvComposer := NULL;
               
                  SELECT pvd_matricula, to_char(pvd_pv)
                    INTO lvMat, lvPv
                    FROM pvd_pvdmcero p
                   WHERE pvd_portico = lnPortico
                     AND pvd_fecha > ldIntvInf
                     AND pvd_fecha < ldIntvSup
                     AND pvd_matricula <> lrIRI.udv_tx_patente
                     AND (pvd_matricula LIKE lvPatSimilar01 OR
                         pvd_matricula LIKE lvPatSimilar02 OR
                         pvd_matricula LIKE lvPatSimilar03 OR
                         pvd_matricula LIKE lvPatSimilar04 OR
                         pvd_matricula LIKE lvPatSimilar05 OR
                         pvd_matricula LIKE lvPatSimilar06);
               
                  lvComposer := 'k'; --composer IRC
               
               EXCEPTION
                  WHEN no_data_found THEN
                  
                     lvComposer := 'e'; --supone evasion de portico
                  
                     BEGIN
                        --verifica si efectivamente es una evasión
                        --buscando en la banda de tiempo
                        SELECT pvd_matricula, to_char(pvd_pv)
                          INTO lvMat, lvPv
                          FROM pvd_pvdmcero
                         WHERE pvd_portico = lnPortico
                           AND pvd_fecha > ldIntvInf
                           AND pvd_fecha < ldIntvSup
                           AND pvd_matricula IN
                               (NULL, 'NO READ', 'NOT VALID', 'NOT PROC')
                           AND pvd_gimagenes IS NOT NULL;
                     
                        lvComposer := 'n'; --compose de ruta en base a los no read, not valid, no ...
                     
                     EXCEPTION
                        WHEN too_many_rows THEN
                           --análisis de la banda de ruido introducida por fallas del OCR 
                           --(lecturas NO VALID, NO READ, NOT PROC y NULL)
                           DECLARE
                              CURSOR lcOCRs IS
                                 SELECT pvd_matricula,
                                        pvd_matriculaocrtras,
                                        pvd_pv,
                                        pvd_fecha,
                                        pvd_categoria,
                                        pvd_velocidad,
                                        pvd_alto,
                                        pvd_ancho,
                                        pvd_largo
                                   FROM pvd_pvdmcero
                                  WHERE pvd_portico = lnPortico
                                    AND pvd_fecha > ldIntvInf
                                    AND pvd_fecha < ldIntvSup
                                    AND pvd_matricula IN
                                        (NULL, 'NO READ', 'NOT VALID',
                                         'NOT PROC')
                                    AND pvd_gimagenes IS NOT NULL;
                           
                              lnPort1 PLS_INTEGER;
                              lnDep   NUMBER; --distancia entre pórticos
                              lnVm    NUMBER; --velocidad media
                              lnV1    NUMBER;
                              lnV2    NUMBER;
                              lnTc    NUMBER;
                              lnTr    NUMBER;
                              lnkR    NUMBER; --pertenencia de ruta
                              ldT2    TIMESTAMP(4);
                              lvT2    VARCHAR2(8);
                              lvT1    VARCHAR2(8);
                              lnT2    NUMBER;
                              lnT1    NUMBER;
                              lvC1    VARCHAR2(1);
                              lvC2    VARCHAR2(1);
                              lnRuid  PLS_INTEGER; --ruido
                              lnErr   NUMBER;
                              lnKem   NUMBER := 0.3; -- tolerancia máxima de error de pertenencia de Ruta (esto significa que aceptan IRIs entre 0.7 y 1.3)
                              -- << parametrizar
                           
                           BEGIN
                              lnPort1 := to_number(fvVectorItem(lrIRI.udv_tx_vrecorrido,
                                                                lnItemIRI));
                              lnV1    := to_number(fvVectorItem(lrIRI.udv_tx_vvelocidad,
                                                                lnItemIRI));
                              lvC1    := fvVectorItem(lrIRI.udv_tx_vcategoria,
                                                      lnItemIRI);
                              lnDep   := fnDistanceEP(lnPort1, lnPortico);
                           
                              lvT1 := to_char(ldIntvInf, 'hh24:mi:ss');
                              lnT1 := to_number(substr(lvT1, 1, 2)) * 3600 +
                                      to_number(substr(lvT1, 4, 2)) * 60 +
                                      to_number(substr(lvT1, 7, 2));
                           
                              lvComposer := 'e'; --inicializa suponiendo que es evasión de pórtico
                              lvPv       := 'e';
                              lvMat      := 'e';
                              lnRuid     := 0;
                              lnErr      := 10000; --incializa un error
                           
                              FOR lrOCR IN lcOCRs LOOP
                              
                                 IF lrOCR.pvd_matriculaocrtras IS NOT NULL AND
                                    lrOCR.pvd_matriculaocrtras NOT IN
                                    ('NOT VALID', 'NO READ', 'NOT PROC') THEN
                                 
                                    --existe información de patente trasera
                                    IF kstr.fnLevenshtein(lrOCR.pvd_matriculaocrtras,
                                                          lrIRI.udv_tx_patente) <= 2 THEN
                                       -- << parametrizar el valor 2
                                       --//evaluar;
                                       lvComposer := 't'; --compone la ruta
                                       lvPv       := lrOCR.pvd_pv;
                                       lvMat      := lrOCR.pvd_matriculaocrtras;
                                       EXIT;
                                    END IF;
                                 
                                 ELSE
                                    --no existe información de patente trasera
                                 
                                    lvC2 := lrOCR.pvd_categoria;
                                 
                                    IF lvC1 = lvC2 THEN
                                       --// falta poner condición de verificación de largo (desviación)
                                       lnV2 := to_number(lrOCR.pvd_velocidad);
                                       lnVm := (lnV1 + lnV2) / 2 / 100;
                                       ldT2 := lrOCR.pvd_fecha + 0;
                                       lvT2 := to_char(ldT2, 'hh24:mi:ss');
                                       lnT2 := to_number(substr(lvT2, 1, 2)) * 3600 +
                                               to_number(substr(lvT2, 4, 2)) * 60 +
                                               to_number(substr(lvT2, 7, 2));
                                    
                                       IF lnVm <> 0 THEN
                                          lnTc := lnDep / lnVm;
                                          lnTr := lnT2 - lnT1;
                                          lnkR := lnTr / lnTc;
                                       ELSE
                                          lnkR := 0;
                                       END IF;
                                    
                                       lnErr := 1 - lnkR;
                                    
                                       IF lnErr < 0 THEN
                                          lnErr := lnErr * (-1);
                                       END IF;
                                    
                                       IF lnErr <= lnKem THEN
                                          lnRuid     := lnRuid + 1;
                                          lvComposer := 'n'; --compone la ruta
                                          lvPv       := lrOCR.pvd_pv;
                                          lvMat      := lrOCR.pvd_matricula;
                                       
                                          IF lvMat IS NULL THEN
                                             lvMat := 'null';
                                          END IF;
                                       
                                          IF lnRuid > 1 THEN
                                             lvComposer := 'r'; --hay mucho ruido para decidir
                                             lvPv       := 'r';
                                             lvMat      := 'r';
                                             EXIT;
                                          END IF;
                                       END IF;
                                    
                                    END IF;
                                 END IF;
                              
                              END LOOP;
                           END;
                        
                        WHEN no_data_found THEN
                           lvComposer := 'e';
                           lvPv       := 'e';
                           lvMat      := 'e';
                     END;
                  WHEN too_many_rows THEN
                     lvComposer := 'i'; --varias patente similares (investigar)
                     lvPv       := 'i';
                     lvMat      := 'i';
               END;
            
               lvIRC         := lvIRC || ';' || lvComposer;
               lvMatComposer := lvMatComposer || ';' || lvMat;
               lvPvComposer  := lvPvComposer || ';' || lvPv;
            
            ELSE
               lvIRC         := lvIRC || ';' || lvIRI_XItem;
               lvMatComposer := lvMatComposer || ';' || lrIRI.udv_tx_patente;
               lvPvComposer  := lvPvComposer || ';' || lvPv;
               lnItemIRI     := lnItemIRI + 1;
            END IF;
         END LOOP;
      
         lvIRC         := ltrim(lvIRC, ';');
         lvIRC         := rtrim(lvIRC, ';');
         lvMatComposer := ltrim(lvMatComposer, ';');
         lvMatComposer := rtrim(lvMatComposer, ';');
         lvPvComposer  := ltrim(lvPvComposer, ';');
         lvPvComposer  := rtrim(lvPvComposer, ';');
      
         UPDATE udv_udvia
            SET udv_tx_virc         = lvIRC,
                udv_tx_vpatcomposer = lvMatComposer,
                udv_tx_vpvcomposer  = lvPvComposer
          WHERE ROWID = lrIRI.ROWID;
      
         lvMat      := NULL;
         lvPv       := NULL;
         lvComposer := NULL;
      
         kbtc.pRealizarCommit;
      
      END LOOP;
   
      kbtc.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         dbms_output.put_line(kBTC.avErrMsg);
         kBTC.pCreateMessage(1, lvPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbConformIRC;

   -- #############################################################################
   /**
   Aplica IRC.
   Aplica el algoritmo de información de ruta corregida.
   %author RHE / Raciel Hernandez
   %version 1A / 14Jun'09/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbApplyIRC RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fbApplyIRC';
   BEGIN
      --Marca las regiones IRC      
      IF NOT fbMarcarRegionIRC THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
      END IF;
   
      --Conforma IRC
      IF NOT fbConformIRC THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
      END IF;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbApplyIRC;

   -- #############################################################################
   /**
   Lista los pv a consolidar automaticamente segun IRI.
   Lista los pasos de vehiculos a consolidar automaticamente con prediccion de patente y categoria.
   %author RHE / Raciel Hernandez
   %version 1A / 31mar'09/ RHE
   %version 1B / 28may'09 / RHE > modificacion para que utilice PIRI en vez de IRI_IRIAVI
   %version 2A / 22ene'10 / RHE > se incluye el nomble del operador (IRI/IRC)
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION pListIRI RETURN BOOLEAN IS
      CURSOR lcPATs IS
         SELECT udv_tx_patente,
                udv_tx_pvnoavi,
                udv_tx_pvnoavicat,
                udv_tx_categoria_rnc,
                udv_tx_regla_rnc,
                udv_tx_aplicar_rnc
           FROM udv_udvia u
          WHERE udv_tx_pvnoavi IS NOT NULL
            AND udv_nm_excepcion = 0;
   
      lvProgram VARCHAR2(30) := gvcPkg || 'pListIRI';
   
      lvCAT  VARCHAR2(7);
      lnCAT  NUMBER(1);
      lvRNC  VARCHAR2(5);
      lvApl  VARCHAR2(5);
      lnPV   NUMBER(20);
      lnLong PLS_INTEGER;
   BEGIN
      -- trunca tabla IRI
      EXECUTE IMMEDIATE 'TRUNCATE TABLE piri_propuestas_iri';
   
      -- fija la frecuencia de COMMIT;
      kBTC.pInitCommit(1000, lvProgram);
   
      -- determian los datos que se consolidaran
      FOR lrPAT IN lcPATs LOOP
         -- inicializa variables
         lvCAT := NULL;
         lnPV  := NULL;
         lnCAT := NULL;
         lvRNC := NULL;
         lvApl := NULL;
      
         IF INSTR(lrPAT.udv_tx_pvnoavi, ';', 1) = 0 THEN
            -- si solo hay un registro por cada UDV
            -- tomar el pv
            lnPV  := TO_NUMBER(lrPAT.udv_tx_pvnoavi);
            lvCAT := NVL(lrPAT.udv_tx_categoria_rnc, 'null');
            lvRNC := NVL(lrPAT.udv_tx_regla_rnc, 'null');
            lvApl := NVL(lrPAT.udv_tx_aplicar_rnc, 'null');
         
            IF lvCAT = 'null' THEN
               lnCAT := NULL;
            ELSE
               lnCAT := TO_NUMBER(lvCAT);
            END IF;
         
            IF lvRNC = 'null' THEN
               lvRNC := NULL;
            END IF;
         
            IF lvApl = 'null' THEN
               lvApl := NULL;
            END IF;
         
            -- inserta datos de pv, patente y categoria a consolidar segun IRI
            INSERT INTO piri
               (piri_nm_pv,
                piri_tx_patente,
                piri_nm_categoria,
                piri_fk_rnc_regla,
                piri_ck_aplicar,
                piri_tx_operator)
            VALUES
               (lnPV, --piri_nm_pv
                lrPAT.udv_tx_patente, --piri_tx_patente
                lnCAT, --categoría a cosolidar
                to_number(lvRNC), --regla RNC correspondiente
                lvApl,
                'IRI'); --indica si la regla será aplicada
         
            kBTC.pRealizarCommit;
         ELSE
            -- hay varios pv por UDV
            -- cantidad de pv a consolidar automaticamente por UDV
            lnLong := fnVectorSize(lrPAT.udv_tx_pvnoavi);
         
            -- por cada pv
            FOR j IN 1 .. lnLong LOOP
               lnPV := TO_NUMBER(fvVectorItem(lrPAT.udv_tx_pvnoavi, j, ';'));
               IF lrPAT.udv_tx_pvnoavicat IS NULL THEN
                  lnCAT := NULL;
                  lvRNC := NULL;
                  lvApl := NULL;
               ELSE
                  lvCAT := fvVectorItem(lrPAT.udv_tx_categoria_rnc, j, ';');
                  lvRNC := fvVectorItem(lrPAT.udv_tx_regla_rnc, j, ';');
                  lvApl := fvVectorItem(lrPAT.udv_tx_aplicar_rnc, j, ';');
                  IF lvCAT = 'null' THEN
                     lnCAT := NULL;
                  ELSE
                     lnCAT := TO_NUMBER(lvCAT);
                  END IF;
                  IF lvRNC = 'null' THEN
                     lvRNC := NULL;
                  END IF;
                  IF lvApl = 'null' THEN
                     lvApl := NULL;
                  END IF;
               END IF;
            
               -- inserta datos de pv, patente y categoria a consolidar segun IRI
               INSERT INTO piri
                  (piri_nm_pv,
                   piri_tx_patente,
                   piri_nm_categoria,
                   piri_fk_rnc_regla,
                   piri_ck_aplicar,
                   piri_tx_operator)
               VALUES
                  (lnPV, --piri_nm_pv
                   lrPAT.udv_tx_patente, --piri_tx_patente
                   lnCAT, --categoría a cosolidar
                   to_number(lvRNC), --regla RNC correspondiente
                   lvApl,
                   'IRI'); --indica si la regla será aplicada
            
            END LOOP;
         END IF;
      
         kBTC.pRealizarCommit;
      
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         dbms_output.put_line(kBTC.avErrMsg);
         RETURN FALSE;
   END pListIRI;

   -- #############################################################################
   /**
   Lista los pv a consolidar automaticamente segun IRC.
   Lista los pasos de vehiculos a consolidar automaticamente con prediccion de patente y categoria.
   %author RHE / Raciel Hernandez (CleverGlobal)
   %version 1A / 22ene'10/ RHE
   %return TRUE Si el proceso termina satisfactoriamente.
   %return FALSE Si el proceso se ejecuta con error.
   */
   FUNCTION fbListIRC RETURN BOOLEAN IS
      CURSOR lcIRCs IS
         SELECT udv_tx_patente,
                udv_tx_pvnoavi,
                udv_tx_pvnoavicat,
                udv_tx_categoria_rnc,
                udv_tx_regla_rnc,
                udv_tx_aplicar_rnc,
                udv_tx_virc,
                udv_tx_vpvcomposer
           FROM udv_udvia
          WHERE (instr(udv_tx_virc, 'k', 1) <> 0 OR
                instr(udv_tx_virc, 't', 1) <> 0 OR
                instr(udv_tx_virc, 'n', 1) <> 0)
            AND udv_nm_excepcion = 0;
   
      lvProgram VARCHAR2(30) := gvcPkg || 'fbListIRC';
   
      lvCAT  VARCHAR2(7);
      lnCAT  NUMBER(1);
      lvRNC  VARCHAR2(5);
      lvApl  VARCHAR2(5);
      lnPV   NUMBER(20);
      lnLong PLS_INTEGER;
      lnItem PLS_INTEGER;
   BEGIN
   
      -- fija la frecuencia de COMMIT;
      kBTC.pInitCommit(1000, lvProgram);
   
      -- determian los datos que se consolidaran
      FOR lrIRC IN lcIRCs LOOP
         -- inicializa variables
         lvCAT := NULL;
         lnPV  := NULL;
         lnCAT := NULL;
         lvRNC := NULL;
         lvApl := NULL;
      
         lnItem := fnVectorSize(lrIRC.udv_tx_virc, ';');
         FOR i IN 1 .. lnItem LOOP
            IF fvVectorItem(lrIRC.udv_tx_virc, i) IN ('k', 't') THEN
               lnPV := to_number(fvVectorItem(lrIRC.udv_tx_vpvcomposer, i));
               INSERT INTO piri
                  (piri_nm_pv,
                   piri_tx_patente,
                   piri_nm_categoria,
                   piri_fk_rnc_regla,
                   piri_ck_aplicar,
                   piri_tx_operator)
               VALUES
                  (lnPV, --piri_nm_pv
                   lrIRC.udv_tx_patente, --piri_tx_patente
                   lnCAT, --categoría a cosolidar
                   to_number(lvRNC), --regla RNC correspondiente
                   lvApl,
                   'IRC');
            
               kBTC.pRealizarCommit;
            
            END IF;
         END LOOP;
      
      END LOOP;
   
      kBTC.pFinalizarCommit;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         dbms_output.put_line(kBTC.avErrMsg);
         RETURN FALSE;
   END fbListIRC;

   /*##############################################################################
   %desc Envia PV de IRI a SOP para consolidar automaticamente, comprobando en forma
         parampetrica (CNF=enviarIRI2SOP) si debe realizarse el envío.
   %rtrn TRUE = el proceso terminó satisfactoriamente
   %rtrn FALSE = el proceso se ejecutó con error
   %autr RHE / Raciel Hernandez
   %vers 1A / 31mar'09 / RHE
   %vers 1B / 28may'09 / RHE > modificacion para que utilice PIRI en vez de IRI_IRIAVI
   */
   FUNCTION fbEnviar2SOP RETURN BOOLEAN IS
      CURSOR lcIRIs IS
         SELECT piri_nm_pv, piri_tx_patente, piri_nm_categoria
           FROM piri
          WHERE piri_nm_categoria = 1
             OR piri_nm_categoria IS NULL;
      lvProgram CONSTANT VARCHAR2(61) := gvcPkg || 'fbEnviar2SOP';
   BEGIN
      -- verifica si se configuro el envio
      IF kCNF.fvRecuperar('enviarIRI2SOP', 'F') = 'V' THEN
         FOR lrIRI IN lcIRIs LOOP
            IF NOT kENV.fbEnviar_0001(lrIRI.piri_nm_pv,
                                      lrIRI.piri_tx_patente,
                                      lrIRI.piri_nm_categoria) THEN
               kBTC.pSaveMessage(1, lvProgram, FALSE, lrIRI.piri_nm_pv);
            END IF;
         
         END LOOP;
      
         -- graba los envios
         COMMIT;
      
      ELSE
         kBTC.pSaveMessage(196, lvProgram, TRUE);
      END IF;
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvProgram, 'STD', TRUE);
         RETURN FALSE;
   END fbEnviar2SOP;


   /*##############################################################################
   %desc Ejectua el procesamiento IRI para el día especificado como parámetro
         de entrada o en su defecto, calculado a partir de delayConsolidacionIRI, para
         la consolidación automática de los PV de la AVI.
   %parm fpdAnalysisDay día que se desea analizar, si se informa NULL, el sistema recupera
                       el valor del parámetro CNF "delayConsolidacionIRI" y lo resta
                       al día actual
   %rtrn TRUE Si el proceso termina satisfactoriamente.
   %rtrn FALSE Si el proceso se ejecuta con error.
   %autr RHE / Raciel RHernandez
   %vers 1A / 31mar'09 / RHE
   %vers 2B / 25oct'09 / RHE > incorporación del parámetro configurable modoEjecuciónIRI
              que permite la ejecución en modo prueba y modo productivo. El modo productivo
              envía los datos de IRI a SOP, el modo prueba no.
   */
   FUNCTION fbEjecutarIRI(fpdAnalysisDay IN DATE) RETURN BOOLEAN IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'fbEjecutarIRI';
   BEGIN
      -- crea la copia local de PV para un dia
      IF NOT fbObtenerPVD(fpdAnalysisDay) THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- crea la tabla PVD_PVDMCERO
      IF NOT fbPvdMCero THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- resume UDV
      IF NOT fbResumeUDV THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- conformamos la matriz de rutas IRI de UDV
      IF NOT fbConformingIRI THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- prediccion de patentes segun IRI
      IF NOT fbIRIConsPat THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- marca de incoherencia de categoria en ruta
      IF NOT fbMarcaUICat THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- marca de incoherencia de longitud en ruta
      IF NOT fbMarcaUILon THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- marca de incoherencia de remolque en ruta
      IF NOT fbMarcaUIRem THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- consolidacion de la categoria en ruta
      IF NOT fbIRIConsCat THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      -- gestion de excecciones IRI
      IF NOT fbGestionExi THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      --obtiene los datos de categorías para cada vehículo     
      IF NOT fbObtenerDCVeh THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      --aplica RNC para consolidación de categorías     
      IF NOT fbApplyRNC THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
      --1=1 condición que indica aplicar IRC
      IF 1 = 1 THEN
         NULL;
      END IF;
   
      -- lista los pv con prediccion de patente y categoria
      IF NOT pListIRI THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
   
   
   
      -- registra en la interfaz el envio de PV para consolidacion IRI      
      /*
      IF NOT fbEnviar2SOP THEN
         kBTC.pCreateMessage(1, lvcPrg, 'STD', FALSE);
         RETURN FALSE;
      END IF;
      */
   
      RETURN TRUE;
   
   EXCEPTION
      WHEN OTHERS THEN
         kBTC.avErrMsg := SQLERRM;
         kBTC.pCreateMessage(1, lvcPrg, 'STD', TRUE);
         RETURN FALSE;
   END fbEjecutarIRI;


   -- #############################################################################
   PROCEDURE pRunIRI(fpdAnalysisDay IN DATE DEFAULT NULL) IS
      lvcPrg CONSTANT VARCHAR2(61) := gvcPkg || 'pRunIRI';
   BEGIN
      -- fija la zona horaria para que los mensajes queden con al hora correcta
      EXECUTE IMMEDIATE 'ALTER SESSION SET TIME_ZONE = ''Chile/Continental''';
   
      -- fija el programa inicial de registro en la bitacora
      kBTC.pIniciarRegistro(lvcPrg);
   
      -- registra el inicio de ejecucion
      kBTC.avErrMsg := 'IRI';
      kBTC.pSaveMessage(142, lvcPrg, TRUE);
   
      -- ejecuta el algoritmo IRI
      IF NOT fbEjecutarIRI(fpdAnalysisDay) THEN
         kBTC.pSaveMessage(1, lvcPrg, FALSE);
      END IF;
   
      -- registra el fin de ejecucion
      kBTC.avErrMsg := 'IRI';
      kBTC.pSaveMessage(85, lvcPrg, TRUE);
   
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
         kBTC.avErrMsg := SQLERRM;
         kBTC.pSaveMessage(1, lvcPrg, TRUE);
   END pRunIRI;

END kIRI_v4;
/
