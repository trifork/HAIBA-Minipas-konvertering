/**
 * The MIT License
 *
 * Original work sponsored and donated by National Board of e-Health (NSI), Denmark
 * (http://www.nsi.dk)
 *
 * Copyright (C) 2011 National Board of e-Health (NSI), Denmark (http://www.nsi.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package dk.nsi.haiba.minipasconverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.dao.impl.MinipasHAIBADAOImpl;
import dk.nsi.haiba.minipasconverter.executor.MinipasPreprocessor;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;
import dk.nsi.haiba.minipasconverter.status.CurrentImportProgress;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
// not able to run from maven without db2 driver
@Ignore
public class PreprocessorTest {
    private static final Logger aLog = Logger.getLogger(PreprocessorTest.class);
    private static final String C_SGH = "sgh";
    private static final String C_AFD = "afd";
    private static final String C_PATTYPE = "1";
    private static final String V_CPR = "2612482516";
    private static final Date D_INDDTO = new DateTime(2013, 10, 10, 10, 10).toDate();
    private static final Date D_UDDTO = new DateTime(2013, 10, 11, 10, 10).toDate();

    // XXX undone tests: cache overflow with 1 entry, cache overflow with 2 entries, aborted transaction

    @Configuration
    @Import({ TestConfiguration.class })
    static class MyConfiguration {
        @Bean
        public CurrentImportProgress currentImportProgress() {
            return Mockito.mock(CurrentImportProgress.class);
        }
    }

    @Autowired
    MinipasDAO minipasDAO;

    @Autowired
    MinipasHAIBADAO minipasHAIBADAO;

    @Autowired
    MinipasPreprocessor minipasPreprocessor;

    @Autowired
    CurrentImportProgress currentImportProgress;

    @Autowired
    @Qualifier("minipasJdbcTemplate")
    JdbcTemplate minipasJdbc;

    @Autowired
    @Qualifier("minipasSyncJdbcTemplate")
    JdbcTemplate minipasSyncJdbc;

    @Autowired
    @Qualifier("minipasHaibaJdbcTemplate")
    JdbcTemplate haibaJdbc;

    Random random = new Random(System.currentTimeMillis());
    private int aKRecnum;

    @Before
    public void init() {
        Logger.getLogger(MinipasPreprocessor.class).setLevel(Level.DEBUG);
        Logger.getLogger(PreprocessorTest.class).setLevel(Level.DEBUG);
        Logger.getLogger(MinipasHAIBADAOImpl.class).setLevel(Level.TRACE);
    }

    @Test
    public void testEmptyImport() {
        if (aLog.isDebugEnabled()) {
            aLog.debug("testEmptyImport: ");
        }
        resetDb();
        setImportFlagOk();
        minipasPreprocessor.doManualProcess();
        assertEquals(0, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_adm"));

        // start and end timestamp are both set?
        long maxSyncId = haibaJdbc.queryForLong("SELECT max(v_sync_id) FROM T_LOG_SYNC");
        long okIfOne = haibaJdbc.queryForLong("SELECT count(*) FROM T_LOG_SYNC WHERE v_sync_id = ?", maxSyncId);
        assertEquals(1, okIfOne);
    }

    @Test
    public void testUpdated() {
        if (aLog.isDebugEnabled()) {
            aLog.debug("testUpdated: ");
        }
        Date nyuddto = new DateTime().withMillisOfDay(0).toDate();
        resetDb();
        // insert ok status
        setImportFlagOk();
        MinipasTADM minipasTADM = createRandomTADM();
        MinipasTDIAG minipasTDIAG = createRandomTDIAG(minipasTADM);
        minipasTDIAG.setC_tildiag("123");
        MinipasTSKSUBE_OPR minipasTSKSUBE = createRandomTSKSUBE_OPR(minipasTADM);
        MinipasTSKSUBE_OPR minipasTSKSOPR = createRandomTSKSUBE_OPR(minipasTADM);

        insertMinipasTADM(minipasTADM);
        insertMinipasTDIAG(minipasTDIAG);
        insertMinipasTSKSUBE_OPR(minipasTSKSUBE, "T_SKSUBE2014");
        insertMinipasTSKSUBE_OPR(minipasTSKSOPR, "T_SKSOPR2014");

        minipasPreprocessor.doManualProcess();

        assertEquals(1, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(3, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_adm"));
        List<String> list = haibaJdbc.queryForList("select V_TYPE from t_koder where V_RECNUM=?", String.class,
                minipasTADM.getIdnummer());
        assertTrue(list.contains("dia"));
        assertTrue(list.contains("opr"));
        assertTrue(list.contains("und"));

        // simulate that the row is read
        haibaJdbc.update("UPDATE T_ADM SET D_IMPORTDTO=? WHERE V_RECNUM=?", new Date(), minipasTADM.getIdnummer());
        assertNotNull(haibaJdbc.queryForObject("select D_IMPORTDTO from t_adm WHERE V_RECNUM=?", Date.class,
                minipasTADM.getIdnummer()));

        insertMinipasTSKSUBE_OPR(createRandomTSKSUBE_OPR(minipasTADM), "T_SKSOPR2014");
        insertMinipasTDIAG(createRandomTDIAG(minipasTADM));

        // reset skemaopdat, indicating change
        minipasJdbc.update("UPDATE T_ADM2014 SET SKEMAOPDAT=?,D_UDDTO=? WHERE IDNUMMER=?", new Date(), nyuddto,
                minipasTADM.getIdnummer());
        minipasJdbc.update("UPDATE T_DIAG2014 SET C_DIAG='hest' WHERE IDNUMMER=? AND C_TILDIAG=?",
                minipasTADM.getIdnummer(), "123");

        minipasPreprocessor.doManualProcess();

        // also test d_importdto is reset
        assertNull(haibaJdbc.queryForObject("select D_IMPORTDTO from t_adm WHERE V_RECNUM=?", Date.class,
                minipasTADM.getIdnummer()));
        Date tadmUddto = haibaJdbc.queryForObject("select D_UDDTO from t_adm WHERE V_RECNUM=?", Date.class,
                minipasTADM.getIdnummer());
        assertEquals("t_adm, uddto ikke opdateret: " + tadmUddto + " vs. " + nyuddto, new Date(tadmUddto.getTime()),
                new Date(nyuddto.getTime()));
        assertEquals(1, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(5, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_adm"));
        String actual = haibaJdbc.queryForObject("SELECT C_KODE FROM T_KODER WHERE V_RECNUM=? AND C_TILKODE=?",
                String.class, minipasTADM.getIdnummer(), "123");
        System.out.println("actual='" + actual + "'");
        assertEquals("hest", actual.trim());
    }

    @Test
    public void testCreatedUpdatedDeleted() {
        if (aLog.isDebugEnabled()) {
            aLog.debug("testCreatedUpdatedDeleted: ");
        }
        resetDb();
        // insert ok status
        setImportFlagOk();

        MinipasTADM minipasTADM1 = createRandomTADM();
        insertMinipasTADM(minipasTADM1);
        insertMinipasTDIAG(createRandomTDIAG(minipasTADM1));

        MinipasTADM minipasTADM2 = createRandomTADM();
        insertMinipasTADM(minipasTADM2);
        insertMinipasTDIAG(createRandomTDIAG(minipasTADM2));

        minipasPreprocessor.doManualProcess();

        assertEquals(2, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(2, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(2, haibaJdbc.queryForInt("select count(*) from t_adm"));

        // delete 1
        minipasJdbc.update("DELETE FROM T_ADM2014 WHERE IDNUMMER=?", minipasTADM1.getIdnummer());

        // update 2, reset skemaopdat, indicating change
        minipasJdbc
                .update("UPDATE T_ADM2014 SET SKEMAOPDAT=? WHERE IDNUMMER=?", new Date(), minipasTADM2.getIdnummer());

        // create 3
        MinipasTADM minipasTADM3 = createRandomTADM();
        insertMinipasTADM(minipasTADM3);
        insertMinipasTDIAG(createRandomTDIAG(minipasTADM3));

        // simulate read
        haibaJdbc.update("UPDATE T_ADM SET D_IMPORTDTO=? WHERE V_RECNUM=?", new Date(), minipasTADM1.getIdnummer());
        haibaJdbc.update("UPDATE T_ADM SET D_IMPORTDTO=? WHERE V_RECNUM=?", new Date(), minipasTADM2.getIdnummer());

        minipasPreprocessor.doManualProcess();

        // also test d_importdto is reset
        assertNull(haibaJdbc.queryForObject("select D_IMPORTDTO from t_adm WHERE V_RECNUM=?", Date.class,
                minipasTADM2.getIdnummer()));
        assertEquals(2, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(2, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(2, haibaJdbc.queryForInt("select count(*) from t_adm"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_log_sync_history"));
    }

    @Test
    public void testDeleted() {
        if (aLog.isDebugEnabled()) {
            aLog.debug("testDeleted: ");
        }
        resetDb();
        // insert ok status
        setImportFlagOk();
        MinipasTADM minipasTADM = createRandomTADM();
        MinipasTDIAG minipasTDIAG = createRandomTDIAG(minipasTADM);
        MinipasTSKSUBE_OPR minipasTSKSUBE = createRandomTSKSUBE_OPR(minipasTADM);
        MinipasTSKSUBE_OPR minipasTSKSOPR = createRandomTSKSUBE_OPR(minipasTADM);

        insertMinipasTADM(minipasTADM);
        insertMinipasTDIAG(minipasTDIAG);
        insertMinipasTSKSUBE_OPR(minipasTSKSUBE, "T_SKSUBE2014");
        insertMinipasTSKSUBE_OPR(minipasTSKSOPR, "T_SKSOPR2014");

        minipasPreprocessor.doManualProcess();

        assertEquals(1, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(3, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_adm"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_log_sync_history"));

        minipasJdbc.update("delete from T_DIAG2014");
        minipasJdbc.update("delete from T_SKSOPR2014");
        minipasJdbc.update("delete from T_SKSUBE2014");
        minipasJdbc.update("delete from T_ADM2014");

        minipasPreprocessor.doManualProcess();

        assertEquals(0, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_adm"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_log_sync_history"));
        String deleted = haibaJdbc.queryForObject("select C_ACTION_TYPE from t_log_sync_history where v_recnum = ?",
                String.class, minipasTADM.getIdnummer());
        assertEquals("DELETE", deleted);
    }

    private void insertMinipasTSKSUBE_OPR(MinipasTSKSUBE_OPR m, String table) {
        minipasJdbc.update("INSERT INTO " + table
                + " (V_RECNUM, IDNUMMER, C_OPR, C_TILOPR, C_OPRART, INDBERETNINGSDATO) " + "VALUES (?, ?, ?, ?, ?, ?)",
                m.getV_recnum(), m.getIdnummer(), m.getC_opr(), m.getC_tilopr(), m.getC_oprart(),
                m.getIndberetningsdato());
    }

    private void insertMinipasTDIAG(MinipasTDIAG m) {
        minipasJdbc.update(
                "INSERT INTO T_DIAG2014 (V_RECNUM, IDNUMMER, C_DIAG, C_DIAGTYPE, C_TILDIAG, INDBERETNINGSDATO) "
                        + "VALUES (?, ?, ?, ?, ?, ?)", m.getV_recnum(), m.getIdnummer(), m.getC_diag(),
                m.getC_diagtype(), m.getC_tildiag(), m.getIndberetningsdato());
    }

    private MinipasTSKSUBE_OPR createRandomTSKSUBE_OPR(MinipasTADM minipasTADM) {
        MinipasTSKSUBE_OPR returnValue = new MinipasTSKSUBE_OPR();
        returnValue.setIdnummer(minipasTADM.getIdnummer());
        returnValue.setV_recnum(minipasTADM.getK_recnum());

        returnValue.setC_opr("opr");
        returnValue.setC_tilopr("tilopr");
        returnValue.setC_oprart("x");
        returnValue.setIndberetningsdato(new Date());
        return returnValue;
    }

    private MinipasTDIAG createRandomTDIAG(MinipasTADM minipasTADM) {
        MinipasTDIAG returnValue = new MinipasTDIAG();
        returnValue.setIdnummer(minipasTADM.getIdnummer());
        returnValue.setV_recnum(minipasTADM.getK_recnum());

        returnValue.setC_diag("diag");
        returnValue.setC_diagtype("x");
        returnValue.setC_tildiag("tildiag");
        returnValue.setIndberetningsdato(new Date());
        return returnValue;
    }

    @Test
    public void testCreated() {
        if (aLog.isDebugEnabled()) {
            aLog.debug("testCreated: ");
        }
        resetDb();
        // insert ok status
        setImportFlagOk();
        MinipasTADM minipasTADM = createRandomTADM();

        insertMinipasTADM(minipasTADM);

        minipasPreprocessor.doManualProcess();

        assertEquals(1, minipasSyncJdbc.queryForInt("select count(*) from t_minipas_sync"));
        assertEquals(0, haibaJdbc.queryForInt("select count(*) from t_koder"));
        assertEquals(1, haibaJdbc.queryForInt("select count(*) from t_adm"));
    }

    public void setImportFlagOk() {
        minipasJdbc
                .update("insert into T_MINIPAS_UGL_STATUS (K_ID, D_STARTDATETIME, D_ENDDATETIME, V_RETURNCODE) VALUES (1, '2014-03-10', '2014-03-11', 1)");
    }

    public void resetDb() {
        minipasJdbc.update("delete from T_MINIPAS_UGL_STATUS");
        minipasJdbc.update("delete from T_DIAG2014");
        minipasJdbc.update("delete from T_SKSOPR2014");
        minipasJdbc.update("delete from T_SKSUBE2014");
        minipasJdbc.update("delete from T_ADM2014");
        minipasSyncJdbc.update("delete from T_MINIPAS_SYNC");
        haibaJdbc.update("delete from T_ADM");
        haibaJdbc.update("delete from T_KODER");
        haibaJdbc.update("delete from T_LOG_SYNC_HISTORY");
    }

    private void insertMinipasTADM(MinipasTADM m) {
        minipasJdbc
                .update("INSERT INTO T_ADM2014 (K_RECNUM, IDNUMMER, SKEMAOPDAT, SKEMAOPRET, C_SGH, C_AFD, C_PATTYPE, V_CPR, D_INDDTO, D_UDDTO) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", m.getK_recnum(), m.getIdnummer(), m.getSkemaopdat(),
                        m.getSkemaopret(), m.getC_sgh(), m.getC_afd(), m.getC_pattype(), m.getV_cpr(), m.getD_inddto(),
                        m.getD_uddto());
    }

    public MinipasTADM createRandomTADM() {
        MinipasTADM returnValue = new MinipasTADM();
        returnValue.setK_recnum(aKRecnum++);
        returnValue.setIdnummer(UUID.randomUUID().toString());
        Date d = new Date();
        returnValue.setSkemaopret(d);
        returnValue.setSkemaopdat(d);
        returnValue.setC_sgh(C_SGH);
        returnValue.setC_afd(C_AFD);
        returnValue.setC_pattype(C_PATTYPE);
        returnValue.setV_cpr(V_CPR);
        returnValue.setD_inddto(D_INDDTO);
        returnValue.setD_uddto(D_UDDTO);
        return returnValue;
    }
}
