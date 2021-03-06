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
package dk.nsi.haiba.minipasconverter.dao.impl;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTBES;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MinipasHAIBADAOImpl extends CommonDAO implements MinipasHAIBADAO {
    private static final Logger aLog = Logger.getLogger(MinipasHAIBADAOImpl.class);
    private static SimpleDateFormat aSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Autowired
    @Qualifier("haibaJdbcTemplate")
    JdbcTemplate jdbc;

    @Value("${jdbc.minipashaibatableprefix:}")
    String tableprefix;

    private Map<Integer, Map<String, SyncStruct>> aPendingSyncStructsForYear = new HashMap<Integer, Map<String, SyncStruct>>();

    @Autowired
    @Qualifier("haibaTransactionManager")
    private PlatformTransactionManager transactionManager;
    private TransactionStatus transactionStatus;

    @Value("${minipas.syncidnummerfetchbatchsize:100}")
    int batchSize;

    public MinipasHAIBADAOImpl(String dialect) {
        super(dialect);
    }

    @Override
    public void createKoderFromSksUbeOpr(MinipasTADM minipasTADM, Collection<MinipasTSKSUBE_OPR> ubeoprs) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createKoderFromSksUbeOpr");
        for (MinipasTSKSUBE_OPR m : ubeoprs) {
            jdbc.update(
                    "INSERT INTO "
                            + tableprefix
                            + "T_KODER (V_RECNUM, C_KODE, C_TILKODE, C_KODEART, D_PDTO, C_PSGH, C_PAFD, V_TYPE) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    m.getIdnummer(), m.getC_opr(), m.getC_tilopr(), m.getC_oprart(), m.getD_odto(),
                    minipasTADM.getC_sgh(), minipasTADM.getC_afd(), m.getType());
        }
        mon.stop();
    }

    @Override
    public void clearKoder(String idnummer) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.clearKoder");
        int update = jdbc.update("DELETE FROM " + tableprefix + "T_KODER WHERE V_RECNUM=?", idnummer);
        if (aLog.isTraceEnabled()) {
            aLog.trace("clearKoder: number of rows affected " + update + " for idnummer=" + idnummer);
        }
        mon.stop();
    }

    @Override
    public void clearBes(String idnummer) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.clearBes");
        int update = jdbc.update("DELETE FROM " + tableprefix + "T_BES WHERE V_RECNUM=?", idnummer);
        if (aLog.isTraceEnabled()) {
            aLog.trace("clearBes: number of rows affected " + update + " for idnummer=" + idnummer);
        }
        mon.stop();
    }

    @Override
    public void createKoderFromDiag(MinipasTADM minipasTADM, Collection<MinipasTDIAG> diags) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createKoderFromDiag");
        for (MinipasTDIAG m : diags) {
            jdbc.update(
                    "INSERT INTO "
                            + tableprefix
                            + "T_KODER (V_RECNUM, C_KODE, C_TILKODE, C_KODEART, C_PSGH, C_PAFD, V_TYPE) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    m.getIdnummer(), m.getC_diag(), m.getC_tildiag(), m.getC_diagtype(), minipasTADM.getC_sgh(),
                    minipasTADM.getC_afd(), "dia");
        }
        mon.stop();
    }

    @Override
    public void createBesFromBes(MinipasTADM minipasTADM, Collection<MinipasTBES> bes) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createBesFromBes");
        for (MinipasTBES be : bes) {
            jdbc.update(
                    "INSERT INTO "
                            + tableprefix
                            + "T_BES (V_RECNUM, D_AMBDTO, INDBERETNINGSDATO) VALUES (?, ?, ?)",
                    be.getIdnummer(), be.getD_ambdto(), be.getIndberetningsdato());
        }
        mon.stop();
    }

    @Override
    public void createAdm(Collection<MinipasTADM> minipasTADMs) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createAdm");
        Date importDto = new Date();
        for (MinipasTADM m : minipasTADMs) {
            try {
                updateT_Adm(importDto, m, "a");
            } catch (DuplicateKeyException e) {
                // well, that was already present. remove and try again
                aLog.debug("createAdm: duplicate key found, reinserting key " + m.getIdnummer());
                clearAdm(m.getIdnummer());
                updateT_Adm(importDto, m, "a");

                // this means that T_MINIPAS_SYNC already has an entry with this idnummer.
                // blast this, else it will cause the T_ADM entry to be deleted during processing of the existing year
                // if this year is in the future, all is good. if in the past, this means that the entry is already
                // present in two years since we have this exception - we should be fine, but data are strange
                List<String> dieList = new ArrayList<String>();
                dieList.add(m.getIdnummer());
                syncCommitDeleted(dieList);
            }
        }
        mon.stop();
    }

    private int updateT_Adm(Date importDto, MinipasTADM m, String v_status) {
        return jdbc.update(
                "INSERT INTO "
                        + tableprefix
                        + "T_ADM (V_RECNUM, C_SGH, C_AFD, C_PATTYPE, V_CPR, D_INDDTO, D_UDDTO, C_INDM, D_IMPORTDTO, V_STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                m.getIdnummer(), m.getC_sgh(), m.getC_afd(), m.getC_pattype(), m.getV_cpr(), m.getD_inddto(),
                m.getD_uddto(), m.getC_indm(), importDto, v_status);
    }

    @Override
    public void clearAdm(String idnummer) {
        int update = jdbc.update("DELETE FROM " + tableprefix + "T_ADM WHERE V_RECNUM=?", idnummer);
        if (aLog.isTraceEnabled()) {
            aLog.trace("clearAdm: number of rows affected " + update + " for idnummer=" + idnummer);
        }
    }

    @Override
    public void resetAdmD_IMPORTDTO(Collection<MinipasTADM> minipasTADMs) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.resetAdmD_IMPORTDTO");
        for (MinipasTADM m : minipasTADMs) {
            jdbc.update("UPDATE " + tableprefix + "T_ADM SET D_IMPORTDTO=NULL WHERE V_RECNUM=?", m.getIdnummer());
        }
        mon.stop();
    }

    @Override
    public void importStarted() {
        jdbc.update("INSERT INTO " + tableprefix + "T_LOG_SYNC (START_TIME) VALUES (?)", new Date());
    }

    private long getNewestSyncId() throws EmptyResultDataAccessException {
        long returnValue = -1;
        String sql = null;
        if (MYSQL.equals(getDialect())) {
            sql = "SELECT V_SYNC_ID FROM T_LOG_SYNC ORDER BY START_TIME DESC LIMIT 1";
        } else {
            // MSSQL
            sql = "SELECT TOP 1 V_SYNC_ID FROM " + tableprefix + "T_LOG_SYNC ORDER BY START_TIME DESC";
        }
        returnValue = jdbc.queryForLong(sql);
        return returnValue;
    }

    @Override
    public void importEnded() {
        try {
            Long newestOpenId = getNewestSyncId();
            jdbc.update("UPDATE " + tableprefix + "T_LOG_SYNC SET END_TIME=? WHERE V_SYNC_ID=?", new Date(),
                    newestOpenId);
        } catch (EmptyResultDataAccessException e) {
            aLog.debug("importEnded: it seems we do not have any open statuses, let's not update");
        }
    }

    @Override
    public void setDeleted(String idnummer) {
        try {
            Long newestOpenId = getNewestSyncId();
            jdbc.update("INSERT INTO " + tableprefix
                            + "T_LOG_SYNC_HISTORY (V_SYNC_ID, V_RECNUM, C_ACTION_TYPE) VALUES (?, ?, 'DELETE')", newestOpenId,
                    idnummer);
        } catch (EmptyResultDataAccessException e) {
            aLog.debug("setDeleted: it seems we do not have any open statuses, let's not update");
        }
    }

    @Override
    public void reset() {
        aPendingSyncStructsForYear.clear();
    }

    @Override
    public void setupTransaction() {
        transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
    }

    @Override
    public void commitTransaction() {
        transactionManager.commit(transactionStatus);
    }

    @Override
    public void syncCleanupRowsFromTablesOlderThanYear(int year) {
        // SOURCE_TABLE_NAME has values like T_ADM2010. the year part starts at the 6th position (1 based index)
        String sql = null;
        if (MYSQL.equals(getDialect())) {
            sql = "DELETE FROM " + tableprefix
                    + "T_MINIPAS_SYNC WHERE convert(substring(SOURCE_TABLE_NAME, 6, 4), UNSIGNED INTEGER) < ?";
        } else {
            // MSSQL
            sql = "DELETE FROM " + tableprefix
                    + "T_MINIPAS_SYNC WHERE convert(int, substring(SOURCE_TABLE_NAME, 6, 4)) < ?";
        }
        int update = jdbc.update(sql, year);
        if (aLog.isDebugEnabled()) {
            aLog.debug("cleanupRowsFromTablesOlderThanYear: number of rows affected " + update + " for year=" + year);
        }
    }

    @Override
    public MinipasSyncStructure syncTest(int year, Collection<MinipasTADM> minipasRows) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.test");
        MinipasSyncStructureImpl returnValue = new MinipasSyncStructureImpl();
        Map<String, SyncStruct> pendingIdnummersForYear = getPendingIdNummersForYear(year);
        for (MinipasTADM minipasTADM : minipasRows) {
            Date skemaopdat = null;
            // remove the query result. the remains are considered deleted
            SyncStruct syncStruct = pendingIdnummersForYear.remove(minipasTADM.getIdnummer());
            if (syncStruct != null) {
                skemaopdat = syncStruct.aSkemaOpdat;
            }
            Date d = minipasTADM.getSkemaopdat() != null ? minipasTADM.getSkemaopdat() : minipasTADM.getSkemaopret();
            if (skemaopdat == null) {
                returnValue.aCreated.add(minipasTADM);
            } else if (!skemaopdat.equals(d)) {
                if (aLog.isTraceEnabled()) {
                    aLog.trace("test: " + aSimpleDateFormat.format(skemaopdat) + "!=" + d + " for "
                            + minipasTADM.getIdnummer());
                }
                returnValue.aUpdated.add(minipasTADM);
            }
        }
        mon.stop();
        return returnValue;
    }

    private Map<String, SyncStruct> getPendingIdNummersForYear(int year) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.getPendingIdNummersForYear");
        Map<String, SyncStruct> returnValue = aPendingSyncStructsForYear.get(year);
        if (returnValue == null) {
            if (aLog.isDebugEnabled()) {
                aLog.debug("getPendingIdNummersForYear: fetching rows for year=" + year);
            }
            returnValue = new HashMap<String, SyncStruct>();
            // download the idnummer list, 100 at a time
            // id is auto incrementing
            int id = -1;
            while (true) {
                String sql = null;
                if (MYSQL.equals(getDialect())) {
                    sql = "SELECT * FROM " + tableprefix
                            + "T_MINIPAS_SYNC WHERE SOURCE_TABLE_NAME=? AND ID>? ORDER BY ID LIMIT " + batchSize;
                } else {
                    // MSSQL
                    sql = "SELECT TOP " + batchSize + " * FROM " + tableprefix
                            + "T_MINIPAS_SYNC WHERE SOURCE_TABLE_NAME=? AND ID>? ORDER BY ID";
                }
                List<SyncStruct> list = jdbc.query(sql, new RowMapper<SyncStruct>() {
                    @Override
                    public SyncStruct mapRow(ResultSet rs, int rowNum) throws SQLException {
                        SyncStruct returnValue = new SyncStruct();
                        returnValue.aId = rs.getInt("ID");
                        returnValue.aIdNummer = rs.getString("IDNUMMER");
                        returnValue.aSkemaOpdat = new Date(rs.getLong("SKEMAOPDAT_MS"));
                        return returnValue;
                    }
                }, "T_ADM" + year, id);
                if (list == null || list.isEmpty()) {
                    break;
                }
                for (SyncStruct syncStruct : list) {
                    id = Math.max(id, syncStruct.aId);
                    returnValue.put(syncStruct.aIdNummer, syncStruct);
                }
            }
            aPendingSyncStructsForYear.put(year, returnValue);
            aLog.debug("fetched " + returnValue.size() + " for " + year);
        }
        mon.stop();
        return returnValue;
    }

    @Override
    public void syncCommit(int year, MinipasSyncStructure syncStructure) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.commit");
        for (MinipasTADM minipasTADM : syncStructure.getCreated()) {
            Date skemaopdat = minipasTADM.getSkemaopdat() == null ? minipasTADM.getSkemaopret() : minipasTADM
                    .getSkemaopdat();
            jdbc.update("INSERT INTO " + tableprefix
                            + "T_MINIPAS_SYNC (IDNUMMER, SKEMAOPDAT_MS, SOURCE_TABLE_NAME) VALUES (?, ?, ?)",
                    minipasTADM.getIdnummer(), skemaopdat.getTime(), "T_ADM" + year);
        }
        for (MinipasTADM minipasTADM : syncStructure.getUpdated()) {
            Date skemaopdat = minipasTADM.getSkemaopdat();
            Date skemaopret = minipasTADM.getSkemaopret();
            Date dateToUseForSKEMAOPDAT_MS = skemaopdat;
            if (dateToUseForSKEMAOPDAT_MS == null) {
                // seen in march 2016
                dateToUseForSKEMAOPDAT_MS = skemaopret;
            }
            if (dateToUseForSKEMAOPDAT_MS == null) {
                // just backup, never seen
                dateToUseForSKEMAOPDAT_MS = new Date();
            }

            long time = dateToUseForSKEMAOPDAT_MS.getTime();
            if (aLog.isTraceEnabled()) {
                aLog.trace("commit: updating using " + dateToUseForSKEMAOPDAT_MS + " for " + minipasTADM.getIdnummer() + ", skemaopdat=" + skemaopdat);
            }
            jdbc.update("UPDATE " + tableprefix + "T_MINIPAS_SYNC SET SKEMAOPDAT_MS=? WHERE IDNUMMER=?", time, minipasTADM.getIdnummer());
        }
        mon.stop();
    }

    @Override
    public void syncCommitDeleted(Collection<String> deleted) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.commitDeleted");
        for (String idnummer : deleted) {
            jdbc.update("DELETE FROM " + tableprefix + "T_MINIPAS_SYNC WHERE IDNUMMER=?", idnummer);
        }
        mon.stop();
    }

    public static class MinipasSyncStructureImpl implements MinipasSyncStructure {
        private List<MinipasTADM> aCreated = new ArrayList<MinipasTADM>();
        private List<MinipasTADM> aUpdated = new ArrayList<MinipasTADM>();

        @Override
        public Collection<MinipasTADM> getCreated() {
            return aCreated;
        }

        @Override
        public Collection<MinipasTADM> getUpdated() {
            return aUpdated;
        }
    }

    public static class SyncStruct {
        private int aId;
        private String aIdNummer;
        private Date aSkemaOpdat;
    }

    @Override
    public Collection<String> syncGetDeletedIdnummers(int year) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.getDeletedIdnummers");
        Collection<String> returnValue = new ArrayList<String>();
        Map<String, SyncStruct> map = aPendingSyncStructsForYear.remove(year);
        if (map != null) {
            returnValue = map.keySet();
            if (aLog.isTraceEnabled()) {
                aLog.trace("getDeletedIdnummers: year " + year + ":" + returnValue);
            }
        } else {
            aLog.error("no rows fetched for year " + year);
        }
        mon.stop();
        return returnValue;
    }

    @Override
    public void reinsertAdm(MinipasTADM m) {
        clearAdm(m.getIdnummer());
        Date importDto = new Date();
        updateT_Adm(importDto, m, "m");
    }

    @Override
    public List<MinipasTADM> getTADMFromIdnummer(String sYear, String idnummer, int batchSize) {
        RowMapper<MinipasTADM> rowMapper = new RowMapper<MinipasTADM>() {
            @Override
            public MinipasTADM mapRow(ResultSet rs, int i) throws SQLException {
                MinipasTADM returnValue = new MinipasTADM();
                returnValue.setIdnummer(rs.getString("V_RECNUM"));
                // never mind the rest
                return returnValue;
            }
        };
        List<MinipasTADM> returnValue = jdbc.query("SELECT TOP " + batchSize + " * FROM " + tableprefix + "T_ADM WHERE V_RECNUM > ? ORDER BY V_RECNUM ASC", rowMapper,
                idnummer);
        return returnValue;
    }

    @Override
    public void insertUpdateT_BesAndC_Indm(List<MinipasTBES> minipasTBESList, List<MinipasTADM> minipasTADMList) {
        if (aLog.isTraceEnabled()) {
            aLog.trace("insertUpdateT_BesAndC_Indm: t_bes...");
        }
        for (MinipasTBES be : minipasTBESList) {
            jdbc.update(
                    "INSERT INTO " + tableprefix + "T_BES (V_RECNUM, D_AMBDTO, INDBERETNINGSDATO) VALUES (?, ?, ?)",
                    be.getIdnummer(), be.getD_ambdto(), be.getIndberetningsdato());
        }
        if (aLog.isTraceEnabled()) {
            aLog.trace("insertUpdateT_BesAndC_Indm: c_indm...");
        }
        for (MinipasTADM m : minipasTADMList) {
            jdbc.update("UPDATE " + tableprefix + "T_ADM SET C_INDM=? WHERE V_RECNUM=?", m.getC_indm(), m.getIdnummer());
        }
        if (aLog.isTraceEnabled()) {
            aLog.trace("insertUpdateT_BesAndC_Indm: done");
        }
    }
}
