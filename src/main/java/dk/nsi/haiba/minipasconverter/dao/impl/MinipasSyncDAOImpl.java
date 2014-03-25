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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import dk.nsi.haiba.minipasconverter.dao.MinipasSyncDAO;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;

public class MinipasSyncDAOImpl extends CommonDAO implements MinipasSyncDAO {
    private static final Logger aLog = Logger.getLogger(MinipasSyncDAOImpl.class);
    private Map<Integer, Map<String, SyncStruct>> aPendingSyncStructsForYear = new HashMap<Integer, Map<String, SyncStruct>>();

    public MinipasSyncDAOImpl(String dialect) {
        super(dialect);
    }

    @Autowired
    @Qualifier("haibaSyncJdbcTemplate")
    JdbcTemplate jdbc;

    @Value("${minipas.syncidnummerfecthbatchsize:100}")
    int batchSize;

    @Value("${jdbc.minipashaibasynctableprefix:}")
    String tableprefix;

    @Override
    public void reset() {
        aPendingSyncStructsForYear.clear();
    }

    @Override
    public void cleanupRowsFromTablesOlderThanYear(int year) {
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
    public MinipasSyncStructure test(int year, Collection<MinipasTADM> minipasRows) {
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
                    aLog.trace("test: " + skemaopdat + "!=" + d + " for " + minipasTADM.getIdnummer());
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
            returnValue = new HashMap<String, MinipasSyncDAOImpl.SyncStruct>();
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
                        returnValue.aSkemaOpdat = rs.getTimestamp("SKEMAOPDAT");
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
    public void commit(int year, MinipasSyncStructure syncStructure) {
        Monitor mon = MonitorFactory.start("MinipasSyncDAOImpl.commit");
        for (MinipasTADM minipasTADM : syncStructure.getCreated()) {
            Date skemaopdat = minipasTADM.getSkemaopdat() == null ? minipasTADM.getSkemaopret() : minipasTADM
                    .getSkemaopdat();
            jdbc.update("INSERT INTO " + tableprefix
                    + "T_MINIPAS_SYNC (IDNUMMER, SKEMAOPDAT, SOURCE_TABLE_NAME) VALUES (?, ?, ?)",
                    minipasTADM.getIdnummer(), skemaopdat, "T_ADM" + year);
        }
        for (MinipasTADM minipasTADM : syncStructure.getUpdated()) {
            if (aLog.isTraceEnabled()) {
                aLog.trace("commit: updating " + minipasTADM.getSkemaopdat() + " for " + minipasTADM.getIdnummer());
            }
            jdbc.update("UPDATE " + tableprefix + "T_MINIPAS_SYNC SET SKEMAOPDAT=? WHERE IDNUMMER=?",
                    minipasTADM.getSkemaopdat(), minipasTADM.getIdnummer());
        }
        mon.stop();
    }

    @Override
    public void commitDeleted(int year, Collection<String> deleted) {
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
    public Collection<String> getDeletedIdnummers(int year) {
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
}
