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

import dk.nsi.haiba.minipasconverter.dao.MinipasSyncDAO;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;

public class MinipasSyncDAOImpl extends CommonDAO implements MinipasSyncDAO {
    private static final Logger aLog = Logger.getLogger(MinipasSyncDAOImpl.class);
    private Map<Integer, Map<String, SyncStruct>> aPendingSyncStructsForYear = new HashMap<Integer, Map<String, SyncStruct>>();

    public MinipasSyncDAOImpl(String dialect) {
        super(dialect);
    }
    
    @Autowired
    @Qualifier("minipasSyncJdbcTemplate")
    JdbcTemplate jdbc;

    @Value("${minipas.syncidnummerfecthbatchsize:100}")
    int batchSize;

    @Override
    public void cleanupRowsFromTablesOlderThanYear(int year) {
        // SOURCE_TABLE_NAME has values like T_ADM2010. the year part starts at the 6th position (1 based index)
        int update = jdbc.update("DELETE FROM T_MINIPAS_SYNC WHERE int(subtring(SOURCE_TABLE_NAME, 6)) < ?", year);
        if (aLog.isDebugEnabled()) {
            aLog.debug("cleanupRowsFromTablesOlderThanYear: number of rows affected " + update + " for year=" + year);
        }
    }

    @Override
    public MinipasSyncStructure test(int year, Collection<MinipasTADM> minipasRows) {
        MinipasSyncStructureImpl returnValue = new MinipasSyncStructureImpl();
        Map<String, SyncStruct> pendingIdnummersForYear = getPendingIdNummersForYear(year);
        for (MinipasTADM minipasTADM : minipasRows) {
            Date skemaopdat = null;
            SyncStruct syncStruct = pendingIdnummersForYear.get(minipasTADM.getIdnummer());
            if (syncStruct != null) {
                skemaopdat = syncStruct.aSkemaOpdat;
            }
            Date d = minipasTADM.getSkemaopdat() != null ? minipasTADM.getSkemaopdat() : minipasTADM.getSkemaopret();
            if (skemaopdat == null) {
                returnValue.aCreated.add(minipasTADM);
            } else if (!skemaopdat.equals(d)) {
                returnValue.aUpdated.add(minipasTADM);
            }
        }
        return returnValue;
    }

    private Map<String, SyncStruct> getPendingIdNummersForYear(int year) {
        Map<String, SyncStruct> returnValue = aPendingSyncStructsForYear.get(year);
        if (returnValue == null) {
            if (aLog.isDebugEnabled()) {
                aLog.debug("getPendingIdNummersForYear: fetching rows for year=" + year);
            }
            returnValue = new HashMap<String, MinipasSyncDAOImpl.SyncStruct>();
            // download the idnummer list, 100 at a time
            int id = -1;
            while (true) {
                List<SyncStruct> list = jdbc.query(
                        "SELECT * FROM T_MINIPAS_SYNC WHERE SOURCE_TABLE_NAME=? AND ID>? FETCH FIRST ? ROWS ONLY",
                        new RowMapper<SyncStruct>() {
                            @Override
                            public SyncStruct mapRow(ResultSet rs, int rowNum) throws SQLException {
                                SyncStruct returnValue = new SyncStruct();
                                returnValue.aId = rs.getInt("ID");
                                returnValue.aIdNummer = rs.getString("IDNUMMER");
                                returnValue.aSkemaOpdat = rs.getDate("SKEMAOPDAT");
                                return returnValue;
                            }
                        }, "T_ADM" + year, id, batchSize);
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
        return returnValue;
    }

    @Override
    public void commit(int year, MinipasSyncStructure syncStructure) {
        for (MinipasTADM minipasTADM : syncStructure.getCreated()) {
            Date skemaopdat = minipasTADM.getSkemaopdat() == null ? minipasTADM.getSkemaopret() : minipasTADM
                    .getSkemaopdat();
            jdbc.update("INSERT INTO T_MINIPAS_SYNC (IDNUMMER, SKEMAOPDAT, SOURCE_TABLE_NAME) VALUES (?, ?, ?)",
                    minipasTADM.getIdnummer(), skemaopdat, "T_ADM" + year);
        }
        for (MinipasTADM minipasTADM : syncStructure.getCreated()) {
            jdbc.update("UPDATE T_MINIPAS_SYNC SET SKEMAOPDAT=? WHERE IDNUMMER=?", minipasTADM.getIdnummer(),
                    minipasTADM.getSkemaopdat());
        }
        for (MinipasTADM minipasTADM : syncStructure.getCreated()) {
            jdbc.update("DELETE FROM T_MINIPAS_SYNC WHERE IDNUMMER=?", minipasTADM.getIdnummer());
        }
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
        Collection<String> returnValue = new ArrayList<String>();
        Map<String, SyncStruct> map = aPendingSyncStructsForYear.get(year);
        if (map != null) {
            returnValue = map.keySet();
        } else {
            aLog.error("no rows fetched for year " + year);
        }
        return returnValue;
    }
}