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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import dk.nsi.haiba.minipasconverter.dao.DAOException;
import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.model.MinipasRowWithRecnum;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;

public class MinipasDAOImpl implements MinipasDAO {
    private static final Logger aLog = Logger.getLogger(MinipasDAOImpl.class);
    @Autowired
    @Qualifier("minipasJdbcTemplate")
    JdbcTemplate jdbc;

    @Value("${minipas.minipasstatustablename:T_MINIPAS_UGL_STATUS}")
    String minipasStatusTableName;

    @Value("${minipas.minipasprefix:MINIPAS_UGL.}")
    String minipasPrefix;

    Map<Integer, Collection<MinipasTSKSUBE_OPR>> aRecnumToSKSUBECollectionMap;
    String aCurrentSKSUBETableYear;
    Map<Integer, Collection<MinipasTSKSUBE_OPR>> aRecnumToSKSOPRCollectionMap;
    String aCurrentSKSOPRTableYear;
    Map<Integer, Collection<MinipasTDIAG>> aRecnumToDIAGCollectionMap;
    String aCurrentDIAGTableYear;

    @Override
    public void reset() {
        aRecnumToSKSUBECollectionMap = null;
        aRecnumToSKSOPRCollectionMap = null;
        aRecnumToDIAGCollectionMap = null;
    }

    @Override
    public Collection<MinipasTADM> getMinipasTADM(String year, long fromKRecnum, int batchSize) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasTADM");
        String tableName = "T_ADM" + year;
        // maybe this works - needs to find out if order by executes on resultset or before (before is the intend)
        List<MinipasTADM> query = jdbc.query("SELECT * FROM " + minipasPrefix + tableName
                + " WHERE K_RECNUM > ? ORDER BY K_RECNUM FETCH FIRST " + batchSize + " ROWS ONLY",
                new RowMapper<MinipasTADM>() {
                    @Override
                    public MinipasTADM mapRow(ResultSet rs, int rowNum) throws SQLException {
                        MinipasTADM returnValue = new MinipasTADM();
                        returnValue.setC_adiag(rs.getString("C_ADIAG"));
                        returnValue.setC_afd(rs.getString("C_AFD"));
                        returnValue.setC_bopamt(rs.getString("C_BOPAMT"));
                        returnValue.setC_hafd(rs.getString("C_HAFD"));
                        returnValue.setC_henm(rs.getString("C_HENM"));
                        returnValue.setC_hsgh(rs.getString("C_HSGH"));
                        returnValue.setC_indm(rs.getString("C_INDM"));
                        returnValue.setC_kom(rs.getString("C_KOM"));
                        returnValue.setC_kontaars(rs.getString("C_KONTAARS"));
                        returnValue.setC_pattype(rs.getString("C_PATTYPE"));
                        returnValue.setC_sex(rs.getString("C_SEX"));
                        returnValue.setC_sgh(rs.getString("C_SGH"));
                        returnValue.setC_sghamt(rs.getString("C_SGHAMT"));
                        returnValue.setC_spec(rs.getString("C_SPEC"));
                        returnValue.setC_udm(rs.getString("C_UDM"));
                        returnValue.setD_hendto(rs.getTimestamp("D_HENDTO"));
                        returnValue.setD_inddto(rs.getTimestamp("D_INDDTO"));
                        returnValue.setD_uddto(rs.getTimestamp("D_UDDTO"));
                        returnValue.setIdnummer(rs.getString("IDNUMMER"));
                        returnValue.setK_recnum(rs.getInt("K_RECNUM"));
                        returnValue.setSkemaopdat(rs.getTimestamp("SKEMAOPDAT"));
                        returnValue.setSkemaopret(rs.getTimestamp("SKEMAOPRET"));
                        returnValue.setV_alder(rs.getInt("V_ALDER"));
                        returnValue.setV_behdage(rs.getInt("V_BEHDAGE"));
                        returnValue.setV_cpr(rs.getString("V_CPR"));
                        returnValue.setV_sengdage(rs.getInt("V_SENGDAGE"));
                        return returnValue;
                    }
                }, fromKRecnum);
        mon.stop();
        return query;
    }

    @Override
    public long lastReturnCodeElseNegativeOne() {
        try {
            String sql1 = "SELECT max(K_ID) FROM " + minipasPrefix + minipasStatusTableName;
            String sql2 = "SELECT V_RETURNCODE FROM " + minipasPrefix + minipasStatusTableName
                    + " WHERE K_ID = ? AND D_ENDDATETIME IS NOT NULL";

            long maxSyncId = jdbc.queryForLong(sql1);
            if (aLog.isTraceEnabled()) {
                aLog.trace("lastReturnCodeElseNegativeOne: maxSyncId=" + maxSyncId);
            }
            long returnCode = jdbc.queryForLong(sql2, maxSyncId);
            if (aLog.isTraceEnabled()) {
                aLog.trace("lastReturnCodeElseNegativeOne: returnCode=" + returnCode);
            }
            return returnCode;
        } catch (EmptyResultDataAccessException e) {
            // LPR is not ready for Import
            System.out.println("empty");
        } catch (RuntimeException e) {
            throw new DAOException("Error fetching database status from " + minipasPrefix + minipasStatusTableName, e);
        }
        return -1;
    }

    @Override
    public Collection<MinipasTSKSUBE_OPR> getMinipasSKSUBE_OPR(String year, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasSKSUBE_OPR");
        List<MinipasTSKSUBE_OPR> returnValue = new ArrayList<MinipasTSKSUBE_OPR>();
        returnValue.addAll(getMinipasSKSOPR(year, recnum));
        returnValue.addAll(getMinipasSKSUBE(year, recnum));
        mon.stop();
        return returnValue;
    }

    @Override
    public Collection<MinipasTDIAG> getMinipasDIAG(String year, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasDIAG");
        if (!year.equals(aCurrentDIAGTableYear)) {
            // new cache
            aCurrentDIAGTableYear = year;
            aRecnumToDIAGCollectionMap = null;
        }
        
        if (doBuildCache(aRecnumToDIAGCollectionMap, recnum)) {
            if (aLog.isTraceEnabled()) {
                aLog.trace("getMinipasDIAG: rebuilding cache");
            } 
            // start or cache just got obsolete, refill it
            RowMapper<MinipasTDIAG> rowMapper = new MyMinipasTDIAGRowMapper();
            String tableName = "T_DIAG" + year;

            // evict
            aRecnumToDIAGCollectionMap = new HashMap<Integer, Collection<MinipasTDIAG>>();

            // start from the requested recnum, assuming to be asked in a recnum order
            buildCache(aRecnumToDIAGCollectionMap, rowMapper, tableName, recnum);
        }

        // most just goes here
        Collection<MinipasTDIAG> collection = aRecnumToDIAGCollectionMap.get(recnum);
        if (collection == null) {
            collection = new ArrayList<MinipasTDIAG>();
        }
        if (aLog.isTraceEnabled()) {
            aLog.trace("getMinipasDIAG: " + (collection.isEmpty() ? "none" : collection.size()) + " for " + recnum);
        }
        mon.stop();
        return collection;
    }

    private Collection<MinipasTSKSUBE_OPR> getMinipasSKSUBE(String year, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasSKSUBE");
        if (!year.equals(aCurrentSKSUBETableYear)) {
            // new cache
            aCurrentSKSUBETableYear = year;
            aRecnumToSKSUBECollectionMap = null;
        }

        if (doBuildCache(aRecnumToSKSUBECollectionMap, recnum)) {
            if (aLog.isTraceEnabled()) {
                aLog.trace("getMinipasSKSUBE: rebuilding cache");
            } 
            // start or cache just got obsolete, refill it
            RowMapper<MinipasTSKSUBE_OPR> rowMapper = new MyMinipasTSKSUBE_OPRRowMapper("und");
            String tableName = "T_SKSUBE" + year;

            // evict
            aRecnumToSKSUBECollectionMap = new HashMap<Integer, Collection<MinipasTSKSUBE_OPR>>();

            // start from the requested recnum, assuming to be asked in a recnum order
            buildCache(aRecnumToSKSUBECollectionMap, rowMapper, tableName, recnum);
        }

        // most just goes here
        Collection<MinipasTSKSUBE_OPR> collection = aRecnumToSKSUBECollectionMap.get(recnum);
        if (collection == null) {
            collection = new ArrayList<MinipasTSKSUBE_OPR>();
        }
        if (aLog.isTraceEnabled()) {
            aLog.trace("getMinipasSKSUBE: " + (collection.isEmpty() ? "none" : collection.size()) + " for " + recnum);
        }
        mon.stop();
        return collection;
    }

    private Collection<MinipasTSKSUBE_OPR> getMinipasSKSOPR(String year, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasSKSOPR");
        if (!year.equals(aCurrentSKSOPRTableYear)) {
            // new cache
            aCurrentSKSOPRTableYear = year;
            aRecnumToSKSOPRCollectionMap = null;
        }

        if (doBuildCache(aRecnumToSKSOPRCollectionMap, recnum)) {
            if (aLog.isTraceEnabled()) {
                aLog.trace("getMinipasSKSOPR: rebuilding cache");
            } 
            
            // start or cache just got obsolete, refill it
            RowMapper<MinipasTSKSUBE_OPR> rowMapper = new MyMinipasTSKSUBE_OPRRowMapper("opr");
            String tableName = "T_SKSOPR" + year;

            // evict
            aRecnumToSKSOPRCollectionMap = new HashMap<Integer, Collection<MinipasTSKSUBE_OPR>>();

            // start from the requested recnum, assuming to be asked in a recnum order
            buildCache(aRecnumToSKSOPRCollectionMap, rowMapper, tableName, recnum);
        }

        // most just goes here
        Collection<MinipasTSKSUBE_OPR> collection = aRecnumToSKSOPRCollectionMap.get(recnum);
        if (collection == null) {
            collection = new ArrayList<MinipasTSKSUBE_OPR>();
        }
        if (aLog.isTraceEnabled()) {
            aLog.trace("getMinipasSKSOPR: " + (collection.isEmpty() ? "none" : collection.size()) + " for " + recnum);
        }
        mon.stop();
        return collection;
    }

    private <T extends MinipasRowWithRecnum> boolean doBuildCache(Map<Integer, Collection<T>> map, int recnum) {
        // rebuild if we have no map, if the recnum is not in the map or if the recnum is not inside the range of the
        // recnums last fetched
        boolean returnValue = map == null || (!map.containsKey(recnum) && recnumNotInKeyRange(map, recnum));
        return returnValue;
    }

    private <T extends MinipasRowWithRecnum> boolean recnumNotInKeyRange(Map<Integer, Collection<T>> map, int recnum) {
        boolean returnValue = false;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        Set<Integer> keySet = map.keySet();
        for (Integer key : keySet) {
            min = Math.min(min, key);
            max = Math.max(max, key);
        }
        returnValue = (recnum < min) || (max < recnum);
        if (aLog.isTraceEnabled()) {
            aLog.trace("recnumNotInKeyRange: " + returnValue + ", min=" + min + ", max=" + max + ", recnum=" + recnum);
        }
        return returnValue;
    }

    // All this assumes that we are asked in an recnum ordered way. if lowest recnums are here first, then we request
    // from this recnum and forward a batch size. when this batch is done, we get a new batch and so on
    public <T extends MinipasRowWithRecnum> void buildCache(Map<Integer, Collection<T>> destination,
            RowMapper<T> rowMapper, String tableName, int currentRecnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.buildCache");
        int batchSize = 1000;
        List<T> query = jdbc.query("SELECT * FROM " + minipasPrefix + tableName
                + " WHERE V_RECNUM >= ? ORDER BY V_RECNUM FETCH FIRST " + batchSize + " ROWS ONLY", rowMapper,
                currentRecnum);

        // be sure to tell that we have already been here for this recnum, even if there is no data
        destination.put(currentRecnum, new ArrayList<T>());
        for (T t : query) {
            // store in cache
            Collection<T> collection = destination.get(t.getV_recnum());
            if (collection == null) {
                collection = new ArrayList<T>();
                destination.put(t.getV_recnum(), collection);
            }
            collection.add(t);
        }
        mon.stop();
    }

    private final class MyMinipasTSKSUBE_OPRRowMapper implements RowMapper<MinipasTSKSUBE_OPR> {
        private final String aType;

        private MyMinipasTSKSUBE_OPRRowMapper(String type) {
            aType = type;
        }

        @Override
        public MinipasTSKSUBE_OPR mapRow(ResultSet rs, int rowNum) throws SQLException {
            MinipasTSKSUBE_OPR returnValue = new MinipasTSKSUBE_OPR();
            returnValue.setC_oafd(rs.getString("C_OAFD"));
            returnValue.setC_opr(rs.getString("C_OPR"));
            returnValue.setC_oprart(rs.getString("C_OPRART"));
            returnValue.setC_osgh(rs.getString("C_OSGH"));
            returnValue.setC_tilopr(rs.getString("C_TILOPR"));
            returnValue.setD_odto(rs.getTimestamp("D_ODTO"));
            returnValue.setIdnummer(rs.getString("IDNUMMER"));
            returnValue.setIndberetningsdato(rs.getTimestamp("INDBERETNINGSDATO"));
            returnValue.setV_recnum(rs.getInt("V_RECNUM"));
            returnValue.setType(aType);
            return returnValue;
        }
    }

    private final class MyMinipasTDIAGRowMapper implements RowMapper<MinipasTDIAG> {
        @Override
        public MinipasTDIAG mapRow(ResultSet rs, int rowNum) throws SQLException {
            MinipasTDIAG returnValue = new MinipasTDIAG();
            returnValue.setC_diag(rs.getString("C_DIAG"));
            returnValue.setC_diagtype(rs.getString("C_DIAGTYPE"));
            returnValue.setC_tildiag(rs.getString("C_TILDIAG"));
            returnValue.setIdnummer(rs.getString("IDNUMMER"));
            returnValue.setIndberetningsdato(rs.getTimestamp("INDBERETNINGSDATO"));
            returnValue.setV_recnum(rs.getInt("V_RECNUM"));
            return returnValue;
        }
    }
}
