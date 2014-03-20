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
    String aCurrentSKSUBETable;
    Map<Integer, Collection<MinipasTSKSUBE_OPR>> aRecnumToSKSOPRCollectionMap;
    String aCurrentSKSOPRTable;
    Map<Integer, Collection<MinipasTDIAG>> aRecnumToDIAGCollectionMap;
    String aCurrentDIAGTable;

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
        returnValue.addAll(getMinipasSKSUBE_OPRFromTable("T_SKSUBE" + year, recnum));
        returnValue.addAll(getMinipasSKSUBE_OPRFromTable("T_SKSOPR" + year, recnum));
        mon.stop();
        return returnValue;
    }

    private Collection<MinipasTSKSUBE_OPR> getMinipasSKSUBE_OPRFromTable(String tableName, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasSKSUBE_OPRFromTable");
        // maybe this works - needs to find out if order by executes on resultset or before (before is the intend)
        // check if these are correct v_types to put into T_KODER XXX
        // 'ube'->'und' for now, possibly also 'til'?
        final String type = tableName.toLowerCase().contains("ube") ? "und" : "opr";
        List<MinipasTSKSUBE_OPR> query = jdbc.query("SELECT * FROM " + minipasPrefix + tableName
                + " WHERE V_RECNUM = ?", new RowMapper<MinipasTSKSUBE_OPR>() {
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
                returnValue.setType(type);
                return returnValue;
            }
        }, recnum);
        mon.stop();
        return query;
    }

    @Override
    public Collection<MinipasTDIAG> getMinipasDIAG(String year, int recnum) {
        Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasDIAG");
        if (!year.equals(aCurrentDIAGTable)) {
            // new cache
            aCurrentDIAGTable = year;
            aRecnumToDIAGCollectionMap = new HashMap<Integer, Collection<MinipasTDIAG>>();
            RowMapper<MinipasTDIAG> rowMapper = new RowMapper<MinipasTDIAG>() {
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
            };
            
            int queryRecnum = -1;
            Collection<MinipasTDIAG> query = null;
            int batchSize = 1000;
            while (query == null || !query.isEmpty()) {
                query = jdbc.query("SELECT * FROM " + minipasPrefix + "T_DIAG" + year
                        + " WHERE V_RECNUM > ? ORDER BY V_RECNUM FETCH FIRST " + batchSize + " ROWS ONLY", rowMapper,
                        queryRecnum);

                for (MinipasTDIAG minipasTDIAG : query) {
                    // store in cache
                    Collection<MinipasTDIAG> collection = aRecnumToDIAGCollectionMap.get(minipasTDIAG.getV_recnum());
                    if (collection == null) {
                        collection = new ArrayList<MinipasTDIAG>();
                        aRecnumToDIAGCollectionMap.put(minipasTDIAG.getV_recnum(), collection);
                    }
                    collection.add(minipasTDIAG);

                    // prepare next query
                    queryRecnum = Math.max(queryRecnum, minipasTDIAG.getV_recnum());
                }
            }
        }
        // 99.9% just goes here
        Collection<MinipasTDIAG> collection = aRecnumToDIAGCollectionMap.get(recnum);
        mon.stop();
        return collection;
    }
    // @Override
    // public Collection<MinipasTDIAG> getMinipasDIAG(String year, int recnum) {
    // Monitor mon = MonitorFactory.start("MinipasDAOImpl.getMinipasDIAG");
    // // maybe this works - needs to find out if order by executes on resultset or before (before is the intend)
    // Collection<MinipasTDIAG> query = jdbc.query("SELECT * FROM " + minipasPrefix + "T_DIAG" + year
    // + " WHERE V_RECNUM = ?", new RowMapper<MinipasTDIAG>() {
    // @Override
    // public MinipasTDIAG mapRow(ResultSet rs, int rowNum) throws SQLException {
    // MinipasTDIAG returnValue = new MinipasTDIAG();
    // returnValue.setC_diag(rs.getString("C_DIAG"));
    // returnValue.setC_diagtype(rs.getString("C_DIAGTYPE"));
    // returnValue.setC_tildiag(rs.getString("C_TILDIAG"));
    // returnValue.setIdnummer(rs.getString("IDNUMMER"));
    // returnValue.setIndberetningsdato(rs.getTimestamp("INDBERETNINGSDATO"));
    // returnValue.setV_recnum(rs.getInt("V_RECNUM"));
    // return returnValue;
    // }
    // }, recnum);
    // mon.stop();
    // return query;
    // }
}
