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

import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;

public class MinipasHAIBADAOImpl extends CommonDAO implements MinipasHAIBADAO {
    private static final Logger aLog = Logger.getLogger(MinipasHAIBADAOImpl.class);
    @Autowired
    @Qualifier("haibaJdbcTemplate")
    JdbcTemplate jdbc;

    @Value("${jdbc.minipashaibatableprefix:}")
    String tableprefix;

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
                    m.getIdnummer(), m.getC_opr(), m.getC_tilopr(), m.getC_oprart(), m.getIndberetningsdato(),
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
    public void createKoderFromDiag(MinipasTADM minipasTADM, Collection<MinipasTDIAG> diags) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createKoderFromDiag");
        for (MinipasTDIAG m : diags) {
            jdbc.update(
                    "INSERT INTO "
                            + tableprefix
                            + "T_KODER (V_RECNUM, C_KODE, C_TILKODE, C_KODEART, D_PDTO, C_PSGH, C_PAFD, V_TYPE) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    m.getIdnummer(), m.getC_diag(), m.getC_tildiag(), m.getC_diagtype(), m.getIndberetningsdato(),
                    minipasTADM.getC_sgh(), minipasTADM.getC_afd(), "dia");
        }
        mon.stop();
    }

    @Override
    public void createAdm(Collection<MinipasTADM> minipasTADMs) {
        Monitor mon = MonitorFactory.start("MinipasHAIBADAOImpl.createAdm");
        for (MinipasTADM m : minipasTADMs) {
            jdbc.update(
                    "INSERT INTO "
                            + tableprefix
                            + "T_ADM (V_RECNUM, C_SGH, C_AFD, C_PATTYPE, V_CPR, D_INDDTO, D_UDDTO) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    m.getIdnummer(), m.getC_sgh(), m.getC_afd(), m.getC_pattype(), m.getV_cpr(), m.getD_inddto(),
                    m.getD_uddto());
        }
        mon.stop();
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
}
